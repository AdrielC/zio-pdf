# Shared PDF parser migration

The Kyo structural scanner, content-token parser and primitive/object decoder now
operate on shared byte views. `PdfDocument.decode`, the Paper Machine admission
path that calls it, and the ZIO adapters use this implementation.

## Data flow

Array APIs borrow the caller's array for the parse. Do not mutate it during the
operation. Chunked `PdfRead` and ZIO inputs make one ownership copy into bounded
blocks. The header probe and random-access parser then share those same blocks.
No whole-document String or contiguous flattened input is constructed.

The structural producer emits admitted object boundaries to a generic `Scan`
consumer which constructs typed values. A consumer error stops the producer.
Forward `/Length` references share a lazy classic-xref numeric-object map.
Output Strings, primitive values and stream vectors remain owned by the resulting
document. Expanded object streams remain bounded at 8 MiB; structural measurement
and typed object-stream decoding still perform separate bounded expansion work.

## Early completion and errors

- `PdfParser.scanUntil(bytes, limit)(predicate)` stops after an admitted object
  satisfies the predicate. Its report is partial and does not validate the suffix.
- Full scanning and decoding stop at the first structural, primitive, encryption
  or retention-policy failure. Limits are checked before passing each object to
  the typed decoder, rather than after decoding the whole document.
- Streaming header syntax and input-limit failures stop upstream pulls immediately
  after the failing transport chunk. Scope exit closes the reader and releases input.
- Full stream scanning/decoding requires EOF: forward references and incremental
  xrefs can refer to the end of the document. Body errors are therefore discovered
  during the EOF parse; this does not claim arbitrary body errors fail during upload.
- The partial `scanUntil` API currently accepts array input. The regular ZIO
  `scanStream` and `decodeStream` APIs require EOF for successful completion.

`PdfRead` admits at most the caller's ByteLimit and a supported maximum of 64 MiB,
in blocks of at most 64 KiB, with at most 65,536 block descriptors. Empty transport
chunks allocate no storage. This deliberately retains bounded input for random
access. It is not a constant-memory parser or a disk spool. Account for admitted
input, bounded metadata and the retained typed document together; configured
payload-byte accounting does not measure all object/Vector heap overhead.

The immutable `PdfInput` compatibility API also reads its existing chunk vectors
without flattening them. Empty feeds are ignored, and block metadata is bounded.
New streaming integrations should use `PdfRead` or `PdfZIO.decodeStream`.

## Local comparison with public v0.6.0

The benchmark script reconstructs its baseline only from the pinned public commit
`88d0ebb131e91a2978da790e0aea7404388b0420`. It checks decoded document and content-token equality
outside timing and compares the same synchronous public APIs. Chunked ingestion
compares the public baseline PdfInput against PdfRead using 64 KiB chunks.

Environment: Apple M3 Pro, arm64 macOS; Scala 3.8.4; Temurin JVM 24.0.2 with
3 GiB maximum heap; Scala Native 0.5.12 debug build, immix GC, no LTO.
Inputs are synthetic and built before measurement. Each case has ten warmup
pairs followed by fifteen alternating measurement pairs; values below are medians.
These qualify this local workload, not general production throughput or a tuned
release-mode Native build. Native allocation measurements are unavailable.

Measured implementation: `9355a296e9e9c168aee10ef3a6e175676636bece`. The host is shared; measurements
vary across runs. Each baseline/candidate pair is measured in the same run.

| Workload | Input bytes | JVM old → new (ms) | JVM speedup | Native old → new (ms) | Native speedup |
|---|---:|---:|---:|---:|---:|
| small-scan | 447 | 0.294 → 0.146 | 2.01× | 0.408 → 0.213 | 1.91× |
| small-decode | 447 | 0.591 → 0.604 | 0.98× | 0.738 → 0.311 | 2.37× |
| small-chunked-decode | 447 | 0.864 → 0.556 | 1.55× | 0.643 → 0.296 | 2.17× |
| stream-heavy-scan | 1,049,564 | 9.883 → 0.141 | 69.93× | 135.158 → 0.422 | 320.44× |
| stream-heavy-decode | 1,049,564 | 22.205 → 3.571 | 6.22× | 247.651 → 144.569 | 1.71× |
| stream-heavy-chunked-decode | 1,049,564 | 30.858 → 5.700 | 5.41× | 237.976 → 71.964 | 3.31× |
| object-heavy-scan | 28,576 | 1.494 → 0.658 | 2.27× | 17.463 → 9.564 | 1.83× |
| object-heavy-decode | 28,576 | 3.666 → 2.797 | 1.31× | 32.096 → 14.114 | 2.27× |
| object-heavy-chunked-decode | 28,576 | 4.256 → 4.122 | 1.03× | 35.060 → 14.620 | 2.40× |
| content | 62,000 | 7.811 → 1.015 | 7.70× | 960.067 → 34.632 | 27.72× |

For the 1,049,564-byte stream-heavy document, JVM full-decode allocations fell
from 67,197,824 to 4,970,520 bytes per operation; chunked-decode allocations fell
from 73,140,984 to 6,047,808 bytes. Token-only content allocations fell from
36,784,016 to 2,193,488 bytes. These are total current-thread allocations, not
peak live memory or RSS; Native allocation measurements are unavailable.

Content parsing uses ASCII grammar loops instead of compiling regular expressions
per token. Sign/decimal/operator boundaries, unsupported markers, token limits and
full benchmark token equality are checked separately. Small JVM full decode is
about 2.2% slower in this run; object-heavy chunked JVM decoding is nearly flat.
This is not a claim that every PDF or scan becomes faster.

[Raw measurements](benchmarks/pdf-parser-migration-2026-11-05.json) include allocation
figures for every JVM case.

Reproduce:

```sh
bash scripts/benchmark-pdf-parsers.sh jvm
bash scripts/benchmark-pdf-parsers.sh native
```

The benchmark creates temporary baseline sources, never imports another checkout,
and removes them on exit. Run correctness suites separately with:

```sh
sbt ';kyoPdf/testFull;kyoPdfZio/testFull;kyoPdfPipeline/testFull;paperMachine/testFull'
sbt ';kyoPdfNative/testFull;kyoPdfPipelineNative/testFull'
```

[Generic Scan / FS2 comparison](scan-performance.md) reports both wins and losses
for kernels, bounded batched pipes and elementwise yielding.
