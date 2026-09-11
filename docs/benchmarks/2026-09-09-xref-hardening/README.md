# Parser hardening and performance evidence

This change fixes the cross-reference stream failure reported with DocuSigned PDFs and strengthens the same decoding boundary. It does not qualify the entire library as production-ready.

## Changes

- Read unsigned cross-reference fields directly after validating widths, rather than building scodec tuple decoders for every entry. Four-byte generation/index fields and eight-byte offsets no longer fail codec construction or narrow silently.
- Preserve `/Index` section order; reject malformed explicit indexes rather than treating them as absent. Validate integers, ranges, payload lengths and partial bytes before allocating entries.
- Preserve omitted-field defaults and treat unknown entry types as null references. Normalize DocuSign's `0xffffffff` permanently-free sentinel only for object zero, with zero next-free pointer. In-use generations remain strictly bounded.
- Bound cross-reference expansion to one million entries by default, independently of payload size. This covers zero-width entries that otherwise permit huge expansion from empty input. Direct callers can configure `XrefStream.Limits`; this bounds entry count, not total heap.
- Enforce ASCIIHex/ASCII85 output limits before output writes and avoid copying complete input views. Reject overflowing ASCII85 tuples, incomplete one-digit tuples and malformed terminators.
- Include the vendored Volga classes in the JVM artifact without an unpublished Maven dependency, compile the shared graph implementation for Scala.js, and identify JVM fused-ingest facades as JVM-only. Extend external-consumer and artifact checks to cover those dependencies.
- Preserve isolated graph nodes in Mermaid/Dot, retain package paths in the browser source JAR, and freeze the consumer-proof snapshot version across publication and resolution.

## Test strategy

Most new tests are generated properties: valid field encodings, compressed references, sparse ranges, unsigned overflow, malformed declarations, payload mutation, exact entry/output limits, ASCII variants, and graph composition. Fixed fixtures cover the DocuSign sentinel, large offsets, a logical multi-gigabyte view, and a tiny complete PDF independently checked with PDFBox. No private PDF is committed.

The shared cross-reference and ASCII suites run on both JVM and Scala.js. Their new coverage consists of 18 generated properties and six fixed regressions; the JVM integration suite adds two complete-PDF checks, and the browser graph suite adds two generated properties. In total, 20 of 28 new tests are property-based.

Both locally reported DocuSigned PDFs also passed Preflight's actual `PdfLayout.inspect` path with the candidate classes: eight pages and 27 pages. This read-only smoke check did not rewrite the signed files. Inspection does not validate signature authenticity or establish that all editing/rendering paths work.

## Benchmark method

Baseline: `f681c8d` from AdrielC/zio-pdf main, with only the new benchmark harness added. Candidate: parser code accompanying this report. Both runs used the same Apple M3 Pro machine, macOS 15.6.1, OpenJDK 24.0.2, JMH 1.37, one worker thread, two independent JVM forks, three one-second warmups and five one-second measurements per fork, with the GC profiler. No other test/build from this task ran during measurements. This is a local comparison, not an isolated-host production load test.

| Operation | Baseline mean | Candidate mean | Allocation before → after |
| --- | ---: | ---: | ---: |
| Decode and construct 100 xref entries | 15.31 µs | 6.79 µs | 131,172 → 36,988 B/op |
| Decode and construct 10,000 xref entries | 2,212.45 µs | 553.81 µs | 12,347,535 → 3,482,194 B/op |
| Complete evidence, SCOTUS order list | 22.82 ms | 21.30 ms | 103,472,601 → 101,827,095 B/op |
| Complete evidence, Fourth Circuit opinion | 25.47 ms | 24.74 ms | 78,672,460 → 78,116,527 B/op |

The targeted xref path improved 2.25–4.00x and allocated about 72% fewer bytes per operation. Full evidence means improved, but confidence intervals overlap; these samples do not establish a statistically significant end-to-end speedup. They provide a check against a large regression on two public documents. The full evidence path reads the file, decodes, inspects, extracts text evidence, validates, checks policy, and computes SHA-256.

Allocation/op is total allocated bytes, not retained heap or peak RSS. There is no claimed speedup for ASCII filters, the browser, rendering, or signature verification. Raw JMH results are in the four adjacent JSON files.

Reproduce the measurements:

```sh
sbt -batch 'bench/Jmh/run -wi 3 -i 5 -w 1s -r 1s -f 2 -t 1 -prof gc -rf json -rff /tmp/xref.json .*XrefStreamBench.*'
sbt -batch 'bench/Jmh/run -wi 3 -i 5 -w 1s -r 1s -f 2 -t 1 -prof gc -p fixture=court-corpus/scotus-order-list-2025-05-19.pdf,court-corpus/ca4-bayramov-v-american-credit-acceptance.pdf -rf json -rff /tmp/evidence.json .*PublicPdfCorpusBench.pathEvidenceBundle'
```

## Completed verification

- 388 JVM tests passed using `root/testOnly *`; after the isolated-node renderer fix, all 17 affected JVM graph tests passed again.
- All 53 Scala.js tests and 13 benchmark-module tests passed.
- JVM example execution and both JMH benchmark projects compiled successfully.
- JVM and Scala.js artifact audits passed, including vendored classes, source-JAR packaging and absence of unpublished module dependencies.
- A separate sbt 1.x consumer resolved the published temporary Maven artifact and executed both structural scanning and the graph runtime.
- The optimized browser demo built successfully and passed its PDF preview compatibility gate.
- Local publication succeeded. Public Maven publication and application installation were not performed.

## Scope and remaining work

This audit covers xref decoding and the ASCII filter allocation boundary, plus the build/package defects encountered during verification. Flate predictor internals, LZW/RunLength implementations, aggregate decoded-object memory, and all other parser paths have not been exhaustively hardened by this change. Signed original bytes must remain immutable; successful parsing is not permission to rewrite signatures. No package was released to Maven Central and no installed Preflight application was replaced.

Format reference: [PDF Association's ISO 32000-2 syntax errata, section 7.5.8](https://pdf-issues.pdfa.org/32000-2-2020/clause07.html).
