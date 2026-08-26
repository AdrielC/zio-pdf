# zio-pdf

[![CI](https://github.com/AdrielC/zio-pdf/actions/workflows/ci.yml/badge.svg)](https://github.com/AdrielC/zio-pdf/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

`zio-pdf` is a Scala 3 and ZIO 2 library for incremental PDF parsing, content-addressed ingestion, structural inspection, evidence extraction, and fail-closed rewriting. The same parser and hashing model runs on the JVM and Scala.js.

The library separates two workloads that should not be conflated:

- `PdfObjectScanner` finds indirect-object boundaries with bounded structural carry and never copies stream payloads.
- `PdfEngine` expands PDF objects for validation, text, evidence, transforms, and semantic diffing. Any content-stream allocation is guarded by a typed `ByteLimit`.

That distinction makes the raw path suitable for very large uploads while keeping high-level parsing honest about the memory it needs.

## Project status

Release `0.2.0-RC6` is available from Maven Central for both the JVM and Scala.js. The source, tests, [browser playground](https://adrielc.github.io/zio-pdf/), POM metadata, signed-release workflow, and independent-consumer check are public.

```scala
libraryDependencies += "io.github.adrielc" %%  "zio-pdf" % "0.2.0-RC6" // JVM
libraryDependencies += "io.github.adrielc" %%% "zio-pdf" % "0.2.0-RC6" // Scala.js
```

Only the JVM and Scala.js `zio-pdf` artifacts are publishable. Examples, benchmarks, the browser application, and the Kyo comparison module are build-only projects.

## Quick start

Decode a path incrementally and consume results without collecting the document:

```scala
import java.nio.file.Path
import zio.*
import zio.pdf.*

val program =
  PdfEngine
    .stream(Path.of("filing.pdf"))
    .runForeach {
      case Decoded.DataObj(obj)          => ZIO.logDebug(s"object ${obj.index.number}")
      case Decoded.ContentObj(obj, _, _) => ZIO.logDebug(s"stream ${obj.index.number}")
      case Decoded.Meta(_, _, _)         => ZIO.unit
    }
    .provide(PdfEngine.live)
```

For a caller-owned upload body, keep the bytes lazy:

```scala
def inspectUpload(body: zio.stream.ZStream[Any, Throwable, Byte]) =
  PdfEngine
    .decode(body)
    .runForeach(decoded => index(decoded))
    .provide(PdfEngine.live)
```

`PdfEngine.decode(body)` returns a `ZStream`; it does not collect the upload. Use `runCollect` only when the caller has already established a suitable bound and actually needs the full decoded timeline.

## Content-addressed object scanning

`PdfObjectScanner` is the minimal CAS-facing API. Feed it whatever chunks arrive from HTTP, file I/O, or object storage and persist each completed byte range under your own digest.

```scala
import zio.*
import zio.pdf.*

val config = PdfObjectScanner.Config(maxCarryBytes = 1024 * 1024)

def scanChunk(
  cursor: PdfObjectScanner.Cursor,
  chunk: Chunk[Byte]
): IO[PdfObjectScanner.Error, (PdfObjectScanner.Cursor, Chunk[PdfObjectScanner.Boundary])] =
  ZIO.fromEither(PdfObjectScanner.step(config, cursor, chunk))
```

Each `Boundary` contains the indirect-object index and an absolute `Long` offset for the next unread byte. The scanner:

- retains at most `maxCarryBytes` of incomplete syntax;
- skips payload bytes according to a direct numeric `/Length`;
- ignores `endobj` bytes that occur inside payload data;
- preserves offsets beyond one tebibyte;
- reports an indirect `/Length` as typed `Error.IndirectLength`, because resolving it correctly requires xref-backed random access.

The payload can be a terabyte because it is forwarded and counted, not assembled. The tests exercise logical offsets beyond one tebibyte without allocating such a file; CI also scans vendored public court PDFs and exercises multi-mebibyte streaming paths. Those are separate, truthful proofs.

## Bounded high-level decoding

`StreamingDecode.Config` controls both parser behavior and per-stream allocation:

```scala
val streamLimit = ByteLimit.mebibytes(128)

val config = StreamingDecode.Config(
  inlineMaxBytes = 256 * 1024L,
  maxCarryBytes = Some(1024 * 1024),
  maxMaterializedStreamBytes = streamLimit
)

val options = PdfEngine.Options(
  config = config,
  batchSize = 1024 * 1024,
  maxInputBytes = 4L * 1024L * 1024L * 1024L,
  maxMaterializedDocumentBytes = ByteLimit.mebibytes(256)
)
```

`maxInputBytes` is an optional whole-source admission ceiling and may remain unbounded for incremental paths. `maxMaterializedDocumentBytes` independently protects APIs that return a complete `Chunk` of decoded objects or elements. `maxMaterializedStreamBytes` bounds any one raw or decompressed stream that the high-level decoder must materialize. JVM and Scala.js Flate decoders stop when output crosses that bound. RunLength and LZW expansion are guarded by the same limit.

Use the API that matches the ownership model:

| Input and result | API | Memory behavior |
| --- | --- | --- |
| arbitrary upload, object boundaries | `PdfObjectScanner.step` | bounded carry, no payload collection |
| arbitrary upload, raw events | `PdfEngine.streaming(options)` | bounded parser state |
| path, decoded events | `PdfEngine.stream(path, options)` | bounded file windows and per-stream limit |
| path, effectful consumer | `PdfEngine.sinkZIO(path, options)(f)` | native ZIO interruption and scope |
| collected decoded timeline | `PdfEngine.decode(path, options)` | typed 256 MiB document bound by default |
| already-owned bytes | `PdfEngine.decode(chunk, options)` | caller owns the input; collected output still has the document bound |
| intentionally collected file bytes | `PdfIO.readAtMost(path, ByteLimit)` | typed hard bound |

`PdfIO.readAll` is retained as a convenience but is capped at 64 MiB. Complete decoded collections are capped at 256 MiB by default. Both limits are typed and configurable. Use `PdfIO.reader`, `PdfEngine.stream`, or `PdfEngine.sinkZIO` for arbitrary-size files.

## Streaming writer

`Part.StreamObj` accepts a `ZStream[Byte]` and a declared `Long` length. The writer rechunks payload output to 64 KiB, counts without retaining content, rejects negative lengths and non-dictionary stream headers, and checks the declared length before emitting `endstream`.

```scala
val part = Part.StreamObj(
  index = Obj.Index(42, 0),
  data = Prim.dict("Type" -> Prim.Name("EmbeddedFile")),
  length = knownLength,
  payload = uploadBytes
)
```

Cross-reference offsets are generated in physical output order. Tests compare every xref row with the actual object position, and CI validates rendered output with independent PDF tools when available.

## Inspection and evidence

Inspection plans are immutable and publicly inspectable. They have no private runtime dependency and no opaque callback leaves.

```scala
import zio.pdf.PdfInspection.*

val filingGate =
  linearized >>> pdfA >>> thumbnail >>> forbidJavaScript

val result = PdfEngine.inspect(path, filingGate).provide(PdfEngine.live)
val staticProfile = PdfInspection.profile(filingGate)
```

Positive-only plans may stop when every requested signal has appeared. An absence policy such as `forbidJavaScript` reads to the end on success and rejects at the first observed violation.

`PdfEvidence` combines raw SHA-256, inspection, validation, policy, native text, and provenance over one decoded event stream:

```scala
val bundle =
  PdfEngine
    .evidence(path, PdfEvidence.Plan.complete)
    .provide(PdfEngine.live)
```

Evidence records distinguish structural facts from heuristics. PDF/A metadata is reported as a producer declaration, not as a full conformance certification. Missing native text becomes an explicit recovery request for a caller-owned OCR or human-review step.

## Document transforms

`PdfTransform[A]` composes named operations into an immutable plan. A caller can analyze `transform.program` before execution.

```scala
val tokenizer = PdfTransform.text.Tokenizer.words

val transform =
  PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace") >>>
    PdfTransform.text.tokenize(tokenizer)

val profile = transform.profile

val rewritten = for
  output <- transform.run(PdfIO.reader(path))
  count  <- output.bytes.run(PdfIO.writer(target))
yield (count, output.value)
```

Rendering begins only after all transform preconditions succeed. Existing-font remapping verifies the source and replacement resources before changing page bindings. Text tokenization retains logical page object numbers for indexing and citations.

Document transforms currently retain the decoded object graph, so they are not the arbitrary-size upload path. Their individual content streams remain guarded by `ByteLimit`.

## Semantic diff

`PdfDiff` compares schema-backed document components in bounded LCS windows:

```scala
PdfEngine
  .diff(oldBytes, newBytes, PdfDiff.Config(windowSize = 128, maximumCells = 16384))
  .runForeach(window => persist(window))
  .provide(PdfEngine.live)
```

Each emitted window is an exact edit script for that window. The library does not claim a globally minimal streaming LCS, which would require retaining both complete sequences.

## ZIO Blocks integration

The build uses the current Maven Central releases of:

- `zio-blocks-schema` 0.017 for structural schemas and derivation;
- `zio-blocks-chunk` 0.017 for allocation-conscious chunk operations;
- `zio-blocks-mediatype` 0.0.51 for media-type boundaries;
- `zio-blocks-ringbuffer` 0.0.51 only in tests and benchmarks.

`ScodecDeriver` implements all seven ZIO Blocks derivation shapes: primitive, record, variant, wrapper, sequence, map, and dynamic. Primitive coverage includes Java time values, currency, UUID, `BigInt`, and `BigDecimal`; the artifact gate rejects placeholder markers.

## Scala.js playground

The browser application performs parsing, hashing, evidence extraction, and document transforms locally through the Scala.js artifact. A selected file is consumed through `Blob.stream()` rather than `arrayBuffer()` for the evidence scan. The resulting font inventory exposes the PDF's actual resource names, object numbers, subtypes, and unambiguous replacement candidates.

```bash
npm ci
npm --prefix examples-js/frontend ci
npm --prefix examples-js/frontend run build
npm --prefix examples-js/frontend run dev
```

The preview reads bounded ranges from the browser `Blob` and renders one page at a time. It bundles PDF.js's translated and polyfilled legacy display layer and worker so supported mobile Safari releases do not depend on the modern bundle's newest JavaScript APIs. The evidence scan remains the arbitrary-size streaming path. Browser transforms retain a decoded document graph and therefore enforce explicit 64 MiB input and output bounds before producing a downloadable PDF. Font remapping runs the same encoding, widths, CID-metric, and `ToUnicode` checks as the library API and rejects incompatible replacements without rendering partial output.

OCR is an explicit browser-only recovery step. Neither preview nor OCR is presented as work performed by the core parser.

## Real corpus and operational proof

The test corpus contains six immutable PDFs from the U.S. Supreme Court, federal appellate courts, GovInfo, and a federal district court. [`src/test/resources/court-corpus/README.md`](src/test/resources/court-corpus/README.md) records the source URL, document facts, and SHA-256 for each fixture.

CI proves more than compilation:

```bash
sbt -batch ';root/test;scanKyo/test;scalaJs/test;bench/test'
sbt -batch ';root/package;root/packageDoc;scalaJs/package'
npm --prefix examples-js/frontend run build
bash scripts/audit-published-artifact.sh
bash scripts/verify-external-consumer.sh
```

The corpus suite checks exact hashes, fused-versus-streaming parity, object baselines, early downstream termination, content preservation, evidence extraction, and no-collection sink paths. The external-consumer script publishes to a fresh local Ivy repository and runs a separate sbt build against the resulting dependency.

Production invariants are recorded in [`docs/PRODUCTION_INVARIANTS.md`](docs/PRODUCTION_INVARIANTS.md).

## Repository layout

| Path | Purpose | Published |
| --- | --- | --- |
| `src/` | JVM parser, scanner, engine, writer, evidence, transform, diff | yes |
| `js/` | Scala.js platform implementations and tests | yes |
| `examples-js/` | interactive Vite and Scala.js application | no |
| `examples/` | runnable JVM examples | no |
| `scan-kyo/` | experimental scan-algebra comparison | no |
| `bench/`, `bench-fs2/` | JMH projects | no |
| `legacy/` | archived fs2-pdf source for provenance | no |

## Release process

Tags matching `v*` run the complete test and package proof before `sbt-ci-release` signs and uploads both JVM and Scala.js artifacts. The workflow fails if Sonatype or PGP credentials are absent. It reads both coordinates back from Maven Central before creating the GitHub release.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
