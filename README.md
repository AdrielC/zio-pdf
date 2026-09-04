# zio-pdf

[![CI](https://github.com/AdrielC/zio-pdf/actions/workflows/ci.yml/badge.svg)](https://github.com/AdrielC/zio-pdf/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

`zio-pdf` is a Scala 3 and ZIO 2 library for incremental PDF parsing, content-addressed ingestion, structural inspection, evidence extraction, and fail-closed rewriting. The same parser and hashing model runs on the JVM and Scala.js.

The library separates two workloads that should not be conflated:

- `PdfObjectScanner` finds indirect-object boundaries with bounded structural carry and never copies stream payloads.
- `PdfEngine` expands PDF objects for validation, text, evidence, transforms, and semantic diffing. Any content-stream allocation is guarded by a typed `ByteLimit`.

That distinction makes the raw path suitable for very large uploads while keeping high-level parsing honest about the memory it needs.

## Project status

Release `0.2.1` is available from Maven Central for both the JVM and Scala.js. The source, tests, [browser playground](https://adrielc.github.io/zio-pdf/), POM metadata, signed-release workflow, and independent-consumer check are public.

```scala
libraryDependencies += "io.github.adrielc" %%  "zio-pdf" % "0.2.1" // JVM
libraryDependencies += "io.github.adrielc" %%% "zio-pdf" % "0.2.1" // Scala.js
```

Only the JVM and Scala.js `zio-pdf` artifacts are publishable. Examples, benchmarks, the browser application, and comparison benches are build-only projects.

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
| owned bytes, object boundaries | `PdfObjectScanner.scan(chunk)` | one carry-bounded window, no Reader copy |
| Blocks `Reader[Byte]` / `Sink`, object boundaries | `PdfObjectScanner.scan` / `sink` | tight `readBytes` pull, no per-item ZIO |
| ZStream of boundary windows | `PdfObjectScanner.streamWindows` | one stream step per pulled window |
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

## Production PDF workflows

`PdfEngine` exposes cross-platform write paths for filings and web delivery. Path-based `merge` is JVM-only; `mergeBytes`, append, linearize, thumbnail enrichment, and structural form flatten work on any platform that can hold the input bytes. Encrypted PDFs are detected and rejected before rewrite.

Merge two filings from owned bytes (JVM or Scala.js):

```scala
val merged =
  PdfEngine
    .mergeBytes(NonEmptyChunk(leftBytes, rightBytes))
    .provide(PdfEngine.live)
```

Merge two filings in path order (JVM):

```scala
import java.nio.file.Path
import zio.*
import zio.pdf.*

val merged =
  PdfEngine
    .merge(NonEmptyChunk(Path.of("left.pdf"), Path.of("right.pdf")))
    .provide(PdfEngine.live)
```

Append an incremental revision without rewriting the original prefix:

```scala
val revision = Chunk(
  Part.Obj(IndirectObj.nostream(100L, Prim.dict("Producer" -> Prim.Name("zio-pdf")))),
  Part.Meta(Trailer(BigDecimal(101), Prim.dict("Info" -> Prim.Ref(100L, 0)), None))
)

val updated = PdfAppend.append(existingBytes, revision)
```

Extract a 1-based page range, split every page, or add `/Rotate`:

```scala
val excerpt   = PdfEngine.extractPages(filing, 3, 7)
val pages     = PdfEngine.splitPages(filing)
val landscape = PdfEngine.rotatePages(filing, 90, 1, 2)
val stamped   = PdfEngine.watermark(filing, PdfWatermark.Text("FILED", diagonal = true))
val draft     = PdfEngine.watermark(
                  filing,
                  PdfWatermark.Text(
                    "DRAFT",
                    font = PdfWatermark.StandardFont.TimesBold,
                    color = PdfWatermark.Color.Rgb(0.8, 0.1, 0.1),
                    opacity = 0.5,
                    placement = PdfWatermark.Placement.BottomRight,
                    diagonal = false
                  )
                )
val logo      = PdfEngine.watermark(
                  filing,
                  PdfWatermark.GrayImage(width = 64, height = 64, pixels = grayPixels, opacity = 0.35, scale = 0.2)
                )

val program = PdfPrep.Program.of(
  PdfPrep.Op.DateStamp(PdfPrep.StampDate(PdfPrep.DateSource.Today)),
  PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "EX-", start = 1, width = 6)),
  PdfPrep.Op.SetPageLabels(PdfPrep.PageLabels(prefix = "A-")),
  PdfPrep.Op.RedactBoxes(PdfPrep.Redact(List(PdfPrep.RedactRect(1, 72, 700, 120, 16))))
)
val json    = PdfPrep.toJson(program)
val applied = PdfEngine.applyPrep(filing, PdfPrep.fromJson(json).toOption.get)
```

Inventory or flatten AcroForm widgets. Fill text fields by qualified name, then flatten so `/V` is baked into page content:

```scala
val filled =
  PdfEngine.setFieldValues(existingBytes, Map("Attorney" -> "Jane Doe", "Address.Street" -> "Main"))
val flattened = PdfEngine.flattenForms(filled)

// or as a persisted prep program:
val program = PdfPrep.Program.of(
  PdfPrep.Op.SetFieldValues(List(PdfPrep.FieldValue("Attorney", "Jane Doe"))),
  PdfPrep.Op.FlattenForms
)
```

Inventory or flatten without filling:

```scala
val inventory = PdfAcroForm.extract(decoded)
val flattened = PdfEngine.flattenForms(existingBytes)
```

Court filing recipes (ECF file-and-serve, exhibit binders, append certificate, redacted public version, portal gate, web viewing) are documented in [`docs/court-workflows.md`](docs/court-workflows.md). Runnable example:

```bash
export INPUT_PDF=filing.pdf OUTPUT_PDF=filed.pdf
sbt 'examples/runMain zio.pdf.examples.CourtFilingPrep'
```

Encrypted filings produce a first-class `PdfEvidence.ProcessingBlocker.Encrypted` record. The library does not decrypt; write workflows fail closed.

Linearize for fast first-page web display while preserving top-level object bytes:

```scala
val linearized = PdfLinearize.fromBytes(existingBytes)
val firstPagePrefix = PdfLinearize.firstPageByteLength(linearized)
```

Attach `/Thumb` image XObjects. The shared API stays renderer-agnostic; inject pixels at the platform edge:

| Platform | Renderer | When to use |
| --- | --- | --- |
| JVM server | [PDFBox](https://pdfbox.apache.org/) via `PdfBoxRenderer.pixelSource` | filing prep, batch linearization with real previews |
| Browser | [PDF.js](https://mozilla.github.io/pdf.js/) canvas → grayscale bytes | workbench export, client-side `/Thumb` without shipping PDFBox |
| Either | `PdfThumbnail.placeholderOptions()` | structure tests, linearized layout proofs, no native deps |

```scala
// Shared — placeholders everywhere (JVM + Scala.js)
val placeholder = PdfThumbnail.placeholderOptions()

// Backend — PDFBox at the application edge (examples/tests only; not in the published jar)
val server = PdfThumbnail.renderedOptions(PdfBoxRenderer.pixelSource(pdfBytes))

// Frontend — PDF.js renders to canvas; pass DeviceGray bytes into the JS export
// ZioPdfDemo.attachFirstPageThumbnail(input, grayPixels, width, height)
val browser = PdfThumbnail.renderedOptions((_, w, h) => Right(canvasGrayBytes.take(w * h)))
```

```scala
val previewable = PdfEngine.withThumbnailsBytes(existingBytes, server)
```

[Gotenberg](https://gotenberg.dev/) fits HTML and office-to-PDF conversion on the server, but it does not rasterize existing PDF pages to pixels. Use PDFBox or Poppler on the backend; use PDF.js on the frontend.

On the JVM, runnable examples live under `examples/`:

```bash
MERGE_LEFT=a.pdf MERGE_RIGHT=b.pdf OUTPUT_PDF=merged.pdf sbt 'examples/runMain zio.pdf.examples.MergeFilings'
INPUT_PDF=doc.pdf OUTPUT_PDF=signed.pdf sbt 'examples/runMain zio.pdf.examples.AppendRevision'
INPUT_PDF=doc.pdf OUTPUT_PDF=web.pdf sbt 'examples/runMain zio.pdf.examples.LinearizeFromFile'
INPUT_PDF=doc.pdf OUTPUT_PDF=thumb.pdf RENDER_THUMBS=true sbt 'examples/runMain zio.pdf.examples.ThumbnailsFromPdf'
bash scripts/benchmark-linearized-first-page.sh /path/to/large.pdf
```

The benchmark script linearizes a file, reports the measured first-page prefix size, and compares a byte-range fetch (`curl -r`) against downloading the full linearized file.

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

- `zio-blocks-schema` 0.0.51 for structural schemas and derivation;
- `zio-blocks-chunk` 0.0.51 for allocation-conscious chunk operations;
- `zio-blocks-mediatype` 0.0.51 for media-type boundaries;
- `zio-blocks-streams` 0.0.51 for pull `Reader` / `Writer` / `Stream` (lifted into `ZStream` / `ZChannel` by `BlocksLift`);
- `zio-blocks-scope` / `zio-blocks-context` / `zio-blocks-config` 0.0.51 for resource handles, typed settings, and `Config.load`;
- `zio-blocks-ringbuffer` 0.0.51 for the cross-platform MPSC mailbox (lock-free on the JVM, sequential on Scala.js).

The hot scan stays inside a Blocks `Reader` / `Sink`. `PdfObjectScanner.scan` / `sink` pull with unboxed `readBytes` and `StreamingDecode.stepChunkBytes` — no ZIO per object. `streamWindows` lifts only at the rim and emits a `Chunk[Boundary]` per window; flatten that only if a caller wants one object per ZStream step. `BlocksLift.fromBytes` is the matching windowed byte lift. The MPSC mailbox is for a real JVM thread boundary, not the same-fiber scan. JVM-only Blocks APIs (virtual threads, NIO sinks) are not used, so the same path ships in the Scala.js jar.

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
sbt -batch ';root/test;scalaJs/test;bench/test'
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
| `examples/` | runnable JVM examples (PDFBox optional for rendered thumbnails) | no |
| `bench/`, `bench-fs2/` | JMH projects | no |
| `legacy/` | archived fs2-pdf source for provenance | no |

## Release process

Tags matching `v*` run the complete test and package proof before `sbt-ci-release` signs and uploads both JVM and Scala.js artifacts. The workflow fails if Sonatype or PGP credentials are absent. It reads both coordinates back from Maven Central before creating the GitHub release.

## License

Apache-2.0. See [`LICENSE`](LICENSE) and [`NOTICE`](NOTICE).
