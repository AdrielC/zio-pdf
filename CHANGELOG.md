# Changelog

All notable changes to this project are documented here.

## Unreleased

### Changed

- The PDF preview now bundles PDF.js's translated and polyfilled legacy display layer and worker for mobile Safari compatibility.
- The Scala.js workbench renders the first page from bounded PDF.js range requests while the ZIO PDF evidence scan runs independently in a cancellable worker.
- Browser scan progress now reports actual bytes consumed instead of phase-only activity.
- The workbench now inventories font resources from the uploaded PDF and runs selected font remapping and text tokenization through the real `PdfTransform` engine, with a downloadable result and fail-closed compatibility errors.
- Page dictionaries may omit a direct `/MediaBox` when geometry is inherited from their `/Pages` ancestor, as permitted by the PDF page tree.

## [0.2.0-RC6] — 2026-08-25

Bounded-memory PDF infrastructure for the JVM and browser, released as a verified pair of signed Maven Central artifacts.

### Added

- `PdfObjectScanner`, a dependency-clean incremental API that emits complete indirect-object boundaries without copying content-stream payloads
- configurable structural carry limits with typed `CarryLimit` and `Malformed` failures
- exact `Long` source offsets, including logical offsets beyond 1 TiB
- Scala.js streaming sources, browser-local evidence inspection, and shared JVM/browser hashing
- provenance-recorded public PDF corpus, composable inspection, evidence, transform, and bounded-window diff APIs
- signed Maven Central release workflow, package audit, and external-consumer execution proof
- dependency-free, publicly analyzable inspection and transform plans
- typed `ByteLimit` for explicit file and content-stream materialization
- spec-shaped object-stream decoding from `/N` and `/First`, plus offset-correct object-stream encoding

### Changed

- `StreamingDecode` can emit object-end boundaries while suppressing content events for raw structural scanning
- ZIO 2.1.26, Scala 3.8.4, current ZIO Blocks schema/chunk modules, MediaType 0.0.51, scodec-bits 1.2.5, and Kyo 1.0.0-RC4
- standalone public coordinates are `io.github.adrielc %% zio-pdf`
- the JVM and Scala.js artifacts have no private resolvers, coordinates, or runtime dependencies
- experimental byte-scan and local pure-interpreter code now lives only in the unpublished benchmark module
- FastCDC now processes arbitrarily large upstream chunks through a fixed `maxSize` buffer and bounded input windows
- published modules no longer require downstream projects to compile in Scala experimental mode
- release automation publishes and verifies the JVM and Scala.js coordinates in the same Maven Central deployment

### Safety

- parser carry can be hard-bounded independently of declared content-stream payloads
- stream payloads are skipped by declared length in boundary mode
- high-level stream and decompression allocation is capped by `ByteLimit`
- complete decoded-document collections have an independent typed 256 MiB default bound while streaming entry points remain suitable for larger inputs
- browser preview materialization is admitted only below 64 MiB; the Scala.js analysis path remains Blob-streamed
- hash adapters never substitute a different digest for an unavailable algorithm
- streaming writer lengths and physical xref offsets are verified before a valid trailer is emitted
- the artifact audit now fails closed instead of masking marker matches through a `pipefail`/SIGPIPE interaction

## [0.2.0-RC4] — 2026-08-12

One-pass, provenance-first review bundles for JVM and Scala.js.

### Added

- **`PdfEngine.evidence`** — one raw-byte digest and one decoded event pass
  produce immutable inspection, structural-validation, strict-policy, native
  text, and provenance results.
- **`PdfEvidence.Plan.browser`** for totals and a bounded native-text preview
  without retaining another full page-text copy, plus `Plan.complete` for
  page-to-content-object evidence.
- Platform-specific incremental SHA-256 adapters: `MessageDigest` on the JVM
  and Noble Hashes on Scala.js.
- Corpus assertions and JMH coverage comparing the fused bundle with the
  former five-pass review path.

### Changed

- The Scala.js demo reads a browser `Blob` once for analysis. PDFMe preview
  remains a separate, explicitly timed random-access rendering concern.
- Policy rejection in an evidence bundle drains the document for complete
  validation, digesting, and provenance, then reports
  `RejectedAfterFullScan`; ordinary `PdfEngine.inspect` remains fail-fast.
- Documentation distinguishes shared evidence fanout (`&&&`) from a typed,
  caller-owned native-text-to-OCR recovery choice (`<+>`). Empty text never
  silently causes another document pass.

## [0.2.0-RC2] — 2026-08-12

Second release candidate: court ingestion, Scala.js, and typed inspection plans.

### Added

- Scala.js publication (`zio-pdf_sjs1_3`) with the same streaming `PdfEngine`
  surface, `PdfSource` adapters for `Blob`, WHATWG `ReadableStream`, and
  `Uint8Array`, plus Node/browser Flate and incremental SHA-256 support.
- `PdfEngine.Options.maxInputBytes` and the typed `PdfEngine.InputTooLarge`
  failure for callers that configure a whole-input ceiling for untrusted PDFs.
- **`PdfEngine.inspect`** — composable typed checks (linearization,
  PDF/A XMP, thumbnails, JavaScript policy) on JVM paths and Scala.js sources.
- **Fluent scan runners** for pipeline composition.
- **Public court PDF test corpus** for regression coverage.

### Changed

- Apply the configured `StreamingDecode.Config` consistently to element and
  text pipelines.
- Enforce the input policy on streaming, `Chunk`, fused mmap, sink, text, and
  element entry points without silently truncating data.
- **Decoder transitions** modeled as **ZPure**.
- Dependency upgrade to Scala **3.8.4**, ZIO **2.1.26**, ZIO Blocks **0.017**,
  and Kyo **1.0.0-RC4**.
- Zero compiler warnings under `-Werror`.

### Fixed

- Run the complete root, Kyo, and Scala.js test suites before packaging API documentation.
- Nightly **JMH** workflow runner compatibility and duplicate ScanBench runs.
- Gitea Actions **artifact upload** smoke test and v3 compatibility notes.


## [0.2.0-RC1] — 2026-08-04

First release candidate of the ZIO / Scala 3 rewrite (formerly fs2-pdf).

### Added

- **`PdfEngine`** — ZIO service façade for PDF decode / validate / digest / policy / text
- **Pipeline-first API** — `decoded()`, `elements()`, `streaming()`, `extractTextPipeline()`, `digestSink`, `runValidate`, `runPolicy`, `runDigest`, `compareStreams`
- **Fused fast path** — `decode(path|bytes)`, `elements(path|bytes)`, `sink`, `sinkZIO`, `elementsSink`, `elementsSinkZIO`, `decodeAndDigest`, `elementsStream` (mmap Hyperdrive, hidden from callers)
- **`PdfIO`** — narrowed to file byte I/O (`reader`, `writer`, `readAll`, `writeAll`)
- Inline HyperFuse sink spines for constant-memory fused decode
- ByteFeed driven by ZPure; HyperdriveStream backpressure for fused element streams

### Changed

- Port from cats-effect / fs2 / scodec-stream (Scala 2.13) to **ZIO 2.1.25 / Scala 3.8.3**
- Public byte surface uses **`Chunk[Byte]`** (not `Array[Byte]`)
- Diagnostics via `enableDiagnostics: Boolean` (no separate `Log` trait)

### Removed

- Experimental public aliases; Hyperdrive internals are `private[pdf]`

### Migration from fs2-pdf

| fs2-pdf | zio-pdf |
|---|---|
| `PdfStream.decode(log)` | `PdfEngine.decode(path)` or `PdfStream.decode()` on `ZStream[Byte]` |
| `fs2.io.readAll` | `PdfIO.readAll` / `PdfIO.reader` |
| `IO` / `F` | `ZIO` + `PdfEngine.live` layer |

Legacy fs2 sources remain in `legacy/` for reference only.
