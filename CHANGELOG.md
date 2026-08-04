# Changelog

All notable changes to this project are documented here.

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

- Public `warp` / `sicko` / `uring` aliases; Hyperdrive internals are `private[pdf]`

### Migration from fs2-pdf

| fs2-pdf | zio-pdf |
|---|---|
| `PdfStream.decode(log)` | `PdfEngine.decode(path)` or `PdfStream.decode()` on `ZStream[Byte]` |
| `fs2.io.readAll` | `PdfIO.readAll` / `PdfIO.reader` |
| `IO` / `F` | `ZIO` + `PdfEngine.live` layer |

Legacy fs2 sources remain in `legacy/` for reference only.
