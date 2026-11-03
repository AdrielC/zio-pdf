# kyo-pdf core

The canonical Kyo-native PDF core. Main sources depend on Kyo only: typed
failures use `Abort[PdfError]`, parsing uses `kyo-parse`, and public data models
derive `kyo-schema` schemas.

The core provides bounded structural inspection, classic
xref resolution for indirect stream lengths, bounded Flate object-stream
decoding, encrypted-document rejection, content-token parsing, linearization
boundary parsing, and renderer-neutral thumbnail object construction.

The sibling `kyo-pdf-zio` artifact is the compatibility edge for existing ZIO
applications. ZIO does not leak into this core artifact.

Public packages live under `io.github.adrielc.kyopdf`. Earlier Kyo pre-release
consumers must update imports and dependency organization and recompile; the
package move changes both source and binary compatibility. See the root
README for source-build coordinates and release availability.
