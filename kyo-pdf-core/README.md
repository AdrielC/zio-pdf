# kyo-pdf core

The canonical Kyo-native PDF core. Main sources depend on Kyo only: typed
failures use `Abort[PdfError]`, parsing uses `kyo-parse`, and public data models
derive `kyo-schema` schemas.

The initial internal release provides bounded structural inspection, classic
xref resolution for indirect stream lengths, bounded Flate object-stream
decoding, encrypted-document rejection, content-token parsing, linearization
boundary parsing, and renderer-neutral thumbnail object construction.

The sibling `kyo-pdf-zio` artifact is the compatibility edge for existing ZIO
applications. ZIO does not leak into this core artifact.
