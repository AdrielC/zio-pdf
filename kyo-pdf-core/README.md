# kyo-pdf core

The canonical Kyo-native PDF core. PDF operations use Kyo: typed
failures use `Abort[PdfError]`, public data models derive `kyo-schema` schemas,
and structural/content/object parsers share byte views. The bounded linearization
prefix helper uses `kyo-parse`.

The core provides bounded structural inspection, classic
xref resolution for indirect stream lengths, bounded Flate object-stream
decoding, encrypted-document rejection, content-token parsing, linearization
boundary parsing, and renderer-neutral thumbnail object construction.

The sibling `kyo-pdf-zio` artifact is the compatibility edge for existing ZIO
applications. ZIO effects do not leak into this core artifact; ZIO Blocks Schema is an
effect-independent dependency used by the generic scan executor.

The `io.github.adrielc.scan.Scan` API introduced in 0.7.0 composes generic incremental
transformations with path-dependent state types and a ZIO Blocks Registers
executor. `io.github.adrielc.bytescan.SharedScan` supplies shared byte ranges,
independent cursors, leases and retention budgets. An optional capture-checked
facade prevents borrowed views escaping their callback. These components have no
PDF or effect-library dependency; the pinned public-source Blocks runtime is
available on JVM and Native. See [shared byte scans](../docs/shared-byte-scans.md)
for the portable API and scoped ZIO adapter.

PDF packages live under `io.github.adrielc.kyopdf`. Earlier Kyo pre-release
consumers must update imports and dependency organization and recompile; the
package move changes both source and binary compatibility. See the root
README for source-build coordinates and release availability.

PDF decoding now consumes structural boundary events over the same byte input,
sharing xref Length resolution. ZIO streaming uses owned blocks with early header
and byte-limit failures instead of collecting and flattening the input. See
[parser migration and benchmarks](../docs/pdf-parser-migration.md).
