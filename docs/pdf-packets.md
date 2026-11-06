# Streamed PDF packets

`PdfPacket` stitches complete PDF sources sequentially, preserving page graphics
and fonts. It generates front matter with document titles, final page ranges,
clickable contents entries and one bookmark per document. Contents expand to
multiple pages automatically; their own page count is included in every offset.

The same engine supports two delivery modes:

- `Live`: the first source's objects can reach the consumer before the next
  source is opened. Backpressure suspends stitching. Output becomes a complete
  PDF only after the final xref/trailer/EOF. A later failure terminates the stream;
  consumers must discard its partial bytes.
- `Staged` (the stream adapter default): the engine writes through
  `ZSink.fromPath` to a scoped temporary file. Only successful completion starts
  `ZStream.fromPath`. Failure or cancellation removes the spool, including when
  the download consumer stops early. This mode uses one output spool write and
  one read. Each incoming source stream is consumed once into its own scoped
  seekable file; that file is removed before the next source opens.

The PDF page tree determines reading order independently of physical object
order. The engine emits body objects immediately, then emits cover/contents
objects and a page tree that places them first. It finishes with a fresh xref and trailer (an xref stream with
64-bit offsets when a classic table cannot represent the output). Input bytes are never concatenated as if that would merge PDFs.

```scala
import io.github.adrielc.kyopdf.PdfPacket
import io.github.adrielc.kyopdf.zio.PdfPacketZIO
import zio.stream.{ZSink, ZStream}

val parts = ZStream(
  PdfPacketZIO.Part("Petition", ZStream.fromPath(petitionPath)),
  PdfPacketZIO.Part("Declaration", ZStream.fromPath(declarationPath)),
  PdfPacketZIO.Part("Exhibits", ZStream.fromPath(exhibitsPath))
)

val packet = PdfPacketZIO.stitch(
  parts,
  PdfPacket.Settings("Filing packet"),
  delivery = PdfPacket.Delivery.Live // or Staged
)

val saved = packet.run(ZSink.fromPath(outputPath))
```

Construction is lazy. Each source is spooled to EOF for its cross references and
page tree before its objects are emitted. The index decodes syntax only; ordinary
content, image and font streams stay as file ranges and copy through a bounded
read window. The adapter does not call `decodeStream` or collect an input PDF.
It checks the PDF header during admission, before spooling the rest of an invalid
source. Compressed object streams contain syntax, so their compressed and expanded
sizes are checked against the metadata budget before retaining them.

`PdfPacket.Bounds` provides independent, configurable limits:

- `sourceBytes`: encoded size of each source (default 64 GiB).
- `outputBytes`: total encoded packet size (default 64 GiB).
- `spoolBytes`: simultaneous temporary disk reservations (default 128 GiB).
  Staged delivery charges both accumulated output and the current input, including
  a chunk reserved before its write. Reservations are released on scope exit.
- `memoryBytes`: working-data budget (default 64 MiB), used to bound syntax nodes,
  token payloads, xref slots and page/section bookkeeping. This bounds retained
  application data; it is not an exact JVM heap or process RSS ceiling. Runtime
  overhead and upstream-owned chunks are additional.
- `windowBytes`: seekable input cache (default 64 KiB); output chunks are at most
  64 KiB and own their bytes.
- `documents`, `pages`, `objects`: packet bookkeeping counts, defaulting to 256,
  8192 and 100000. They can be raised when the memory budget can accommodate them.

Byte limits use `Long` and can be raised above the defaults. Invalid limits and
bookkeeping configurations are rejected before allocating session state. A
metadata-heavy PDF can reach its syntax budget even if most of its encoded bytes
fit the source limit; raise `memoryBytes` deliberately for that workload.

```scala
val limits = PdfPacket.Bounds(
  sourceBytes = 8L * 1024 * 1024 * 1024,
  outputBytes = 32L * 1024 * 1024 * 1024,
  spoolBytes = 40L * 1024 * 1024 * 1024,
  memoryBytes = 64L * 1024 * 1024,
  windowBytes = 64 * 1024
)
val packet = PdfPacketZIO.stitch(parts, bounds = limits)
```

The low-level Kyo `PdfPacketSource.open(path, limits)` / `PdfPacket.open` /
`Session.append(title, source)` / `Pull.read` / `Session.finish` API is shared by
JVM and Native. An already seekable input needs no additional input spool. Keep
its file unchanged and its handle open until the append pull reaches EOF, then
close it. File ownership and any disk quota belong to the caller at this level.
Consume each pull fully before appending another source. The completed receipt
reports sections, page counts, output byte length and SHA-256. Close the session
and source handles on failure or interruption. These objects are single-consumer.

`Session.append(title, PdfDocument)` is also available for already materialized,
bounded documents; it does not make their payload vectors file-backed. Use
`PdfPacketSource` or `PdfPacketZIO` for large inputs.

## Serializable graph operations

The standard registry exposes `pdf.packet-2`, `pdf.packet-3` and `pdf.packet-4`.
They consume two, three or four `File` ports in order and return a finished
`File`. Settings are `title` and optional ordered `titles`; defaults use
`Document 1`, `Document 2`, and so on. Ports and settings come from the registry,
and JSON codecs are derived from their schemas.

```scala
import io.github.adrielc.kyopdf.pipeline.*
import PdfArrow.*

val documents = ((input("petition") >>> write) &&&
  (input("declaration") >>> write)) &&& (input("exhibits") >>> write)

val packet = documents >>> registered(PdfPacketSteps.three,
  PdfPacketSteps.Options("Filing packet", Vector("Petition", "Declaration", "Exhibits")))
```

These are ordinary composable arrows. The tool continues to accept JSON, and
its file outputs remain unavailable until every required step succeeds. Compose
conversion and exact-byte validation after packet construction when needed.
These `File` ports are materialized values with a separate 64 MiB limit, so the
registered convenience operations explicitly enforce that output limit. They do
not expose the large-file streaming transport through a byte collection. Use the
stream API for larger artifacts or longer source sequences.

## Navigation and document boundaries

Source page content and resources remain PDF objects. Explicit and named local
links are resolved and remapped to the copied pages. Source document catalogs,
outlines, accessibility tags and document metadata are replaced with the packet's
new catalog and navigation. Source signatures cannot survive a rewrite.
Interactive forms/widgets and unresolved local links fail explicitly; flatten
forms in an earlier operation when that behavior is intended.

Generated labels currently support WinAnsi characters, including Latin-1 text
and common typographic punctuation, with Courier fonts. Unsupported cover-title
characters fail admission rather than becoming replacement glyphs. Source pages
retain their own fonts and character encodings. Packet creation itself does not
establish PDF/A conformance.

## Validation

Focused tests run without external PDF services:

```sh
sbt 'kyoPdf/testOnly *PdfPacketSpec *PdfPacketLargeSpec' \
    'kyoPdfZio/testOnly *PdfPacketZIOSpec' \
    'kyoPdfPipeline/testOnly *PdfPacketStepsSpec'
sbt 'kyoPdfNative/testOnly *PdfPacketSpec *PdfPacketLargeSpec' \
    'kyoPdfPipelineNative/testOnly *PdfPacketStepsSpec'
```

The stream tests check live backpressure and early consumer termination, staged
failure/cancellation cleanup, byte-identical delivery modes, admission failure,
a 64-source transfer with at most one active source, and an 80 MiB staged transfer
from a lazily generated input. Quota tests count concurrent source/output disk
usage and cover interruption, output failures and early header rejection.
The shared JVM/Native tests physically stitch two 96 MiB payloads into an output
exceeding 192 MiB using an 8 MiB working-data budget and 64 KiB windows; they check
all copied payload bytes and the completed output hash. A separate sparse input
exceeding 2 GiB tests indexing and indirect-length offsets without transferring
that payload. The 64-bit xref-entry test is a logical offset encoding check, not
a physical transfer exceeding 10 GB. Packet
tests cover multiple contents pages, inherited resources, named links, output
quotas, catalog version overrides and completed-output hashes. The example's
`test_registered_packet_cover_and_contents_through_tool` exercises the actual
`run_pdf_pipeline` tool; the semantic-tools CI job includes it.

The dedicated `Large PDFs (jvm)` and `Large PDFs (native)` CI jobs execute
`scripts/verify-large-pdfs.sh jvm|native`. The JVM job forks the shared packet
tests with `-Xmx64m`. Both jobs require a physically generated packet over
192 MiB and run qpdf against its completed bytes; a missing test suite or missing
export fails the job. The JVM job also independently checks the 80 MiB staged
transfer and its page count. Temporary exports are removed on exit. The shared
specifications also remain in the full core suites on both platforms.
