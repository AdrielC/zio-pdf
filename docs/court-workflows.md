# Court filing workflows (0.2.1 APIs)

Recipes for common e-filing and clerk-facing PDF pipelines using `PdfPrep` programs and `PdfEngine` write helpers. All examples assume an **unencrypted** PDF; encrypted inputs fail closed.

## Serializable prep programs

`PdfPrep.Program` derives `zio.blocks.schema.Schema` and round-trips through JSON or `DynamicValue`. Persist a program in your case-management system, then apply it when a docket PDF arrives:

```scala
import java.time.LocalDate
import zio.*
import zio.pdf.*

val ecfProgram = PdfPrep.Program.of(
  PdfPrep.Op.SetFieldValues(
    List(
      PdfPrep.FieldValue("Attorney", "Jane Doe"),
      PdfPrep.FieldValue("CaseNumber", "1:26-cv-00123")
    )
  ),
  PdfPrep.Op.FlattenForms,
  PdfPrep.Op.DateStamp(PdfPrep.StampDate(PdfPrep.DateSource.Today, pattern = "yyyy-MM-dd")),
  PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "EX-", start = 1, width = 6)),
  PdfPrep.Op.Watermark(
    PdfPrep.WatermarkText(text = "FILED", placement = PdfPrep.Placement.TopCenter, fontSize = Some(24))
  ),
  PdfPrep.Op.Linearize
)

def fileAndServe(docket: Chunk[Byte]): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
  PdfEngine.applyPrep(docket, ecfProgram)
```

Store the JSON once:

```scala
val json = PdfPrep.toJson(ecfProgram)
// later
val program = PdfPrep.fromJson(json).toOption.get
```

Use `PdfPrep.profile(program)` to see operation names and whether the program writes page content or the catalog before running it on a large upload.

## ECF file-and-serve

Typical CM/ECF export: fill any remaining form fields, flatten widgets into page content, stamp the filing date, add exhibit Bates numbers, mark as filed, then linearize for the clerk portal.

```scala
val program = PdfPrep.Program.of(
  PdfPrep.Op.SetFieldValues(List(PdfPrep.FieldValue("Attorney", attorneyName))),
  PdfPrep.Op.FlattenForms,
  PdfPrep.Op.DateStamp(PdfPrep.StampDate(PdfPrep.DateSource.Today)),
  PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "EX-", start = batesStart, width = 6)),
  PdfPrep.Op.Watermark(PdfPrep.WatermarkText("FILED")),
  PdfPrep.Op.Linearize
)
```

`SetFieldValues` sets `/V` by qualified AcroForm name (for example `Address.Street`) and removes widget `/AP` so flatten uses the new text. Run `FlattenForms` immediately after filling.

Inspect the form inventory first when field names are unknown:

```scala
for {
  decoded   <- PdfEngine.decode(filing)
  inventory <- ZIO.succeed(PdfAcroForm.extract(decoded))
  _         <- ZIO.logInfo(inventory.fields.map(f => s"${f.name.getOrElse("?")}=${f.value.getOrElse("")}").mkString(", "))
} yield ()
```

## Exhibit binder

Merge multiple exhibit PDFs, apply continuous Bates labels, and set logical page labels for the index:

```scala
import zio.stream.ZStream

for {
  merged <- PdfEngine.mergeBytes(NonEmptyChunk(exhibitA, exhibitB, exhibitC))
  program = PdfPrep.Program.of(
    PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "EX-", start = 100, width = 4)),
    PdfPrep.Op.SetPageLabels(PdfPrep.PageLabels(prefix = "Exhibit ", start = 1))
  )
  binder <- PdfEngine.applyPrep(merged, program)
} yield binder
```

## Append certificate of service

Append an incremental revision without rewriting the original bytes. The prefix is preserved; the new trailer carries `/Prev`.

```scala
val certificate = Chunk(
  Part.Obj(IndirectObj.nostream(500L, Prim.dict("Producer" -> Prim.Name("zio-pdf-cos")))),
  Part.Meta(Trailer(BigDecimal(501), Prim.dict("Info" -> Prim.Ref(500L, 0)), None))
)

for {
  served <- PdfAppend.append(filedMotion, certificate)
  // optional: linearize the combined file for web viewing
  web    <- PdfLinearize.fromBytes(served)
} yield web
```

Double append (for example motion + certificate + proof of service) chains multiple `/Prev` entries. Each append only adds objects; earlier revisions stay byte-identical.

Read the last trailer without a full decode when building the next revision:

```scala
val lastTrailer = PdfAppend.trailerFromTail(existing)
```

## Redacted public version

Apply redaction boxes before stamps so Bates and date marks stay extractable on unredacted pages. Redaction blanks show-text on affected pages.

```scala
val publicProgram = PdfPrep.Program.of(
  PdfPrep.Op.RedactBoxes(
    PdfPrep.Redact(
      boxes = List(
        PdfPrep.RedactRect(page = 3, x = 72, y = 680, width = 200, height = 18),
        PdfPrep.RedactRect(page = 3, x = 72, y = 650, width = 160, height = 18)
      ),
      stripShowText = true
    )
  ),
  PdfPrep.Op.Watermark(PdfPrep.WatermarkText("PUBLIC VERSION", opacity = 0.35)),
  PdfPrep.Op.Linearize
)
```

Apply stamps **after** redaction when they must remain searchable on pages that were not redacted.

## Portal upload gate

Validate structure and record evidence before accepting a filing:

```scala
for {
  validation <- PdfEngine.validate(ZStream.fromChunk(upload))
  bundle     <- PdfEngine.evidence(upload).provide(PdfEngine.live)
} yield (validation, bundle)
```

Encrypted PDFs produce `PdfEvidence.ProcessingBlocker.Encrypted`; merge, linearize, append, watermark, and prep programs reject encrypted inputs before rewrite.

## Web viewing package

Linearize for fast first-page display and attach first-page thumbnails for case-management previews:

```scala
val webProgram = PdfPrep.Program.of(
  PdfPrep.Op.AttachThumbnail(PdfPrep.ThumbnailScope.FirstPageOnly),
  PdfPrep.Op.Linearize
)

// or imperatively, with a JVM renderer at the platform edge:
import zio.pdf.examples.PdfBoxRenderer

val withRealPixels =
  PdfThumbnail.enrichBytes(
    filing,
    PdfThumbnail.Options(
      scope = PdfThumbnail.Scope.FirstPageOnly,
      pixelSource = Some(PdfBoxRenderer.pixelSource(filing.toArray))
    )
  )
```

`PdfLinearize.firstPageByteLength(linearized)` returns the byte offset after the first page for range-request CDNs.

## Runnable example

```bash
export INPUT_PDF=src/test/resources/court-corpus/govinfo-district-court-order.pdf
export OUTPUT_PDF=/tmp/court-filing-prep.pdf
sbt 'examples/runMain zio.pdf.examples.CourtFilingPrep'
```

## Test corpus

Six immutable public court PDFs live under [`src/test/resources/court-corpus/`](../src/test/resources/court-corpus/README.md), including a GovInfo district-court order with an AcroForm. CI exercises merge, append, linearize, flatten, and prep programs against these fixtures.
