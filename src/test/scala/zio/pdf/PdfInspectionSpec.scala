package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.bits.BitVector
import zio.blocks.chunk.ChunkMap
import zio.Chunk
import zio.stream.ZStream
import zio.test.*

object PdfInspectionSpec extends ZIOSpecDefault:

  private def data(number: Long, prim: Prim): Element =
    Element.Data(Obj(Obj.Index(number, 0), prim), Element.DataKind.General)

  private val linearizedElement = data(1L, Prim.dict("Linearized" -> Prim.Number(BigDecimal(1))))
  private val thumb = data(2L, Prim.dict("Thumb" -> Prim.Ref(99L, 0)))
  private val metadata =
    Element.Content(
      Obj(
        Obj.Index(3L, 0),
        Prim.dict("Type" -> Prim.Name("Metadata"), "Subtype" -> Prim.Name("XML"))
      ),
      BitVector.empty,
      Uncompressed.now(
        BitVector(
          "<pdfaid:part>3</pdfaid:part><pdfaid:conformance>B</pdfaid:conformance>"
            .getBytes(StandardCharsets.US_ASCII)
        )
      ),
      Element.ContentKind.General
    )
  private val harmless = data(4L, Prim.dict("Type" -> Prim.Name("Catalog")))
  private val javaScript = data(
    5L,
    Prim.dict("OpenAction" -> Prim.dict("S" -> Prim.Name("JavaScript"), "JS" -> Prim.str("app.alert(1)")))
  )
  private val image =
    Element.Content(
      Obj(Obj.Index(6L, 0), Prim.dict("Type" -> Prim.Name("XObject"), "Subtype" -> Prim.Name("Image"))),
      BitVector.empty,
      Uncompressed.now(BitVector.empty),
      Element.ContentKind.Image(Image(Prim.Dict.empty, Uncompressed.now(BitVector.empty), Image.Codec.Flate))
    )
  private val attachment =
    Element.Content(
      Obj(Obj.Index(7L, 0), Prim.dict("Type" -> Prim.Name("EmbeddedFile"))),
      BitVector.empty,
      Uncompressed.now(BitVector.empty),
      Element.ContentKind.EmbeddedFileStream(Prim.Dict.empty)
    )
  private val tableCandidate =
    Element.Content(
      Obj(Obj.Index(8L, 0), Prim.Dict.empty),
      BitVector.empty,
      Uncompressed.now(BitVector("10 20 30 40 re BT (cell) Tj ET".getBytes(StandardCharsets.US_ASCII))),
      Element.ContentKind.General
    )
  private val tableLookalike =
    Element.Content(
      Obj(Obj.Index(9L, 0), Prim.Dict.empty),
      BitVector.empty,
      Uncompressed.now(BitVector("BT [(re 10 20 30 40) -120 (Tj)] TJ ET".getBytes(StandardCharsets.US_ASCII))),
      Element.ContentKind.General
    )
  private val encryptedTrailer =
    Element.Meta(
      Some(Trailer(BigDecimal(9), Prim.dict("Encrypt" -> Prim.Ref(77L, 0)), Some(Prim.Ref(1L, 0)))),
      None
    )
  private val simpleFont = data(
    10L,
    Prim.dict(
      "Type" -> Prim.Name("Font"),
      "Subtype" -> Prim.Name("TrueType"),
      "BaseFont" -> Prim.Name("ABCDEE+SourceFace")
    )
  )
  private val compositeFont = data(
    11L,
    Prim.dict(
      "Type" -> Prim.Name("Font"),
      "Subtype" -> Prim.Name("Type0"),
      "BaseFont" -> Prim.Name("Identity-H")
    )
  )
  private val compositeDescendant = data(
    12L,
    Prim.dict(
      "Type" -> Prim.Name("Font"),
      "Subtype" -> Prim.Name("CIDFontType2"),
      "BaseFont" -> Prim.Name("Identity-H")
    )
  )

  private def deeplyNestedJavaScript(depth: Int): Prim =
    var nested: Prim = Prim.dict("S" -> Prim.Name("JavaScript"), "JS" -> Prim.str("app.alert(1)"))
    var remaining = depth
    while remaining > 0 do
      nested = Prim.Dict(ChunkMap("Next" -> nested))
      remaining -= 1
    nested

  def spec: Spec[Any, Throwable] = suite("PdfInspection")(
    test("positive observations compose as an inspectable plan and stop once all are present") {
      import PdfInspection.*

      val plan = PdfInspection.linearized >>> PdfInspection.thumbnail >>> PdfInspection.pdfA
      PdfInspection.run(ZStream(linearizedElement, thumb, metadata, harmless), plan).map {
        case PdfInspection.Outcome.Accepted(report) =>
          assertTrue(
            report.linearization.contains(PdfInspection.Linearization(1L)),
            report.thumbnail.contains(PdfInspection.Thumbnail(2L, Prim.Ref(99L, 0))),
            report.pdfA.contains(PdfInspection.PdfA(3L, Some("3"), Some("B"))),
            report.pdfA.exists(_.declaresA3b),
            report.elementsRead == 3L,
            report.completion == PdfInspection.Completion.SatisfiedEarly
          )
        case _ => assertTrue(false)
      }
    },
    test("derives the consumption profile from the embedded operations") {
      import PdfInspection.*

      val plan = PdfInspection.linearized >>> PdfInspection.thumbnail >>> PdfInspection.forbidJavaScript
      val profile = PdfInspection.profile(plan)

      assertTrue(
        profile.required == Set(PdfInspection.Finding.Linearized, PdfInspection.Finding.Thumbnail),
        profile.requiresFullScan,
        plan.size == 3
      )
    },
    test("forbidJavaScript rejects immediately and retains the partial report") {
      PdfInspection.run(ZStream(javaScript, harmless), PdfInspection.forbidJavaScript).map {
        case PdfInspection.Outcome.Rejected(report, PdfInspection.Violation.JavaScript(found)) =>
          assertTrue(
            found == PdfInspection.JavaScript(5L),
            report.javaScript.contains(found),
            report.elementsRead == 1L,
            report.completion == PdfInspection.Completion.RejectedEarly
          )
        case _ => assertTrue(false)
      }
    },
    test("an absence policy reads through the source even after an observation succeeds") {
      import PdfInspection.*

      val plan = PdfInspection.linearized >>> PdfInspection.forbidJavaScript
      PdfInspection.run(ZStream(linearizedElement, harmless), plan).map {
        case PdfInspection.Outcome.Accepted(report) =>
          assertTrue(
            report.linearization.contains(PdfInspection.Linearization(1L)),
            report.elementsRead == 2L,
            report.completion == PdfInspection.Completion.EndOfInput
          )
        case _ => assertTrue(false)
      }
    },
    test("finds JavaScript through deeply nested PDF values without consuming the call stack") {
      val nested = data(6L, deeplyNestedJavaScript(10000))

      PdfInspection.run(ZStream(nested), PdfInspection.forbidJavaScript).map {
        case PdfInspection.Outcome.Rejected(report, PdfInspection.Violation.JavaScript(found)) =>
          assertTrue(
            found == PdfInspection.JavaScript(6L),
            report.elementsRead == 1L,
            report.completion == PdfInspection.Completion.RejectedEarly
          )
        case _ => assertTrue(false)
      }
    },
    test("documentProfile composes structural facts and full-scan counts without a second API") {
      import PdfInspection.*

      PdfInspection
        .run(ZStream(image, attachment, tableCandidate, tableLookalike, encryptedTrailer), PdfInspection.documentProfile)
        .map {
          case PdfInspection.Outcome.Accepted(report) =>
            assertTrue(
              report.encryption.contains(PdfInspection.Encryption(Some(Prim.Ref(77L, 0)))),
              report.imageCount == 1L,
              report.attachmentCount == 1L,
              report.tableCandidateCount == 1L,
              report.imageCountEvidence.confidence == PdfInspection.Confidence.Structural,
              report.tableCandidateCountEvidence.confidence == PdfInspection.Confidence.Heuristic,
              report.elementsRead == 5L,
              report.completion == PdfInspection.Completion.EndOfInput,
              PdfInspection.profile(PdfInspection.documentProfile).requiresFullScan
            )
          case _ => assertTrue(false)
        }
    },
    test("fontInventory retains real BaseFont resources without reading content streams") {
      PdfInspection.run(ZStream(simpleFont, compositeFont, compositeDescendant), PdfInspection.fontInventory).map {
        case PdfInspection.Outcome.Accepted(report) =>
          assertTrue(
            report.fonts == Chunk(
              PdfInspection.Font(10L, "ABCDEE+SourceFace", Some("TrueType")),
              PdfInspection.Font(11L, "Identity-H", Some("Type0")),
              PdfInspection.Font(12L, "Identity-H", Some("CIDFontType2"))
            ),
            report.fonts.head.isExistingResourceRemapCandidate,
            report.fonts(1).isExistingResourceRemapCandidate,
            !report.fonts(2).isExistingResourceRemapCandidate,
            report.elementsRead == 3L,
            report.completion == PdfInspection.Completion.EndOfInput,
            PdfInspection.profile(PdfInspection.fontInventory).requiresFullScan
          )
        case _ => assertTrue(false)
      }
    },
    test("forbidEncrypted rejects a trailer /Encrypt entry immediately") {
      PdfInspection.run(ZStream(encryptedTrailer, harmless), PdfInspection.forbidEncrypted).map {
        case PdfInspection.Outcome.Rejected(report, PdfInspection.Violation.Encrypted(found)) =>
          assertTrue(
            found == PdfInspection.Encryption(Some(Prim.Ref(77L, 0))),
            report.encryption.contains(found),
            report.elementsRead == 1L,
            report.completion == PdfInspection.Completion.RejectedEarly
          )
        case _ => assertTrue(false)
      }
    },
    test("acroForm observes catalog and field dictionaries") {
      val catalog = data(
        20L,
        Prim.dict("Type" -> Prim.Name("Catalog"), "AcroForm" -> Prim.Ref(21L, 0))
      )
      val form = data(
        21L,
        Prim.dict("Fields" -> Prim.Array(Prim.Ref(22L, 0)), "NeedAppearances" -> Prim.Bool(true))
      )
      PdfInspection.run(ZStream(catalog, form), PdfInspection.acroForm).map {
        case PdfInspection.Outcome.Accepted(report) =>
          assertTrue(
            report.acroForm.exists(_.fieldCount == 1),
            report.acroForm.exists(_.needAppearances),
            report.completion == PdfInspection.Completion.EndOfInput
          )
        case _ => assertTrue(false)
      }
    }
  )
