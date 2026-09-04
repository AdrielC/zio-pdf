package zio.pdf

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets
import java.time.LocalDate

import _root_.scodec.bits.BitVector
import zio.*
import zio.blocks.chunk.Chunk as BlocksChunk
import zio.test.*

object PdfPrepSpec extends ZIOSpecDefault {

  private def singlePageParts(label: String): Chunk[Part[Trailer]] = {
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0)))
    Chunk(
      Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0)))),
      Part.Obj(
        IndirectObj.nostream(
          2L,
          Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
        )
      ),
      Part.Obj(
        IndirectObj.nostream(
          3L,
          Prim.dict(
            "Type"     -> Prim.Name("Page"),
            "Parent"   -> Prim.Ref(2L, 0),
            "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
            "Contents" -> Prim.Ref(4L, 0)
          )
        )
      ),
      Part.Obj(IndirectObj.stream(4L, Prim.dict(), BitVector(s"BT 72 720 Td ($label) Tj ET\n".getBytes))),
      Part.Meta(trailer)
    )
  }

  private def singlePagePdf(label: String): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfEngine.writeBytes(singlePageParts(label))

  private def fakeTrueType: Array[Byte] = {
    def table(tag: String, payload: Array[Byte]): (String, Array[Byte]) = tag -> payload
    val head = Array.fill(54)(0.toByte)
    putShort(head, 18, 1000)
    putShort(head, 36, 0)
    putShort(head, 38, -200)
    putShort(head, 40, 800)
    putShort(head, 42, 800)
    val hhea = Array.fill(36)(0.toByte)
    putShort(hhea, 4, 800)
    putShort(hhea, 6, -200)
    putShort(hhea, 34, 1)
    val hmtx = Array[Byte](0x01, 0xf4.toByte, 0x00, 0x00)
    val os2  = Array.fill(90)(0.toByte)
    putShort(os2, 88, 700)
    assembleTrueType(List(table("head", head), table("hhea", hhea), table("hmtx", hmtx), table("OS/2", os2)))
  }

  private def putShort(bytes: Array[Byte], offset: Int, value: Int): Unit = {
    bytes(offset) = ((value >> 8) & 0xff).toByte
    bytes(offset + 1) = (value & 0xff).toByte
  }

  private def assembleTrueType(tables: List[(String, Array[Byte])]): Array[Byte] = {
    val count  = tables.size
    val header = 12 + count * 16
    var offset = header
    val directory = tables.map { (tag, payload) =>
      val aligned = payload.length
      val start   = offset
      offset += aligned
      (tag, start, payload)
    }
    val out = ByteBuffer.allocate(offset).order(ByteOrder.BIG_ENDIAN)
    out.putInt(0x00010000)
    out.putShort(count.toShort)
    out.putShort(0)
    out.putShort(0)
    out.putShort(0)
    directory.foreach { (tag, start, payload) =>
      tag.getBytes(StandardCharsets.US_ASCII).take(4).foreach(b => out.put(b))
      out.putInt(0)
      out.putInt(start)
      out.putInt(payload.length)
    }
    directory.foreach { (_, _, payload) => out.put(payload) }
    out.array()
  }

  def spec: Spec[Any, Throwable] = suite("PdfPrep")(
    test("program JSON and DynamicValue round-trip") {
      val program = PdfPrep.Program.of(
        PdfPrep.Op.DateStamp(PdfPrep.StampDate(PdfPrep.DateSource.Fixed("2026-09-04"))),
        PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "EX-", start = 100, width = 4)),
        PdfPrep.Op.SetPageLabels(PdfPrep.PageLabels(prefix = "A-", start = 1)),
        PdfPrep.Op.RedactBoxes(
          PdfPrep.Redact(List(PdfPrep.RedactRect(1, 72, 72, 120, 24)), stripShowText = true)
        ),
        PdfPrep.Op.Watermark(
          PdfPrep.WatermarkText(
            text = "FILED",
            placement = PdfPrep.Placement.TopCenter,
            rotationDegrees = 15,
            fontSize = Some(28)
          )
        )
      )
      val json   = PdfPrep.toJson(program)
      val back   = PdfPrep.fromJson(json)
      val dynam  = PdfPrep.fromDynamicValue(PdfPrep.toDynamicValue(program))
      assertTrue(
        back == Right(program),
        dynam == Right(program),
        json.contains("DateStamp"),
        json.contains("Bates"),
        json.contains("SetPageLabels"),
        json.contains("RedactBoxes")
      )
    },
    test("apply serializable date, Bates, page-label, and redact program") {
      val program = PdfPrep.Program.of(
        PdfPrep.Op.RedactBoxes(
          PdfPrep.Redact(List(PdfPrep.RedactRect(1, 70, 700, 80, 20)), stripShowText = true)
        ),
        PdfPrep.Op.DateStamp(
          PdfPrep.StampDate(
            source = PdfPrep.DateSource.Fixed("2026-09-04"),
            pattern = "yyyy-MM-dd",
            style = PdfPrep.TextStyle(placement = PdfPrep.Placement.TopRight, fontSize = 11)
          )
        ),
        PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "BATES-", start = 12, width = 3)),
        PdfPrep.Op.SetPageLabels(PdfPrep.PageLabels(style = PdfPrep.PageLabelStyle.Decimal, prefix = "P-"))
      )
      for {
        source <- singlePagePdf("secret")
        out    <- PdfPrep.apply(source, program, today = LocalDate.parse("2026-09-04"))
        text    = new String(out.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("(2026-09-04)"),
        text.contains("(BATES-012)"),
        text.contains("/PageLabels"),
        text.contains("/Nums"),
        text.contains(" re f"),
        !text.contains("(secret)")
      )
    },
    test("applyPrep embeds a TrueType font program") {
      val font = PdfPrep.EmbedFont(
        name = "Custom",
        baseFont = "TestSans",
        bytes = BlocksChunk.fromArray(fakeTrueType)
      )
      for {
        source <- singlePagePdf("open")
        out    <- PdfEngine.applyPrep(source, PdfPrep.Program.of(PdfPrep.Op.EmbedTrueType(font)))
        text    = new String(out.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("/Subtype /TrueType"),
        text.contains("/FontFile2"),
        text.contains("/BaseFont /TestSans"),
        text.contains("/Custom")
      )
    },
    test("fromJson rejects unknown JSON") {
      assertTrue(PdfPrep.fromJson("{not-json").isLeft)
    },
    test("apply set-field-values then flatten prep program") {
      val formParts = Chunk(
        Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0), "AcroForm" -> Prim.Ref(5L, 0)))),
        Part.Obj(
          IndirectObj.nostream(
            2L,
            Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            3L,
            Prim.dict(
              "Type"     -> Prim.Name("Page"),
              "Parent"   -> Prim.Ref(2L, 0),
              "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
              "Annots"   -> Prim.Array(Prim.Ref(6L, 0))
            )
          )
        ),
        Part.Obj(IndirectObj.nostream(4L, Prim.dict())),
        Part.Obj(IndirectObj.nostream(5L, Prim.dict("Fields" -> Prim.Array(Prim.Ref(6L, 0))))),
        Part.Obj(
          IndirectObj.nostream(
            6L,
            Prim.dict(
              "Subtype" -> Prim.Name("Widget"),
              "T"       -> Prim.str("Attorney"),
              "FT"      -> Prim.Name("Tx"),
              "Rect"    -> Prim.Array.nums(72, 700, 272, 720)
            )
          )
        ),
        Part.Meta(Trailer(BigDecimal(7), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0))))
      )
      val program = PdfPrep.Program.of(
        PdfPrep.Op.SetFieldValues(List(PdfPrep.FieldValue("Attorney", "Jane Doe"))),
        PdfPrep.Op.FlattenForms,
        PdfPrep.Op.DateStamp(
          PdfPrep.StampDate(
            source = PdfPrep.DateSource.Fixed("2026-09-04"),
            style = PdfPrep.TextStyle(placement = PdfPrep.Placement.TopRight, fontSize = 9)
          )
        )
      )
      for {
        source <- PdfEngine.writeBytes(formParts)
        out    <- PdfPrep.apply(source, program, today = LocalDate.parse("2026-09-04"))
        text    = new String(out.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("(Jane Doe)"),
        text.contains("(2026-09-04)"),
        !text.contains("/AcroForm")
      )
    },
    test("apply attach-thumbnail prep program adds /Thumb") {
      val program = PdfPrep.Program.of(PdfPrep.Op.AttachThumbnail(PdfPrep.ThumbnailScope.FirstPageOnly))
      for {
        source  <- singlePagePdf("thumb-prep")
        out     <- PdfPrep.apply(source, program)
        outcome <- PdfEngine.inspect(out, PdfInspection.thumbnail)
      } yield assertTrue(
        out.size > source.size,
        outcome match {
          case PdfInspection.Outcome.Accepted(report) => report.thumbnail.nonEmpty
          case PdfInspection.Outcome.Rejected(report, _) => report.thumbnail.nonEmpty
        }
      )
    }
  ).provide(PdfEngine.live)
}
