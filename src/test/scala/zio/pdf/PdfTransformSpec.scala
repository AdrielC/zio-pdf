package zio.pdf

import _root_.scodec.bits.BitVector
import zio.*
import zio.stream.ZStream
import zio.test.*

object PdfTransformSpec extends ZIOSpecDefault {

  private def font(
    baseFont: String,
    width: Int,
    toUnicode: Option[Long],
    subtype: String = "Type1"
  ): IndirectObj = {
    val data = Prim.dict(
        "Type"      -> Prim.Name("Font"),
        "Subtype"   -> Prim.Name(subtype),
        "BaseFont"  -> Prim.Name(baseFont),
        "Encoding"  -> Prim.Name("WinAnsiEncoding"),
        "FirstChar" -> Prim.Number(BigDecimal(32)),
        "LastChar"  -> Prim.Number(BigDecimal(33)),
        "Widths"    -> Prim.Array(Prim.Number(BigDecimal(width)), Prim.Number(BigDecimal(width)))
      )
    val withCMap = toUnicode.fold(data)(number => Prim.Dict(data.data.updated("ToUnicode", Prim.Ref(number, 0))))
    IndirectObj.nostream(if baseFont == "SourceFace" then 5L else 6L, withCMap)
  }

  private def fontPdf(
    targetWidth: Int = 500,
    includeCMaps: Boolean = false,
    targetCMapDiffers: Boolean = false,
    targetSubtype: String = "Type1"
  ): ZIO[Any, Throwable, Chunk[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
        "Resources" -> Prim.dict(
          "Font" -> Prim.dict(
            "F1" -> Prim.Ref(5, 0),
            "F2" -> Prim.Ref(6, 0)
          )
        ),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(
      4,
      Prim.Dict.empty,
      BitVector("BT /F1 12 Tf (AB) Tj ET\n".getBytes)
    )
    val trailer =
      Trailer(BigDecimal(if includeCMaps then 9 else 7), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    val sourceCMap = BitVector(
      """/CIDInit /ProcSet findresource begin
        |2 beginbfchar
        |<41> <0041>
        |<42> <0042>
        |endbfchar
        |end""".stripMargin.getBytes
    )
    val targetCMap =
      if targetCMapDiffers then
        BitVector(
          """/CIDInit /ProcSet findresource begin
            |2 beginbfchar
            |<41> <0041>
            |<42> <0043>
            |endbfchar
            |end""".stripMargin.getBytes
        )
      else sourceCMap
    val sourceCMapObject = Option.when(includeCMaps)(IndirectObj.stream(7, Prim.Dict.empty, sourceCMap))
    val targetCMapObject = Option.when(includeCMaps)(IndirectObj.stream(8, Prim.Dict.empty, targetCMap))
    val sourceFont = font("SourceFace", 500, sourceCMapObject.map(_.obj.index.number))
    val targetFont = font("TargetFace", targetWidth, targetCMapObject.map(_.obj.index.number), targetSubtype)
    val objects = List(catalog, pages, page, content, sourceFont, targetFont) ++ sourceCMapObject ++ targetCMapObject

    ZStream.fromIterable(objects)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((all, next) => all ++ Chunk.fromArray(next.toArray))
  }

  private def type0Pdf(targetDefaultWidth: Int = 1000): ZIO[Any, Throwable, Chunk[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3, 0)), "Count" -> Prim.Number(BigDecimal(1)))
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
        "Resources" -> Prim.dict("Font" -> Prim.dict("F1" -> Prim.Ref(5, 0), "F2" -> Prim.Ref(6, 0))),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.Dict.empty, BitVector("BT /F1 12 Tf <41> Tj ET\n".getBytes))
    val cmap = BitVector(
      """/CIDInit /ProcSet findresource begin
        |1 beginbfchar
        |<41> <0041>
        |endbfchar
        |end""".stripMargin.getBytes
    )
    def type0(number: Long, baseFont: String, descendant: Long, cmapObject: Long): IndirectObj =
      IndirectObj.nostream(
        number,
        Prim.dict(
          "Type"            -> Prim.Name("Font"),
          "Subtype"         -> Prim.Name("Type0"),
          "BaseFont"        -> Prim.Name(baseFont),
          "Encoding"        -> Prim.Name("Identity-H"),
          "DescendantFonts" -> Prim.Array(Prim.Ref(descendant, 0)),
          "ToUnicode"       -> Prim.Ref(cmapObject, 0)
        )
      )
    def cidFont(number: Long, baseFont: String, defaultWidth: Int): IndirectObj =
      IndirectObj.nostream(
        number,
        Prim.dict(
          "Type"        -> Prim.Name("Font"),
          "Subtype"     -> Prim.Name("CIDFontType2"),
          "BaseFont"    -> Prim.Name(baseFont),
          "CIDSystemInfo" -> Prim.dict(
            "Registry"   -> Prim.str("Adobe"),
            "Ordering"   -> Prim.str("Identity"),
            "Supplement" -> Prim.Number(BigDecimal(0))
          ),
          "DW"          -> Prim.Number(BigDecimal(defaultWidth)),
          "W"           -> Prim.Array.nums(1, 500),
          "CIDToGIDMap" -> Prim.Name("Identity")
        )
      )
    val trailer = Trailer(BigDecimal(11), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(
      catalog,
      pages,
      page,
      content,
      type0(5, "SourceComposite", 7, 9),
      type0(6, "TargetComposite", 8, 10),
      cidFont(7, "SourceComposite", 1000),
      cidFont(8, "TargetComposite", targetDefaultWidth),
      IndirectObj.stream(9, Prim.Dict.empty, cmap),
      IndirectObj.stream(10, Prim.Dict.empty, cmap)
    ).via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((all, next) => all ++ Chunk.fromArray(next.toArray))
  }

  private def pageFontBinding(elements: Chunk[Element]): Option[Prim.Ref] =
    elements.collectFirst {
      case Element.Data(Obj(_, data: Prim.Dict), Element.DataKind.Page(_)) =>
        for {
          resources <- data("Resources").collect { case value: Prim.Dict => value }
          fonts <- resources("Font").collect { case value: Prim.Dict => value }
          binding <- fonts("F1").collect { case value: Prim.Ref => value }
        } yield binding
    }.flatten

  def spec: Spec[Any, Throwable] = suite("PdfTransform")(
    test("exposes a named transform plan and derives its execution profile") {
      val transform =
        PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace") >>>
          PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.characters)
      val plan: PdfTransform.Plan = transform.program
      val profile = PdfTransform.profile(plan)

      assertTrue(
        profile == transform.profile,
        profile.operations == Chunk("remap-existing-fonts", "tokenize-text"),
        profile.requiresMaterializedDocument,
        profile.readsContentStreams
      )
    },
    test("document transforms reject input above their typed materialization bound") {
      val limit   = ByteLimit.fromBytes(8L).toOption.get
      val options = PdfEngine.Options(maxMaterializedDocumentBytes = limit)
      PdfTransform.text
        .tokenize(PdfTransform.text.Tokenizer.characters)
        .run(ZStream.fromChunk(Chunk.fill(9)(0.toByte)), options)
        .either
        .provide(PdfEngine.live)
        .map {
          case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
            assertTrue(observed == 9L)
          case _ => assertTrue(false)
        }
    },
    test("remaps a verified existing font and tokenizes the rewritten document") {
      val pairTokenizer = PdfTransform.text.Tokenizer.from { text =>
        Chunk.fromIterable(text.grouped(2).toList)
      }
      val program =
        PdfTransform.fonts
          .replaceExisting("SourceFace", "TargetFace")
          .andThen(PdfTransform.text.tokenize(pairTokenizer))

      for {
        source <- fontPdf()
        output <- program.run(ZStream.fromChunk(source)).provide(PdfEngine.live)
        rendered <- output.bytes.runCollect
        elements <- PdfEngine.elements(rendered).provide(PdfEngine.live)
        text <- PdfEngine.extractText(ZStream.fromChunk(rendered)).runCollect.provide(PdfEngine.live)
        validation <- PdfEngine.validate(ZStream.fromChunk(rendered)).provide(PdfEngine.live)
        replacement = output.value._1
        tokens = output.value._2
      } yield assertTrue(
        replacement.sourceObjectNumbers == Chunk(5L),
        replacement.targetObjectNumber == 6L,
        replacement.resourceBindingsRewritten == 1L,
        pageFontBinding(elements).contains(Prim.Ref(6, 0)),
        tokens == Chunk(PdfTransform.text.PageTokens(3L, Chunk("AB"))),
        text == Chunk(PageText(3L, "AB")),
        validation.isSuccess
      )
    },
    test("arrow composition keeps only the right-hand observation") {
      val program =
        PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace") >>>
          PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.characters)

      for {
        source <- fontPdf()
        output <- program.run(ZStream.fromChunk(source)).provide(PdfEngine.live)
      } yield assertTrue(
        output.value == Chunk(PdfTransform.text.PageTokens(3L, Chunk('A', 'B')))
      )
    },
    test("streams tokenization without constructing a rewritten document") {
      for {
        source <- fontPdf()
        tokens <- PdfTransform.text
                    .tokenize(ZStream.fromChunk(source), PdfTransform.text.Tokenizer.characters)
                    .runCollect
                    .provide(PdfEngine.live)
      } yield assertTrue(
        tokens == Chunk(PdfTransform.text.PageTokens(3L, Chunk('A', 'B')))
      )
    },
    test("accepts equivalent ToUnicode CMaps stored in distinct objects") {
      val program = PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace")

      for {
        source <- fontPdf(includeCMaps = true)
        output <- program.run(ZStream.fromChunk(source)).provide(PdfEngine.live)
        rendered <- output.bytes.runCollect
        text <- PdfEngine.extractText(ZStream.fromChunk(rendered)).runCollect.provide(PdfEngine.live)
      } yield assertTrue(
        output.value.resourceBindingsRewritten == 1L,
        text == Chunk(PageText(3L, "AB"))
      )
    },
    test("remaps a verified Type0 resource without changing its glyph-code meaning") {
      val program = PdfTransform.fonts.replaceExisting("SourceComposite", "TargetComposite")

      for {
        source <- type0Pdf()
        output <- program.run(ZStream.fromChunk(source)).provide(PdfEngine.live)
        rendered <- output.bytes.runCollect
        elements <- PdfEngine.elements(rendered).provide(PdfEngine.live)
        text <- PdfEngine.extractText(ZStream.fromChunk(rendered)).runCollect.provide(PdfEngine.live)
        validation <- PdfEngine.validate(ZStream.fromChunk(rendered)).provide(PdfEngine.live)
      } yield assertTrue(
        output.value.sourceObjectNumbers == Chunk(5L),
        output.value.targetObjectNumber == 6L,
        output.value.resourceBindingsRewritten == 1L,
        pageFontBinding(elements).contains(Prim.Ref(6, 0)),
        text == Chunk(PageText(3L, "A")),
        validation.isSuccess
      )
    },
    test("rejects a Type0 replacement whose CID default width would change layout") {
      val program = PdfTransform.fonts.replaceExisting("SourceComposite", "TargetComposite")

      for {
        source <- type0Pdf(targetDefaultWidth = 900)
        result <- program.run(ZStream.fromChunk(source)).either.provide(PdfEngine.live)
      } yield assertTrue(
        result match {
          case Left(PdfTransform.Error.IncompatibleFont(5L, 6L, "DW")) => true
          case _                                                        => false
        }
      )
    },
    test("rejects a replacement whose ToUnicode CMap would change text") {
      val program = PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace")

      for {
        source <- fontPdf(includeCMaps = true, targetCMapDiffers = true)
        result <- program.run(ZStream.fromChunk(source)).either.provide(PdfEngine.live)
      } yield assertTrue(
        result match {
          case Left(PdfTransform.Error.IncompatibleFont(5L, 6L, "ToUnicode")) => true
          case _ => false
        }
      )
    },
    test("rejects a replacement whose width table would change layout") {
      val program = PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace")

      for {
        source <- fontPdf(targetWidth = 510)
        result <- program.run(ZStream.fromChunk(source)).either.provide(PdfEngine.live)
      } yield assertTrue(
        result match {
          case Left(PdfTransform.Error.IncompatibleFont(5L, 6L, "Widths")) => true
          case _ => false
        }
      )
    },
    test("rejects a cross-kind replacement before it can rewrite resources") {
      val program = PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace")

      for {
        source <- fontPdf(targetSubtype = "Type0")
        result <- program.run(ZStream.fromChunk(source)).either.provide(PdfEngine.live)
      } yield assertTrue(
        result match {
          case Left(PdfTransform.Error.IncompatibleFont(5L, 6L, "Subtype")) => true
          case _ => false
        }
      )
    }
  )
}
