/*
 * Flate recompress round-trip, PdfPolicy compliance, literal text extract.
 */

package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector
import zio.*
import zio.prelude.Validation
import zio.stream.*
import zio.test.*

object StreamProcessPolicyTextSpec extends ZIOSpecDefault {

  private val contentPayload: BitVector =
    BitVector("BT /F1 24 Tf 100 700 Td (hi) Tj ET\n".getBytes)

  private def minimalPdfBytes: ZIO[Any, Throwable, Array[Byte]] = {
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
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.dict(), contentPayload)
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
      .map(_.toArray)
  }

  private def jsPdfBytes: ZIO[Any, Throwable, Array[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict(
        "Type"       -> Prim.Name("Catalog"),
        "Pages"      -> Prim.Ref(2, 0),
        "OpenAction" -> Prim.dict(
          "S"  -> Prim.Name("JavaScript"),
          "JS" -> Prim.str("app.alert('x');")
        )
      )
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
        "MediaBox" -> Prim.Array.nums(0, 0, 1, 1),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.dict(), BitVector("BT (x) Tj ET\n".getBytes))
    val font = IndirectObj.nostream(
      5,
      Prim.dict(
        "Type"     -> Prim.Name("Font"),
        "Subtype"  -> Prim.Name("Type1"),
        "BaseFont" -> Prim.Name("Courier")
      )
    )
    val trailer =
      Trailer(BigDecimal(6), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content, font)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
      .map(_.toArray)
  }

  def spec: Spec[Any, Throwable] = suite("Stream process / policy / text")(

    test("FlateEncode round-trips through FlateDecode") {
      val raw = BitVector("hello flate roundtrip".getBytes)
      val out = for {
        enc <- FlateEncode(raw)
        dec <- FlateDecode(enc, Prim.dict("DecodeParms" -> Prim.Dict.empty))
      } yield dec
      assertTrue(out.toOption.contains(raw))
    },

    test("FlateEncode is safe under parallel fibers") {
      def attemptZIO[A](a: Attempt[A]): ZIO[Any, Throwable, A] =
        ZIO.fromEither(a.toEither).mapError(e => new RuntimeException(e.messageWithContext))

      val payloads =
        Chunk.fromIterable(0 until 64).map(i => BitVector(s"parallel-flate-$i-${"x" * (i * 17)}".getBytes))
      for {
        encoded <- ZIO.foreachPar(payloads)(p => attemptZIO(FlateEncode(p)))
        decoded <- ZIO.foreachPar(encoded.zip(payloads)) { case (enc, raw) =>
                     attemptZIO(FlateDecode(enc, Prim.dict("DecodeParms" -> Prim.Dict.empty))).map(_ == raw)
                   }
      } yield assertTrue(decoded.forall(identity))
    },

    test("mapUncompressedContent identity preserves content bytes") {
      for {
        bytes <- minimalPdfBytes
        rewritten <- ZStream
          .fromChunk(Chunk.fromArray(bytes))
          .via(PdfStream.mapUncompressedContent()(identity))
          .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
        decoded <- ZStream
          .fromChunk(rewritten)
          .via(PdfStream.decode())
          .runCollect
        content = decoded.collect { case Decoded.ContentObj(_, _, s) => s.exec.toOption }
      } yield assertTrue(
        content.nonEmpty,
        content.forall(_.exists(_.bytes.containsSlice(contentPayload.bytes)))
      )
    },

    test("mapUncompressedContent reports callback failures in the stream error channel") {
      val expected = new IllegalArgumentException("transform rejected content")
      for {
        bytes <- minimalPdfBytes
        exit <- ZStream
                  .fromChunk(Chunk.fromArray(bytes))
                  .via(PdfStream.mapUncompressedContent()(_ => throw expected))
                  .runDrain
                  .exit
      } yield assertTrue(
        exit.causeOption.exists(_.failureOption.contains(expected)),
        exit.causeOption.forall(_.dieOption.isEmpty)
      )
    },

    test("PdfPolicy.strict flags OpenAction JavaScript") {
      for {
        bytes  <- jsPdfBytes
        result <- PdfStream.policy(PdfPolicy.strict)(ZStream.fromChunk(Chunk.fromArray(bytes)))
      } yield assertTrue(
        !result.isSuccess,
        result.fold(
          errs => errs.exists {
            case PolicyViolation.JavaScript(_, _) => true
            case _                                => false
          },
          _ => false
        )
      )
    },

    test("PdfPolicy denyFonts flags BaseFont") {
      import PdfPolicy.dsl.*
      for {
        bytes   <- jsPdfBytes
        decoded <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.decode()).runCollect
        result = PdfPolicy.fromChunk(denyFonts("Courier"))(decoded)
      } yield assertTrue(
        !result.isSuccess,
        result.fold(
          errs => errs.exists {
            case PolicyViolation.DeniedFont(_, "Courier") => true
            case _                                        => false
          },
          _ => false
        )
      )
    },

    test("PdfPolicy.permissive accepts JS catalog") {
      for {
        bytes  <- jsPdfBytes
        result <- PdfStream.policy(PdfPolicy.permissive)(ZStream.fromChunk(Chunk.fromArray(bytes)))
      } yield assertTrue(result.isSuccess)
    },

    test("Policy DSL when/unless and & composition") {
      import PdfPolicy.dsl.*
      for {
        bytes   <- jsPdfBytes
        decoded <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.decode()).runCollect
        // JS present → when(hasJavaScript)(reject) must fail
        gated = PdfPolicy.fromChunk(
          when(hasJavaScript)(reject("js not allowed")) &
            unless(hasEncrypt)(pass)
        )(decoded)
        // disjunction: denyFonts(missing) passes, so overall anyOf passes
        any = PdfPolicy.fromChunk(
          anyOf(denyFonts("MissingFont"), banJavaScript)
        )(decoded)
        // conjunction still sees JS
        both = PdfPolicy.fromChunk(banJavaScript & denyFonts("Courier"))(decoded)
      } yield assertTrue(
        !gated.isSuccess,
        gated.fold(
          errs => errs.exists {
            case PolicyViolation.Custom("js not allowed") => true
            case _                                        => false
          },
          _ => false
        ),
        any.isSuccess, // MissingFont check passes → anyOf succeeds
        !both.isSuccess
      )
    },

    test("Policy DSL ifElse branches on hasEncrypt") {
      import PdfPolicy.dsl.*
      for {
        bytes   <- minimalPdfBytes
        decoded <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.decode()).runCollect
        result = PdfPolicy.fromChunk(
          ifElse(hasEncrypt)(reject("encrypted"), pass)
        )(decoded)
      } yield assertTrue(result.isSuccess)
    },

    test("TextExtract pulls literal Tj string") {
      assertTrue(TextExtract.extractFromBits(contentPayload).contains("hi"))
    },

    test("TextExtract resolves page font ToUnicode CMaps including bfchar and bfrange") {
      val pageIndex = Obj.Index(3L, 0)
      val contentIndex = Obj.Index(4L, 0)
      val fontIndex = Obj.Index(5L, 0)
      val cmapIndex = Obj.Index(6L, 0)
      val page = Page(
        pageIndex,
        Prim.dict(
          "Contents" -> Prim.Ref(contentIndex.number, contentIndex.generation),
          "Resources" -> Prim.dict("Font" -> Prim.dict("F1" -> Prim.Ref(fontIndex.number, fontIndex.generation)))
        ),
        MediaBox(0, 0, 612, 792)
      )
      val cmap =
        """/CIDInit /ProcSet findresource begin
          |2 beginbfchar
          |<01> <0041>
          |<02> <0042>
          |endbfchar
          |1 beginbfrange
          |<03> <04> <0043>
          |endbfrange
          |end""".stripMargin
      val elements = Chunk(
        Element.Data(Obj(pageIndex, page.data), Element.DataKind.Page(page)),
        Element.Data(
          Obj(fontIndex, Prim.dict("Type" -> Prim.Name("Font"), "ToUnicode" -> Prim.Ref(cmapIndex.number, cmapIndex.generation))),
          Element.DataKind.General
        ),
        Element.Content(
          Obj(contentIndex, Prim.Dict.empty),
          BitVector.empty,
          Uncompressed.now(BitVector("BT /F1 12 Tf <01020304> Tj ET".getBytes)),
          Element.ContentKind.General
        ),
        Element.Content(
          Obj(cmapIndex, Prim.Dict.empty),
          BitVector.empty,
          Uncompressed.now(BitVector(cmap.getBytes)),
          Element.ContentKind.General
        )
      )

      assertTrue(TextExtract.fromElements(elements) == Chunk(PageText(pageIndex.number, "ABCD")))
    },

    test("TextExtract.fromElements on written PDF") {
      for {
        bytes <- minimalPdfBytes
        elems <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.elements()).runCollect
        pages = TextExtract.fromElements(elems)
      } yield assertTrue(
        pages.size == 1,
        pages.head.text.contains("hi")
      )
    },

    test("TextExtract tolerates a page stream that cannot be decompressed") {
      val pageIndex = Obj.Index(3L, 0)
      val streamIndex = Obj.Index(4L, 0)
      val page = Page(
        pageIndex,
        Prim.dict("Contents" -> Prim.Ref(streamIndex.number, streamIndex.generation)),
        MediaBox(0, 0, 612, 792)
      )
      val unreadableIfInflated = Uncompressed.lazily(
        Attempt.failure(Err("fontless artwork must not be inflated for literal text extraction"))
      )
      val elements = Chunk(
        Element.Data(Obj(pageIndex, page.data), Element.DataKind.Page(page)),
        Element.Content(
          Obj(streamIndex, Prim.dict("Filter" -> Prim.Name("FlateDecode"))),
          BitVector.empty,
          unreadableIfInflated,
          Element.ContentKind.General
        )
      )

      assertTrue(TextExtract.fromElements(elements) == Chunk(PageText(pageIndex.number, "")))
    },

    test("Elements classifies Flate image XObjects independently of export codec") {
      val image = Decoded.ContentObj(
        Obj(
          Obj.Index(7L, 0),
          Prim.dict(
            "Subtype" -> Prim.Name("Image"),
            "Filter"  -> Prim.Name("FlateDecode"),
            "Width"   -> Prim.Number(BigDecimal(1)),
            "Height"  -> Prim.Number(BigDecimal(1))
          )
        ),
        BitVector.empty,
        Uncompressed.now(BitVector.empty)
      )
      val codec = Elements.classifyOne(image).toOption.collect {
        case Element.Content(_, _, _, Element.ContentKind.Image(found)) => found.codec
      }
      assertTrue(codec.contains(Image.Codec.Flate))
    },

    test("PdfStream.extractText pipeline") {
      for {
        bytes <- minimalPdfBytes
        pages <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.extractText()).runCollect
      } yield assertTrue(pages.nonEmpty, pages.head.text.contains("hi"))
    }
  )
}
