/*
 * Flate recompress round-trip, PdfPolicy compliance, literal text extract.
 */

package zio.pdf

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

    test("PdfStream.extractText pipeline") {
      for {
        bytes <- minimalPdfBytes
        pages <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.extractText()).runCollect
      } yield assertTrue(pages.nonEmpty, pages.head.text.contains("hi"))
    }
  )
}
