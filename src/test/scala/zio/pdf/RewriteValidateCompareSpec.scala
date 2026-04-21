/*
 * End-to-end tests of the higher-level pipes:
 *   - Rewrite turning Decoded back into Part[Trailer]
 *   - PdfStream.validate against a constructed PDF
 *   - PdfStream.compare comparing the same PDF twice (should pass)
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.prelude.Validation
import zio.stream.*
import zio.test.*

object RewriteValidateCompareSpec extends ZIOSpecDefault {

  // Build a minimal valid PDF with catalog -> pages -> page -> content.
  private val pdfBytesZ: ZIO[Any, Throwable, ByteVector] = {
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
        "MediaBox" -> Prim.Array(
            Prim.Number(BigDecimal(0)),
            Prim.Number(BigDecimal(0)),
            Prim.Number(BigDecimal(612)),
            Prim.Number(BigDecimal(792))
          
        ),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(
      4,
      Prim.dict(),
      BitVector("BT /F1 24 Tf 100 700 Td (hi) Tj ET\n".getBytes)
    )
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content)
      .via(WritePdf.objects(trailer))
      .runFold(ByteVector.empty)(_ ++ _)
  }

  def spec: Spec[Any, Throwable] = suite("Rewrite + Validate + Compare")(

    test("Decoded.parts can re-encode a decoded PDF byte-for-byte structurally") {
      for {
        bytes    <- pdfBytesZ
        // Decode -> parts -> WritePdf.parts.
        roundOut <- ZStream
                      .fromChunk(Chunk.fromArray(bytes.toArray))
                      .via(PdfStream.decode(Log.noop))
                      .via(Decoded.parts)
                      .via(WritePdf.parts)
                      .runFold(ByteVector.empty)(_ ++ _)
        // Re-decode the round-tripped bytes to confirm structural validity.
        decoded2 <- ZStream
                      .fromChunk(Chunk.fromArray(roundOut.toArray))
                      .via(PdfStream.decode(Log.noop))
                      .runCollect
        data2     = decoded2.collect { case Decoded.DataObj(o)        => o }
        ctnt2     = decoded2.collect { case Decoded.ContentObj(o, _, _) => o }
      } yield assertTrue(
        // 3 data objects + 1 content stream after round-trip.
        data2.size == 3,
        ctnt2.size == 1
      )
    },

    test("PdfStream.validate succeeds on a valid catalog -> pages -> page -> content PDF") {
      for {
        bytes <- pdfBytesZ
        v     <- PdfStream.validate(Log.noop)(ZStream.fromChunk(Chunk.fromArray(bytes.toArray)))
      } yield assertTrue(v.isSuccess)
    },

    test("PdfStream.compare on two identical PDFs reports no differences") {
      for {
        bytes <- pdfBytesZ
        cmp   <- PdfStream.compare(Log.noop)(
                   ZStream.fromChunk(Chunk.fromArray(bytes.toArray)),
                   ZStream.fromChunk(Chunk.fromArray(bytes.toArray))
                 )
      } yield assertTrue(cmp.isSuccess)
    }
  )
}
