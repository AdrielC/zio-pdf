/*
 * End-to-end test of the encoder pipeline: build a tiny PDF from
 * scratch, run it through WritePdf.objects, then round-trip it
 * through PdfStream.decode and assert structural equality.
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.stream.*
import zio.test.*

object WriteAndRoundtripSpec extends ZIOSpecDefault {

  // Build a minimal valid PDF: catalog (1) -> pages (2) -> page (3)
  // with one tiny content stream (4).
  private val catalog: IndirectObj = IndirectObj.nostream(
    1,
    Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
  )

  private val pages: IndirectObj = IndirectObj.nostream(
    2,
    Prim.dict(
      "Type"  -> Prim.Name("Pages"),
      "Kids"  -> Prim.Array(List(Prim.Ref(3, 0))),
      "Count" -> Prim.Number(BigDecimal(1))
    )
  )

  private val page: IndirectObj = IndirectObj.nostream(
    3,
    Prim.dict(
      "Type"     -> Prim.Name("Page"),
      "Parent"   -> Prim.Ref(2, 0),
      "MediaBox" -> Prim.Array(
        List(
          Prim.Number(BigDecimal(0)),
          Prim.Number(BigDecimal(0)),
          Prim.Number(BigDecimal(612)),
          Prim.Number(BigDecimal(792))
        )
      ),
      "Contents" -> Prim.Ref(4, 0)
    )
  )

  private val content: IndirectObj = IndirectObj.stream(
    4,
    Prim.dict(),
    BitVector("BT /F1 24 Tf 100 700 Td (hi) Tj ET\n".getBytes)
  )

  private val trailer: Trailer =
    Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

  def spec: Spec[Any, Throwable] = suite("WritePdf + round-trip")(

    test("WritePdf.objects produces a parseable PDF") {
      for {
        bytes <- ZStream(catalog, pages, page, content)
                   .via(WritePdf.objects(trailer))
                   .runFold(ByteVector.empty)(_ ++ _)
        // The output must start with %PDF- and end after %%EOF.
        text   = new String(bytes.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
        // Round-trip through the decoder.
        decoded <- ZStream
                     .fromChunk(Chunk.fromArray(bytes.toArray))
                     .via(PdfStream.decode(Log.noop))
                     .runCollect
        data    = decoded.collect { case Decoded.DataObj(o)        => o }
        ctnt    = decoded.collect { case Decoded.ContentObj(o, _, _) => o }
        metas   = decoded.collect { case m: Decoded.Meta             => m }
      } yield assertTrue(
        text.startsWith("%PDF-"),
        text.contains("%%EOF"),
        // 3 data objects + 1 content stream = 4 objects.
        data.size == 3,
        ctnt.size == 1,
        // Catalog is among them.
        data.exists(o => o.index.number == 1L),
        // Exactly one terminating Meta.
        metas.size == 1
      )
    },

    test("encoded PDF contains an xref + trailer + startxref") {
      for {
        bytes <- ZStream(catalog, pages, page, content)
                   .via(WritePdf.objects(trailer))
                   .runFold(ByteVector.empty)(_ ++ _)
        text   = new String(bytes.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("xref\n"),
        text.contains("trailer\n"),
        text.contains("startxref\n")
      )
    }
  )
}
