/*
 * Classification of [[Decoded]] into [[Element]] (attachments, etc.).
 */

package zio.pdf

import zio.*
import zio.stream.*
import zio.test.*

object ElementsSpec extends ZIOSpecDefault {

  private val payloadBytes: Chunk[Byte] = Chunk.fromArray("test".getBytes("UTF-8"))

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

  private def embeddedFilePart: Part[Trailer] =
    Part.StreamObj(
      index = Obj.Index(4, 0),
      data = Prim.dict(
        "Type"   -> Prim.Name("EmbeddedFile"),
        "Params" -> Prim.dict("Size" -> Prim.Number(BigDecimal(payloadBytes.size)))
      ),
      length = payloadBytes.size.toLong,
      payload = ZStream.fromChunk(payloadBytes)
    )

  private val trailer: Trailer =
    Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

  private def embeddedFilePdfBytes: ZIO[Any, Throwable, Chunk[Byte]] =
    ZStream(
      Part.Obj(catalog),
      Part.Obj(pages),
      Part.Obj(page),
      embeddedFilePart,
      Part.Meta(trailer): Part[Trailer]
    ).via(WritePdf.parts)
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))

  def spec: Spec[Any, Throwable] = suite("Elements")(
    test("Content /Type /EmbeddedFile is EmbeddedFileStream") {
      for {
        bytes <- embeddedFilePdfBytes
        elems <- ZStream.fromChunk(bytes).via(PdfStream.elements()).runCollect
        contents = elems.collect { case c: Element.Content => c }
      } yield assertTrue(
        contents.exists {
          case Element.Content(_, _, _, Element.ContentKind.EmbeddedFileStream(meta)) =>
            meta.data.contains("Params")
          case _ => false
        }
      )
    },
    test("mapContentElements runs only on Content") {
      val pipe = PdfStream.elements() >>>
        PdfStream.mapContentElements { c =>
          ZStream.succeed(c.copy(kind = Element.ContentKind.General))
        }
      for {
        bytes <- embeddedFilePdfBytes
        out   <- ZStream.fromChunk(bytes).via(pipe).runCollect
        metas    = out.collect { case m: Element.Meta => m }
        contents = out.collect { case c: Element.Content => c.kind }
      } yield assertTrue(
        metas.nonEmpty,
        contents.nonEmpty,
        contents.forall(_ == Element.ContentKind.General)
      )
    }
  )
}
