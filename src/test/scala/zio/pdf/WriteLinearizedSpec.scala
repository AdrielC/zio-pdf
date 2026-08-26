package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.*
import zio.stream.ZStream
import zio.test.*

import java.nio.charset.StandardCharsets

object WriteLinearizedSpec extends ZIOSpecDefault {

  private val trailerData = Prim.dict("Root" -> Prim.Ref(2L, 0))

  private def obj(number: Long, data: Prim.Dict): Part[Trailer] =
    Part.Obj(IndirectObj.nostream(number, data))

  private val catalog = obj(2L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(3L, 0)))
  private val pages = obj(
    3L,
    Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(4L, 0)), "Count" -> Prim.Number(1))
  )
  private val page = obj(
    4L,
    Prim.dict(
      "Type" -> Prim.Name("Page"),
      "Parent" -> Prim.Ref(3L, 0),
      "MediaBox" -> Prim.Array.nums(0, 0, 612, 792)
    )
  )

  def spec: Spec[Any, Throwable] = suite("WriteLinearized")(
    test("streams an fs2-compatible linearized prefix followed by a generated tail xref") {
      for {
        bytes <- ZStream(Part.Version(Version.default): Part[Trailer], catalog, pages, page)
                   .via(WriteLinearized.pipe(trailerData, firstPageCount = 1, totalCount = 4, fileSize = 4096L))
                   .runFold(ByteVector.empty)(_ ++ _)
        decoded <- ZStream.fromChunk(Chunk.fromArray(bytes.toArray)).via(PdfStream.decode()).runCollect
        text = new String(bytes.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("/Linearized"),
        decoded.collect { case Decoded.DataObj(value) => value.index.number } == Chunk(1L, 2L, 3L, 4L),
        decoded.collectFirst { case _: Decoded.Meta => () }.isDefined
      )
    },
    test("rejects an impossible layout before consuming parts") {
      ZStream(catalog)
        .via(WriteLinearized.pipe(trailerData, firstPageCount = 0, totalCount = 1, fileSize = 1L))
        .runDrain
        .either
        .map(result => assertTrue(result.left.exists(_.isInstanceOf[WriteLinearized.InvalidLayout])))
    }
  )
}
