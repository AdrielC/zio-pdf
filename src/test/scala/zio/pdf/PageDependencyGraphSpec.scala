package zio.pdf

import zio.*
import zio.stream.ZStream
import zio.test.*

object PageDependencyGraphSpec extends ZIOSpecDefault {

  private def singlePagePdf(label: String): ZIO[Any, Throwable, Chunk[Byte]] = {
    val trailer = Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0)))
    val parts = Chunk(
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
      Part.Obj(
        IndirectObj.stream(4L, Prim.dict(), _root_.scodec.bits.BitVector(s"BT 72 720 Td ($label) Tj ET\n".getBytes))
      ),
      Part.Meta(trailer)
    )
    PdfEngine.writeBytes(parts)
  }

  def spec: Spec[Any, Throwable] = suite("PageDependencyGraph")(
    test("fromDecoded places catalog and pages in the first-page top-level prefix") {
      for {
        bytes   <- singlePagePdf("graph")
        decoded <- ZStream.fromChunk(bytes).via(PdfStream.decode()).runCollect
        raw     <- ZIO.fromEither(PdfGraft.rawObjectParts(bytes.toArray).left.map(new RuntimeException(_)))
        graph    = PageDependencyGraph.fromDecoded(decoded, raw.objects.map(_.index.number).toSet)
      } yield assertTrue(
        graph.firstPageTopLevel.nonEmpty,
        graph.firstPageTopLevel.head == 1L,
        graph.firstPageTopLevel.contains(2L),
        graph.firstPageTopLevel.contains(3L)
      )
    },
    test("reorder moves the first-page prefix ahead of the tail") {
      for {
        bytes <- singlePagePdf("reorder")
        raw   <- ZIO.fromEither(PdfGraft.rawObjectParts(bytes.toArray).left.map(new RuntimeException(_)))
        decoded <- ZStream.fromChunk(bytes).via(PdfStream.decode()).runCollect
        graph = PageDependencyGraph.fromDecoded(decoded, raw.objects.map(_.index.number).toSet)
        reordered = PageDependencyGraph.reorder(raw.objects, graph.firstPageTopLevel)
      } yield assertTrue(
        reordered.take(graph.firstPageTopLevel.size).map(_.index.number) == Chunk.fromIterable(graph.firstPageTopLevel)
      )
    },
    test("linearize fromBytes keeps output near source size with dependency reordering") {
      for {
        source     <- singlePagePdf("linear")
        linearized <- PdfLinearize.fromBytes(source)
      } yield assertTrue(linearized.size <= source.size * 2, linearized.size >= source.size)
    }
  )
}
