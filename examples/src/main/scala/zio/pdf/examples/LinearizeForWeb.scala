package zio.pdf.examples

import java.nio.file.Path

import _root_.scodec.bits.BitVector
import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Write a linearized PDF for fast first-page web display. */
object LinearizeForWeb extends ZIOAppDefault:

  private val trailerData = Prim.dict("Root" -> Prim.Ref(1L, 0))
  private val trailer =
    Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0)))

  private val parts = Chunk(
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
    Part.Obj(IndirectObj.stream(4L, Prim.dict(), BitVector("BT 72 720 Td (linearized) Tj ET\n".getBytes))),
    Part.Meta(trailer)
  )

  def run: ZIO[Any, Throwable, Unit] =
    for {
      out <- ZIO.fromOption(sys.env.get("OUTPUT_PDF").map(Path.of(_))).orElseFail(IllegalArgumentException("OUTPUT_PDF is required"))
      bytes <- PdfLinearize.bytes(trailerData, parts)
      _     <- ZStream.fromChunk(bytes).run(PdfIO.writer(out))
      _     <- Console.printLine(s"wrote linearized PDF to $out (${bytes.size} bytes)")
    } yield ()
