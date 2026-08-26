package zio.pdf.examples

import java.nio.file.Path

import _root_.scodec.bits.BitVector
import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Generate a small PDF through the public streaming writer for external validation. */
object GeneratePdf extends ZIOAppDefault:

  private val catalog = IndirectObj.nostream(
    1,
    Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
  )
  private val pages = IndirectObj.nostream(
    2,
    Prim.dict(
      "Type" -> Prim.Name("Pages"),
      "Kids" -> Prim.Array(Prim.Ref(3, 0)),
      "Count" -> Prim.Number(BigDecimal(1))
    )
  )
  private val page = IndirectObj.nostream(
    3,
    Prim.dict(
      "Type" -> Prim.Name("Page"),
      "Parent" -> Prim.Ref(2, 0),
      "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
      "Resources" -> Prim.Dict.empty,
      "Contents" -> Prim.Ref(4, 0)
    )
  )
  private val content = IndirectObj.stream(
    4,
    Prim.dict(),
    BitVector("BT 72 720 Td (zio-pdf) Tj ET\n".getBytes)
  )
  private val trailer =
    Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

  def run: ZIO[Any, Throwable, Unit] =
    ZIO
      .fromOption(sys.env.get("OUTPUT_PDF"))
      .orElseFail(IllegalArgumentException("OUTPUT_PDF must name the generated file"))
      .flatMap { path =>
        ZStream(catalog, pages, page, content)
          .via(WritePdf.objects(trailer))
          .mapConcatChunk(bytes => Chunk.fromArray(bytes.toArray))
          .run(PdfIO.writer(Path.of(path)))
          .flatMap(written => Console.printLine(s"wrote $written bytes to $path"))
      }
