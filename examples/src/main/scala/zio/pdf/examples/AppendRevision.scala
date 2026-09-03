package zio.pdf.examples

import java.nio.file.Path

import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Append an incremental revision (sign/append) to an existing PDF. */
object AppendRevision extends ZIOAppDefault:

  def run: ZIO[Any, Throwable, Unit] =
    for {
      input  <- envPath("INPUT_PDF")
      output <- envPath("OUTPUT_PDF")
      base   <- PdfIO.reader(input).runCollect
      revision = Chunk(
        Part.Obj(
          IndirectObj.nostream(
            100L,
            Prim.dict("Producer" -> Prim.Name("zio-pdf-append"), "Title" -> Prim.Name("Signed copy"))
          )
        ),
        Part.Meta(Trailer(BigDecimal(101), Prim.dict("Info" -> Prim.Ref(100L, 0)), None))
      )
      updated <- PdfAppend.append(base, revision)
      _       <- ZStream.fromChunk(updated).run(PdfIO.writer(output))
      _       <- Console.printLine(s"appended revision to $input -> $output (${updated.size} bytes)")
    } yield ()

  private def envPath(name: String): ZIO[Any, Throwable, Path] =
    ZIO.fromOption(sys.env.get(name).map(Path.of(_))).orElseFail(IllegalArgumentException(s"$name is required"))
