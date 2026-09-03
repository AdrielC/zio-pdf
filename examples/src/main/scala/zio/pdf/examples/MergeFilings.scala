package zio.pdf.examples

import java.nio.file.Path

import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Merge two PDFs into one filing (paths from MERGE_LEFT and MERGE_RIGHT). */
object MergeFilings extends ZIOAppDefault:

  def run: ZIO[Any, Throwable, Unit] =
    for {
      left  <- envPath("MERGE_LEFT")
      right <- envPath("MERGE_RIGHT")
      out   <- envPath("OUTPUT_PDF")
      merged <- PdfEngine.merge(NonEmptyChunk(left, right)).provide(PdfEngine.live)
      _      <- ZStream.fromChunk(merged).run(PdfIO.writer(out))
      _      <- Console.printLine(s"merged $left + $right -> $out (${merged.size} bytes)")
    } yield ()

  private def envPath(name: String): ZIO[Any, Throwable, Path] =
    ZIO.fromOption(sys.env.get(name).map(Path.of(_))).orElseFail(IllegalArgumentException(s"$name is required"))
