package zio.pdf.examples

import java.nio.file.Path

import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Linearize an existing PDF and print byte-range fetch metrics. */
object LinearizeFromFile extends ZIOAppDefault:

  def run: ZIO[Any, Throwable, Unit] =
    for {
      input  <- envPath("INPUT_PDF")
      output <- envPath("OUTPUT_PDF")
      source <- PdfIO.reader(input).runCollect
      linearized <- PdfLinearize.fromBytes(source)
      firstPage <- ZIO.fromEither(PdfLinearize.firstPageByteLength(linearized).left.map(new RuntimeException(_)))
      _ <- ZStream.fromChunk(linearized).run(PdfIO.writer(output))
      ratio = linearized.size.toDouble / source.size.toDouble
      savings = (1.0 - (firstPage.toDouble / linearized.size.toDouble)) * 100.0
      _ <- Console.printLine(
             s"linearized_bytes=${linearized.size}\n" +
               s"source_bytes=${source.size}\n" +
               s"first_page_prefix_bytes=$firstPage\n" +
               s"size_ratio=${f"$ratio%.4f"}\n" +
               s"first_page_savings_pct=${f"$savings%.1f"}\n" +
               s"output=$output"
           )
    } yield ()

  private def envPath(name: String): ZIO[Any, Throwable, Path] =
    ZIO.fromOption(sys.env.get(name).map(Path.of(_))).orElseFail(IllegalArgumentException(s"$name is required"))
