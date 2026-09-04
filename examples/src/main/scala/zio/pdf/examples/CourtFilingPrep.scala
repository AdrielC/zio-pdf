package zio.pdf.examples

import java.nio.file.Path
import java.time.LocalDate

import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/**
 * Apply a court filing prep program: optional form fill + flatten, date stamp,
 * Bates labels, FILED watermark, first-page thumbnail, and linearize.
 *
 * Environment:
 *   INPUT_PDF  — source PDF (unencrypted)
 *   OUTPUT_PDF — destination path
 *   ATTORNEY   — optional AcroForm field value for qualified name "Attorney"
 */
object CourtFilingPrep extends ZIOAppDefault:

  def run: ZIO[Any, Throwable, Unit] =
    for {
      input  <- envPath("INPUT_PDF")
      output <- envPath("OUTPUT_PDF")
      source <- PdfIO.reader(input).runCollect
      attorney = sys.env.get("ATTORNEY")
      program  = buildProgram(attorney)
      _       <- Console.printLine(s"prep profile: ${PdfPrep.profile(program).operations.mkString(" -> ")}")
      filled  <- PdfPrep.apply(source, program, today = LocalDate.parse("2026-09-04")).provide(PdfEngine.live)
      _       <- ZStream.fromChunk(filled).run(PdfIO.writer(output))
      _       <- Console.printLine(s"court filing prep: $input -> $output (${filled.size} bytes)")
    } yield ()

  private def buildProgram(attorney: Option[String]): PdfPrep.Program = {
    val formOps =
      attorney.toList.flatMap { name =>
        List(
          PdfPrep.Op.SetFieldValues(List(PdfPrep.FieldValue("Attorney", name))),
          PdfPrep.Op.FlattenForms
        )
      }
    PdfPrep.Program.of(
      formOps ++
        List(
          PdfPrep.Op.DateStamp(
            PdfPrep.StampDate(
              source = PdfPrep.DateSource.Fixed("2026-09-04"),
              pattern = "yyyy-MM-dd",
              style = PdfPrep.TextStyle(placement = PdfPrep.Placement.TopRight, fontSize = 9)
            )
          ),
          PdfPrep.Op.Bates(PdfPrep.BatesLabel(prefix = "DOC-", start = 1, width = 5)),
          PdfPrep.Op.Watermark(
            PdfPrep.WatermarkText(
              text = "FILED",
              placement = PdfPrep.Placement.TopCenter,
              fontSize = Some(22),
              rotationDegrees = 0
            )
          ),
          PdfPrep.Op.AttachThumbnail(PdfPrep.ThumbnailScope.FirstPageOnly),
          PdfPrep.Op.Linearize
        )*
    )
  }

  private def envPath(name: String): ZIO[Any, Throwable, Path] =
    ZIO.fromOption(sys.env.get(name).map(Path.of(_))).orElseFail(IllegalArgumentException(s"$name is required"))
