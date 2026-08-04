/*
 * Minimal end-to-end: read a PDF, decode via PdfEngine (fused + pipeline).
 *
 * Run from the repo root:
 *
 *   sbt examples/run
 *   PDF_PATH=/path/to/file.pdf sbt examples/run
 *
 * Default fixture: `xref-stream.pdf` (decode/parse smoke test — not a
 * fully valid catalog tree; see RewriteValidateCompareSpec for validate).
 */

package zio.pdf.examples

import java.nio.file.{Files, Path, StandardCopyOption}

import zio.*
import zio.pdf.{Decoded, PdfEngine, PdfStream}
import zio.pdf.io.PdfIO

object ReadAndDecode extends ZIOAppDefault {

  def run: ZIO[Any, Throwable, ExitCode] =
    for {
      path       <- resolvePath
      _          <- Console.printLine(s"Decoding: $path")
      engineOut  <- PdfEngine.decode(path).provide(PdfEngine.live)
      elements   <- PdfEngine.elements(path).provide(PdfEngine.live)
      zioOut     <- PdfIO.reader(path).via(PdfStream.decode()).runCollect
      objs        = countObjs(engineOut)
      metas       = engineOut.collect { case m: Decoded.Meta => m }.size
      _          <- Console.printLine(
                      s"decode:   ${engineOut.size} events ($objs objects, $metas meta) [fused]"
                    )
      _          <- Console.printLine(
                      s"elements: ${elements.size} classified [fused — PdfEngine.elements]"
                    )
      _          <- Console.printLine(
                      s"pipeline: ${zioOut.size} events (${countObjs(zioOut)} objects) [PdfStream.decode]"
                    )
      _          <- ZIO.when(engineOut == zioOut)(
                      Console.printLine("OK — fused decode matches pipeline decode")
                    )
      _          <- ZIO.unless(engineOut == zioOut)(
                      Console.printLineError("FAIL — fused decode and pipeline differ")
                    )
      _          <- ZIO.when(elements.nonEmpty)(
                      Console.printLine(s"sample element: ${elements.head.getClass.getSimpleName}")
                    )
    } yield ExitCode.success

  private def countObjs(chunk: Chunk[Decoded]): Int =
    chunk.count {
      case Decoded.DataObj(_) | Decoded.ContentObj(_, _, _) => true
      case _                                                => false
    }

  private def resolvePath: ZIO[Any, Throwable, Path] =
    ZIO.attempt {
      sys.env
        .get("PDF_PATH")
        .map(Path.of(_))
        .getOrElse(extractClasspathFixture)
    }

  private def extractClasspathFixture: Path = {
    val is = getClass.getResourceAsStream("/xref-stream.pdf")
    require(is != null, "xref-stream.pdf not on classpath")
    val tmp = Files.createTempFile("zio-pdf-example-", ".pdf")
    Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING)
    is.close()
    tmp
  }
}
