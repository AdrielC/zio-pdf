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
import zio.pdf.{Decoded, PdfEngine, PdfInspection, PdfStream}
import zio.pdf.io.PdfIO

object ReadAndDecode extends ZIOAppDefault {

  def run: ZIO[Any, Throwable, ExitCode] =
    for {
      path       <- resolvePath
      _          <- Console.printLine(s"Decoding: $path")
      engineOut  <- PdfEngine.decode(path).provide(PdfEngine.live)
      elements   <- PdfEngine.elements(path).provide(PdfEngine.live)
      zioOut     <- PdfIO.reader(path).via(PdfStream.decode()).runCollect
      pageText   <- PdfEngine.extractText(path).runCollect.provide(PdfEngine.live)
      inspection <- PdfEngine
                      .inspect(
                        path,
                        PdfInspection.documentProfile
                      )
                      .provide(PdfEngine.live)
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
      _          <- ZIO.when(sameTimeline(engineOut, zioOut))(
                      Console.printLine("OK — fused decode matches pipeline decode")
                    )
      _          <- ZIO.unless(sameTimeline(engineOut, zioOut))(
                      Console.printLineError("FAIL — fused decode and pipeline differ")
                    )
      _          <- ZIO.when(elements.nonEmpty)(
                      Console.printLine(s"sample element: ${elements.head.getClass.getSimpleName}")
                    )
      textPages   = pageText.count(_.text.nonEmpty)
      characters  = pageText.foldLeft(0L)((total, page) => total + page.text.length.toLong)
      _          <- Console.printLine(s"text:     $textPages/${pageText.size} pages, $characters characters [PdfEngine.extractText]")
      _          <- ZIO.when(pageText.exists(_.text.nonEmpty))(
                      Console.printLine(s"sample text: ${pageText.find(_.text.nonEmpty).map(_.text.take(160)).getOrElse("")}")
                    )
      _          <- Console.printLine(s"inspect:  $inspection")
    } yield ExitCode.success

  private def countObjs(chunk: Chunk[Decoded]): Int =
    chunk.count {
      case Decoded.DataObj(_) | Decoded.ContentObj(_, _, _) => true
      case _                                                => false
    }

  /** Uncompressed payload handles are intentionally opaque; compare PDF bytes. */
  private def sameTimeline(left: Chunk[Decoded], right: Chunk[Decoded]): Boolean =
    left.size == right.size && left.zip(right).forall {
      case (a: Decoded.ContentObj, b: Decoded.ContentObj) =>
        a.obj == b.obj && a.rawStream == b.rawStream
      case (a, b) => a == b
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
