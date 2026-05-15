package zio.pdf.blocks

import java.nio.file.Files

import zio.*
import zio.blocks.streams.{Sink, Stream}
import zio.pdf.page.FirstPage
import zio.pdf.testkit.GeneratedPdf
import zio.test.*

object PdfBlockStreamsSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("PdfBlockStreams")(
    test("runs zio-blocks-streams from a scoped path as a ZIO effect") {
      for {
        bytes <- GeneratedPdf.legalFiling
        path  <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-blocks-", ".pdf"))
        _     <- ZIO.attemptBlocking(Files.write(path, bytes.toArray))
        count <- BlocksRuntime.runZIO(PdfBlockStreams.fromPath(path), Sink.count)
        _     <- ZIO.attemptBlocking(Files.deleteIfExists(path))
      } yield assertTrue(count == bytes.size.toLong)
    },
    test("firstPageText(path) agrees with the ZIO pipeline") {
      for {
        bytes  <- GeneratedPdf.legalFiling
        path   <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-blocks-text-", ".pdf"))
        _      <- ZIO.attemptBlocking(Files.write(path, bytes.toArray))
        blocks <- PdfBlockStreams.firstPageText(path)
        zio    <- FirstPage.fromBytes(bytes)
        _      <- ZIO.attemptBlocking(Files.deleteIfExists(path))
      } yield assertTrue(
        blocks.text == zio.text,
        blocks.text.contains("ACME LENDING LLC"),
        !blocks.text.contains(GeneratedPdf.secondPageSentinel)
      )
    },
    test("text-line pipelines are reusable as pure zio-blocks pipelines") {
      val result =
        Stream("  ACME   LENDING LLC  ", " noise ", "v.", " JANE DOE Defendant ")
          .via(PdfBlockStreams.captionCandidateLines)
          .runCollect
      assertTrue(result.exists(_.toList == List("v.", "JANE DOE Defendant")))
    }
  ) @@ TestAspect.timeout(30.seconds)
}
