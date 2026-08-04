package zio.pdf

import java.nio.file.Files

import zio.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream
import zio.test.*

object PdfHyperdriveStreamingSpec extends ZIOSpecDefault {

  private def load(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing")
      val b = is.readAllBytes()
      is.close()
      b
    }

  private def withTempPdf(name: String)(use: java.nio.file.Path => ZIO[Any, Throwable, TestResult]) =
    for {
      bytes  <- load(name)
      path   <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-hyper-", ".pdf"))
      _      <- ZIO.attemptBlocking(Files.write(path, bytes))
      result <- use(path).ensuring(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
    } yield result

  def spec: Spec[Any, Throwable] = suite("PdfHyperdrive streaming parity")(
    test("streaming timeline matches on test-image.pdf") {
      for {
        bytes    <- load("test-image.pdf")
        hyper     = PdfHyperdrive.decodeStreamingSync(bytes)
        streamed <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.streamingDecode()).runCollect
      } yield assertTrue(hyper == streamed)
    },
    test("HyperdriveStream.decoded matches sink count without pre-collect") {
      withTempPdf("test-image.pdf") { path =>
        for {
          streamed <- HyperdriveStream.decoded(path, queueCapacity = 2).runCount
          sunk     <- PdfIO.warpStreaming(path)(_ => ZIO.unit)
        } yield assertTrue(streamed == sunk)
      }
    },
    test("HyperdriveStream backpressures with queueCapacity=1") {
      withTempPdf("test-image.pdf") { path =>
        for {
          first <- HyperdriveStream.decoded(path, queueCapacity = 1).take(1).runCollect
          all   <- PdfIO.warp(path)
          headOk = (first.head, all.head) match {
                     case (a: Decoded.ContentObj, b: Decoded.ContentObj) =>
                       a.obj == b.obj && a.rawStream == b.rawStream
                     case (a, b) => a == b
                   }
        } yield assertTrue(first.size == 1, all.nonEmpty, headOk)
      }
    }
  )
}
