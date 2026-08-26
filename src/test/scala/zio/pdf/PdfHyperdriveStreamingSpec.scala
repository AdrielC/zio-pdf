package zio.pdf

import java.nio.file.Files

import zio.*
import zio.stream.ZStream
import zio.test.*

object PdfHyperdriveStreamingSpec extends ZIOSpecDefault {

  private def sameEvent(left: Decoded, right: Decoded): Boolean =
    (left, right) match {
      case (a: Decoded.ContentObj, b: Decoded.ContentObj) =>
        a.obj == b.obj && a.rawStream == b.rawStream
      case (a, b) =>
        a == b
    }

  private def sameTimeline(left: Chunk[Decoded], right: Chunk[Decoded]): Boolean =
    left.size == right.size && left.zip(right).forall(sameEvent)

  private def load(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing")
      val b = is.readAllBytes()
      is.close()
      b
    }

  private def withTempPdf[R](name: String)(use: java.nio.file.Path => ZIO[R, Throwable, TestResult]) =
    for {
      bytes  <- load(name)
      path   <- ZIO.attemptBlocking(Files.createTempFile("zio-pdf-hyper-", ".pdf"))
      _      <- ZIO.attemptBlocking(Files.write(path, bytes))
      result <- use(path).ensuring(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
    } yield result

  def spec: Spec[Any, Throwable] = suite("PdfHyperdrive streaming parity")(
    test("uses a file-size-aware path buffer without weakening the parser floor") {
      assertTrue(
        HyperdriveStream.adaptiveChunkSize(1L, FusedDecoder.DefaultChunkSize) == FusedDecoder.MinimumChunkSize,
        HyperdriveStream.adaptiveChunkSize(5L * 1024L * 1024L, FusedDecoder.DefaultChunkSize) == 5 * 1024 * 1024,
        HyperdriveStream.adaptiveChunkSize(20L * 1024L * 1024L, FusedDecoder.DefaultChunkSize) == FusedDecoder.DefaultChunkSize,
        HyperdriveStream.adaptiveChunkSize(20L * 1024L * 1024L, 128 * 1024) == 128 * 1024
      )
    },
    test("streaming timeline matches on test-image.pdf") {
      for {
        bytes    <- load("test-image.pdf")
        hyper     = PdfHyperdrive.decodeStreamingSync(bytes)
        streamed <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.streamingDecode()).runCollect
      } yield assertTrue(hyper == streamed)
    },
    test("PdfEngine.stream matches sink count without pre-collect") {
      withTempPdf("test-image.pdf") { path =>
        for {
          streamed <- PdfEngine.stream(path).runCount
          sunk     <- PdfEngine.sink(path)(_ => ()).provide(PdfEngine.live)
        } yield assertTrue(streamed == sunk)
      }.provide(PdfEngine.live)
    },
    test("PdfEngine.stream take(1) matches decode head") {
      withTempPdf("test-image.pdf") { path =>
        for {
          first <- PdfEngine.stream(path).take(1).runCollect
          all   <- PdfEngine.decode(path)
          headOk = sameEvent(first.head, all.head)
        } yield assertTrue(first.size == 1, all.nonEmpty, headOk)
      }.provide(PdfEngine.live)
    },
    test("chunked session preserves the fused timeline at a one-byte source chunk") {
      withTempPdf("test-image.pdf") { path =>
        for {
          bytes    <- load("test-image.pdf")
          streamed <- ZStream
                        .fromChunk(Chunk.fromArray(bytes))
                        .rechunk(1)
                        .via(FusedDecoder.decodePipeline())
                        .runCollect
          direct   <- PdfEngine.decode(path)
        } yield assertTrue(sameTimeline(streamed, direct))
      }.provide(PdfEngine.live)
    },
    test("path stream normalizes a tiny requested decode window") {
      withTempPdf("test-image.pdf") { path =>
        for {
          first  <- PdfEngine.stream(path, PdfEngine.Options(batchSize = 1)).take(1).runCollect
          direct <- PdfEngine.decode(path)
        } yield assertTrue(first.size == 1, sameEvent(first.head, direct.head))
      }.provide(PdfEngine.live)
    }
  )
}
