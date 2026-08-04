package zio.pdf

import zio.*
import zio.stream.ZStream
import zio.test.*

object PdfHyperdriveSpec extends ZIOSpecDefault {

  private def loadFixture(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing from test resources")
      val buf = is.readAllBytes()
      is.close()
      buf
    }

  private def withTempPdf[R](name: String)(use: java.nio.file.Path => ZIO[R, Throwable, TestResult]) =
    for {
      bytes  <- loadFixture(name)
      path   <- ZIO.attemptBlocking {
                  val p = java.nio.file.Files.createTempFile("hyperdrive-", ".pdf")
                  java.nio.file.Files.write(p, bytes)
                  p
                }
      result <- use(path).ensuring(ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path)).ignore)
    } yield result

  def spec: Spec[Any, Throwable] = suite("PdfHyperdrive")(
    test("decodeSync matches PdfStream.decode on xref-stream.pdf") {
      for {
        bytes    <- loadFixture("xref-stream.pdf")
        hyper     = PdfHyperdrive.decodeSync(bytes)
        streamed <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.decode()).runCollect
      } yield assertTrue(hyper == streamed)
    },
    test("decodeSync streaming timeline matches on test-image.pdf") {
      for {
        bytes    <- loadFixture("test-image.pdf")
        hyper     = PdfHyperdrive.decodeStreamingSync(bytes)
        streamed <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.streamingDecode()).runCollect
      } yield assertTrue(hyper == streamed)
    },
    test("PdfEngine.decode path matches in-memory decodeSync") {
      withTempPdf("xref-stream.pdf") { path =>
        for {
          bytes   <- loadFixture("xref-stream.pdf")
          engine  <- PdfEngine.decode(path).provide(PdfEngine.live)
          direct   = PdfHyperdrive.decodeSync(bytes)
        } yield assertTrue(engine == direct)
      }
    },
    test("PdfEngine.elements matches stream decode + Elements.pipe") {
      withTempPdf("xref-stream.pdf") { path =>
        for {
          bytes    <- loadFixture("xref-stream.pdf")
          engine   <- PdfEngine.elements(path).runCollect.provide(PdfEngine.live)
          streamed <- ZStream
                        .fromChunk(Chunk.fromArray(bytes))
                        .via(PdfStream.decode())
                        .via(Elements.pipe)
                        .runCollect
        } yield assertTrue(engine == streamed)
      }
    },
    test("elementsSync matches stream decode + Elements.pipe") {
      for {
        bytes    <- loadFixture("xref-stream.pdf")
        elements <- ZIO.succeed(PdfHyperdrive.elementsSync(bytes))
        streamed <- ZStream
                      .fromChunk(Chunk.fromArray(bytes))
                      .via(PdfStream.decode())
                      .via(Elements.pipe)
                      .runCollect
      } yield assertTrue(elements == streamed)
    }
  )
}
