package zio.pdf

import zio.*
import zio.pdf.io.PdfIO
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

  def spec: Spec[Any, Throwable] = suite("PdfHyperdrive")(
    test("decodeSync matches PdfStream.decode on xref-stream.pdf") {
      for {
        bytes   <- loadFixture("xref-stream.pdf")
        hyper    = PdfHyperdrive.decodeSync(bytes)
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
    test("warp matches decodeDecoded for in-memory fixture") {
      for {
        bytes <- loadFixture("xref-stream.pdf")
        path  <- ZIO.attemptBlocking {
          val p = java.nio.file.Files.createTempFile("hyperdrive-", ".pdf")
          java.nio.file.Files.write(p, bytes)
          p
        }
        warp    <- PdfIO.warp(path)
        decoded <- PdfIO.decodeDecoded(path)
      } yield assertTrue(warp == decoded)
    },
    test("warpMapped matches warp") {
      for {
        bytes <- loadFixture("xref-stream.pdf")
        path  <- ZIO.attemptBlocking {
          val p = java.nio.file.Files.createTempFile("hyperdrive-mmap-", ".pdf")
          java.nio.file.Files.write(p, bytes)
          p
        }
        warp      <- PdfIO.warp(path)
        warpMapped <- PdfIO.warpMapped(path)
      } yield assertTrue(warpMapped == warp)
    },
    test("sicko is warpMapped and decodeDecoded auto-routes sicko") {
      for {
        bytes <- loadFixture("xref-stream.pdf")
        path  <- ZIO.attemptBlocking {
          val p = java.nio.file.Files.createTempFile("sicko-", ".pdf")
          java.nio.file.Files.write(p, bytes)
          p
        }
        sicko   <- PdfIO.sicko(path)
        mapped  <- PdfIO.warpMapped(path)
        decoded <- PdfIO.decodeDecoded(path)
      } yield assertTrue(sicko == mapped, sicko == decoded)
    },
    test("sickoElements matches stream decode + Elements.pipe") {
      for {
        bytes    <- loadFixture("xref-stream.pdf")
        path     <- ZIO.attemptBlocking {
          val p = java.nio.file.Files.createTempFile("sicko-el-", ".pdf")
          java.nio.file.Files.write(p, bytes)
          p
        }
        sicko    <- PdfIO.sickoElements(path)
        streamed <- ZStream
          .fromChunk(Chunk.fromArray(bytes))
          .via(PdfStream.decode())
          .via(Elements.pipe)
          .runCollect
      } yield assertTrue(sicko == streamed)
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
