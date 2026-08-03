package zio.pdf

import zio.*
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

  def spec: Spec[Any, Throwable] = suite("PdfHyperdrive streaming parity")(
    test("streaming timeline matches on test-image.pdf") {
      for {
        bytes    <- load("test-image.pdf")
        hyper     = PdfHyperdrive.decodeStreamingSync(bytes)
        streamed <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(PdfStream.streamingDecode()).runCollect
      } yield assertTrue(hyper == streamed)
    }
  )
}
