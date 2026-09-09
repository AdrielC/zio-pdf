package zio.pdf

import zio.*
import zio.Chunk
import zio.test.*

object NativePdfEngineSpec extends ZIOSpecDefault:

  def spec: Spec[Any, Throwable] = suite("Scala Native platform")(
    test("PdfSource streams an in-memory chunk") {
      PdfSource
        .fromChunk(Chunk[Byte](1, 2, 3))
        .bytes
        .runCollect
        .map(result => assertTrue(result == Chunk[Byte](1, 2, 3)))
    },
    test("PdfMime exposes the PDF media type without zio-blocks-mediatype") {
      ZIO.succeed(assertTrue(PdfMime.contentTypeHeader == "application/pdf"))
    },
    test("NativeFileBackend exposes posix and io_uring handles") {
      import zio.pdf.io.NativeFileBackend
      ZIO.succeed(assertTrue(NativeFileBackend.ioUring ne null, NativeFileBackend.posix ne null))
    }
  )
