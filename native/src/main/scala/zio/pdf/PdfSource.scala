package zio.pdf

import zio.Chunk
import zio.pdf.io.NativeFileBackend
import zio.stream.ZStream

/** A byte source that the platform-specific `PdfEngine` can consume. */
trait PdfSource:
  def bytes: ZStream[Any, Throwable, Byte]

object PdfSource:

  def fromChunk(input: Chunk[Byte]): PdfSource =
    new PdfSource:
      val bytes: ZStream[Any, Throwable, Byte] = ZStream.fromChunk(input)

  /** Read a filesystem path with a POSIX or io_uring backend (see [[NativeFileBackend]]). */
  def fromPath(
    path: String,
    chunkSize: Int = 64 * 1024,
    backend: NativeFileBackend = NativeFileBackend.posix
  ): PdfSource =
    new PdfSource:
      def bytes: ZStream[Any, Throwable, Byte] =
        backend.readStream(path, chunkSize)
