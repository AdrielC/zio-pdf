/*
 * ZIO-only PDF file I/O — reader / writer / readAll / writeAll.
 *
 * File handles are acquired and released by `ZStream` / `ZSink` scopes
 * (`fromInputStreamZIO`, `fromOutputStreamScoped`). For decode / validate /
 * digest, use [[zio.pdf.PdfEngine]].
 *
 * {{{
 *   PdfEngine.decode(PdfIO.reader(path)).runCollect.provide(PdfEngine.live)
 *   PdfEngine.decode(path).provide(PdfEngine.live)
 * }}}
 */

package zio.pdf.io

import java.nio.file.{Files, Path, StandardOpenOption}

import zio.{Chunk, ZIO}
import zio.pdf.ByteLimit
import zio.stream.{ZSink, ZStream}

object PdfIO {

  final case class ReadLimitExceeded(path: Path, maxBytes: ByteLimit, observedBytes: Long)
      extends RuntimeException(
        s"$path contains at least $observedBytes bytes, above the configured ${maxBytes.bytes}-byte readAll limit"
      )

  /** Byte stream from `path`; released when the stream scope ends. */
  def reader(path: Path, chunkSize: Int = 64 * 1024): ZStream[Any, Throwable, Byte] =
    ZStream.fromInputStreamZIO(
      ZIO
        .attemptBlocking(Files.newInputStream(path))
        .refineToOrDie[java.io.IOException],
      chunkSize
    )

  /** Byte sink to `path`; released when the sink scope ends. */
  def writer(path: Path, options: StandardOpenOption*): ZSink[Any, Throwable, Byte, Byte, Long] = {
    val opts: Array[StandardOpenOption] =
      if (options.isEmpty)
        Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
      else
        options.toArray
    ZSink.fromOutputStreamScoped(
      ZIO.fromAutoCloseable(
        ZIO
          .attemptBlocking(Files.newOutputStream(path, opts*))
          .refineToOrDie[java.io.IOException]
      )
    )
  }

  /**
   * Materialize a file with an explicit typed bound. The stream reads at most
   * one byte past the bound so the failure cannot allocate in proportion to an
   * arbitrary input.
   */
  def readAtMost(
    path: Path,
    maxBytes: ByteLimit,
    chunkSize: Int = 64 * 1024
  ): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.attemptBlocking(Files.size(path)).flatMap { size =>
      if size > maxBytes.toLong then ZIO.fail(ReadLimitExceeded(path, maxBytes, size))
      else
        reader(path, chunkSize).take(maxBytes.toLong + 1L).runCollect.flatMap { bytes =>
          if bytes.size.toLong > maxBytes.toLong then
            ZIO.fail(ReadLimitExceeded(path, maxBytes, bytes.size.toLong))
          else ZIO.succeed(bytes)
        }
    }

  /** Materialize at most 64 MiB. Use [[reader]] for unbounded inputs. */
  def readAll(path: Path, chunkSize: Int = 64 * 1024): ZIO[Any, Throwable, Chunk[Byte]] =
    readAtMost(path, ByteLimit.DefaultReadAll, chunkSize)

  def writeAll(path: Path, bytes: Chunk[Byte], options: StandardOpenOption*): ZIO[Any, Throwable, Long] =
    ZStream.fromChunk(bytes).run(writer(path, options*))
}
