/*
 * ZIO-only PDF file I/O — reader / writer / readAll / writeAll.
 *
 * File handles are acquired and released by `ZStream` / `ZSink` scopes
 * (`fromInputStreamZIO`, `fromOutputStreamScoped`). For decode / validate /
 * digest, use [[zio.pdf.PdfEngine]].
 *
 * {{{
 *   PdfIO.reader(path).via(PdfStream.decode()).runCollect  // byte-pipeline
 *   PdfEngine.decode(path).provide(PdfEngine.live)         // path decode
 * }}}
 */

package zio.pdf.io

import java.nio.file.{Files, Path, StandardOpenOption}

import zio.{Chunk, ZIO}
import zio.stream.{ZSink, ZStream}

object PdfIO {

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

  def readAll(path: Path, chunkSize: Int = 64 * 1024): ZIO[Any, Throwable, Chunk[Byte]] =
    reader(path, chunkSize).runCollect

  def writeAll(path: Path, bytes: Chunk[Byte], options: StandardOpenOption*): ZIO[Any, Throwable, Long] =
    ZStream.fromChunk(bytes).run(writer(path, options*))
}
