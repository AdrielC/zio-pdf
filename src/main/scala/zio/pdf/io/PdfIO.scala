/*
 * ZIO-only PDF file I/O.
 *
 * File handles are acquired and released by `ZStream` / `ZSink` scopes
 * (`fromInputStreamZIO`, `fromOutputStreamScoped`). Compose with
 * [[zio.pdf.PdfStream]] pipelines at the call site:
 *
 * {{{
 *   PdfIO.reader(path).via(PdfStream.decode()).runCollect
 * }}}
 *
 * High-level helpers ([[decodeDecoded]], [[validate]], [[comparePaths]])
 * are thin wrappers over the same shape.
 */

package zio.pdf.io

import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import zio.{Chunk, ZIO}
import zio.pdf.*
import zio.prelude.Validation
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

  /**
   * Strict top-level decode when the PDF fits in memory — no streaming
   * interpreter, beats fs2-style Pull on small/medium files.
   */
  def decodeTopLevelStrict(path: Path): ZIO[Any, Throwable, Chunk[TopLevel]] =
    readAll(path).flatMap { bytes =>
      ZIO.fromEither(TopLevel.decodeAll(bytes.toArray).left.map(e => new RuntimeException(e.toString)))
    }

  def writeAll(path: Path, bytes: Chunk[Byte], options: StandardOpenOption*): ZIO[Any, Throwable, Long] =
    ZStream.fromChunk(bytes).run(writer(path, options*))

  def decodeStreamingDecoded(
    path: Path,
    chunkSize: Int = 64 * 1024,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[StreamingDecoded]] =
    reader(path, chunkSize).via(PdfStream.streamingDecode(enableDiagnostics, config)).runCollect

  def decodeDecoded(
    path: Path,
    chunkSize: Int = 64 * 1024,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    hyperdriveThreshold: Long = PdfHyperdrive.defaultAutoThresholdBytes
  ): ZIO[Any, Throwable, Chunk[Decoded]] =
    attemptHyperdrive(path, hyperdriveThreshold, enableDiagnostics, config).flatMap {
      case Some(decoded) => ZIO.succeed(decoded)
      case None          => reader(path, chunkSize).via(PdfStream.decode(enableDiagnostics, config)).runCollect
    }

  /**
   * [[PdfHyperdrive.decodeSync]] for a file — zero `ZStream` on the hot path.
   * Alias: the name we use when we mean business.
   */
  def warp(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Decoded]] =
    readAll(path).map { bytes =>
      PdfHyperdrive.decodeSync(bytes.toArray, enableDiagnostics = enableDiagnostics, config = config)
    }

  /**
   * Memory-mapped [[PdfHyperdrive.decodeSyncMapped]] — avoids a heap
   * copy when the OS mapping is array-backed.
   */
  def warpMapped(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Decoded]] =
    ZIO.acquireReleaseWith(
      ZIO.attemptBlocking(FileChannel.open(path, StandardOpenOption.READ))
    )(ch => ZIO.attemptBlocking(ch.close()).orDie) { channel =>
      ZIO.attemptBlocking {
        val size = channel.size()
        require(size <= Int.MaxValue, s"file too large for mmap warp: $size bytes")
        val mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0L, size)
        PdfHyperdrive.decodeSyncMapped(mapped, enableDiagnostics = enableDiagnostics, config = config)
      }
    }

  /** [[PdfHyperdrive.elementsSync]] for a file on disk. */
  def warpElements(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Element]] =
    readAll(path).map { bytes =>
      PdfHyperdrive.elementsSync(bytes.toArray, enableDiagnostics = enableDiagnostics, config = config)
    }

  private def attemptHyperdrive(
    path: Path,
    threshold: Long,
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config
  ): ZIO[Any, Throwable, Option[Chunk[Decoded]]] =
    ZIO.attemptBlocking(Files.size(path)).flatMap { size =>
      if (PdfHyperdrive.fitsInHyperdrive(size, threshold))
        readAll(path).map { bytes =>
          Some(PdfHyperdrive.decodeSync(bytes.toArray, enableDiagnostics = enableDiagnostics, config = config))
        }
      else
        ZIO.none
    }

  def validate(
    path: Path,
    chunkSize: Int = 64 * 1024,
    enableDiagnostics: Boolean = false
  ): ZIO[Any, Throwable, Validation[PdfError, Unit]] =
    decodeDecoded(path, chunkSize, enableDiagnostics).map(ValidatePdf.fromChunk)

  def comparePaths(
    oldPath: Path,
    newPath: Path,
    chunkSize: Int = 64 * 1024,
    enableDiagnostics: Boolean = false
  ): ZIO[Any, Throwable, Validation[CompareError, Unit]] =
    for {
      oldDecoded <- decodeDecoded(oldPath, chunkSize, enableDiagnostics)
      newDecoded <- decodeDecoded(newPath, chunkSize, enableDiagnostics)
    } yield ComparePdfs.fromChunks(oldDecoded, newDecoded)
}
