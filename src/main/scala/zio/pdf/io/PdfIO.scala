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
      case None          => HyperdriveStream.decoded(path, enableDiagnostics, config).runCollect
    }

  /**
   * Hyperdrive [[Decoded]] stream — mmap fused parse, bounded backpressure.
   * Alias for [[HyperdriveStream.decoded]].
   */
  def decodeStream(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Decoded] =
    HyperdriveStream.decoded(path, enableDiagnostics, config, batchSize, queueCapacity)

  def elementsStream(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Element] =
    HyperdriveStream.elements(path, enableDiagnostics, config, batchSize, queueCapacity)

  /**
   * [[PdfHyperdrive.decodeFromPath]] — mmap fused decode, no heap copy.
   */
  def warp(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Decoded]] =
    ZIO.attemptBlocking(PdfHyperdrive.decodeFromPath(path, enableDiagnostics, config))

  /** Alias for [[warp]] — mmap is the default file path. */
  def warpMapped(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Decoded]] =
    warp(path, enableDiagnostics, config)

  /** mmap/io_uring triple-fused [[Elements]] classification. */
  def warpElements(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Element]] =
    ZIO.attemptBlocking(PdfHyperdrive.elementsFromPath(path, enableDiagnostics, config))

  /**
   * mmap fused decode with a per-event sink — never materialises `Chunk[Decoded]`.
   * Use on memory-constrained servers for large PDFs; process each [[Decoded]]
   * and drop it before the next arrives.
   *
   * {{{
   *   PdfIO.warpStreaming(path)(decoded => ZIO.succeed(handle(decoded)))
   * }}}
   */
  def warpStreaming[R](
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Long] =
    ZIO.runtime[R].flatMap { runtime =>
      ZIO.attemptBlockingInterrupt {
        import zio.Unsafe
        var count = 0L
        PdfHyperdrive.decodeFromPathSink(path, enableDiagnostics, config, batchSize) { decoded =>
          count += 1
          Unsafe.unsafe { implicit u =>
            runtime.unsafe.run(sink(decoded)).getOrThrow()
          }
        }
        count
      }
    }

  /** Triple-fused elements with a per-event sink — never materialises timelines. */
  def warpElementsStreaming[R](
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Long] =
    ZIO.runtime[R].flatMap { runtime =>
      ZIO.attemptBlockingInterrupt {
        import zio.Unsafe
        var count = 0L
        PdfHyperdrive.elementsFromPathSink(path, enableDiagnostics, config, batchSize) { element =>
          count += 1
          Unsafe.unsafe { implicit u =>
            runtime.unsafe.run(sink(element)).getOrThrow()
          }
        }
        count
      }
    }

  /** Full sicko: mmap + fused decode — fastest file path. */
  def sicko(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Decoded]] =
    warp(path, enableDiagnostics, config)

  /** mmap sicko + triple-fused [[Elements]] classification. */
  def sickoElements(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, Chunk[Element]] =
    warpElements(path, enableDiagnostics, config)

  /** Fused decode + SHA-256 in one scan (mmap auto-route). */
  def decodeAndDigest(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, (Chunk[Decoded], Array[Byte])] =
    ZIO.attemptBlocking(PdfHyperdrive.decodeAndDigestFromPath(path, enableDiagnostics, config))

  /**
   * mmap decode + SHA-256 with a per-event sink — never materialises `Chunk[Decoded]`.
   * Returns `(eventCount, rawFileDigest)`.
   */
  def decodeAndDigestStreaming[R](
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, (Long, Array[Byte])] =
    ZIO.runtime[R].flatMap { runtime =>
      ZIO.attemptBlockingInterrupt {
        import zio.Unsafe
        PdfHyperdrive.decodeAndDigestFromPathSink(path, enableDiagnostics, config, batchSize) { decoded =>
          Unsafe.unsafe { implicit u =>
            runtime.unsafe.run(sink(decoded)).getOrThrow()
          }
        }
      }
    }

  /** Triple-fused elements + SHA-256 in one scan. */
  def elementsAndDigest(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZIO[Any, Throwable, (Chunk[Element], Array[Byte])] =
    ZIO.attemptBlocking(PdfHyperdrive.elementsAndDigestFromPath(path, enableDiagnostics, config))

  /** Triple-fused elements + SHA-256 with a per-event sink. */
  def elementsAndDigestStreaming[R](
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, (Long, Array[Byte])] =
    ZIO.runtime[R].flatMap { runtime =>
      ZIO.attemptBlockingInterrupt {
        import zio.Unsafe
        PdfHyperdrive.elementsAndDigestFromPathSink(path, enableDiagnostics, config, batchSize) { element =>
          Unsafe.unsafe { implicit u =>
            runtime.unsafe.run(sink(element)).getOrThrow()
          }
        }
      }
    }

  /**
   * Hyperdrive mmap decode as a [[ZStream]] — fused parse, per-event emission.
   * Bounded queue provides backpressure when downstream is slower than decode.
   */
  def warpStream(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Decoded] =
    decodeStream(path, enableDiagnostics, config, batchSize, queueCapacity)

  /** Triple-fused [[Element]] stream from mmap hyperdrive. */
  def warpElementsStream(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Element] =
    elementsStream(path, enableDiagnostics, config, batchSize, queueCapacity)

  /** SHA-256 over raw file bytes — mmap batched scan, no decode. */
  def digest(
    path: Path,
    batchSize: Int = 10 * 1024 * 1024
  ): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking(PdfHyperdrive.digestFromPath(path, batchSize))

  /**
   * Validate via hyperdrive stream — no timeline [[Chunk]]; [[AssemblePdf]]
   * still builds the assembled [[Pdf]] model (named assemble, not a silent collect).
   */
  def validateHyperdrive(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZIO[Any, Throwable, Validation[PdfError, Unit]] =
    ValidatePdf.fromDecoded(decodeStream(path, enableDiagnostics, config, batchSize, queueCapacity))

  /** Compare two PDFs via hyperdrive streams — incremental assembly per side. */
  def comparePathsHyperdrive(
    oldPath: Path,
    newPath: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZIO[Any, Throwable, Validation[CompareError, Unit]] =
    ComparePdfs.fromDecoded(
      decodeStream(oldPath, enableDiagnostics, config, batchSize, queueCapacity),
      decodeStream(newPath, enableDiagnostics, config, batchSize, queueCapacity)
    )

  private def attemptHyperdrive(
    path: Path,
    threshold: Long,
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config
  ): ZIO[Any, Throwable, Option[Chunk[Decoded]]] =
    ZIO.attemptBlocking(Files.size(path)).flatMap { size =>
      if PdfHyperdrive.fitsInHyperdrive(size, threshold) then
        warp(path, enableDiagnostics, config).map(Some(_))
      else
        ZIO.none
    }

  def validate(
    path: Path,
    chunkSize: Int = 64 * 1024,
    enableDiagnostics: Boolean = false
  ): ZIO[Any, Throwable, Validation[PdfError, Unit]] =
    validateHyperdrive(path, enableDiagnostics = enableDiagnostics)

  def comparePaths(
    oldPath: Path,
    newPath: Path,
    chunkSize: Int = 64 * 1024,
    enableDiagnostics: Boolean = false
  ): ZIO[Any, Throwable, Validation[CompareError, Unit]] =
    comparePathsHyperdrive(oldPath, newPath, enableDiagnostics = enableDiagnostics)
}
