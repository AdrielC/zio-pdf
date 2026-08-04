/*
 * ZIO service façade for PDF decode / ingest.
 *
 * **Compose on `ZStream[Byte]`** via [[decoded]] / [[elements]] / [[streaming]]
 * pipelines; whole-stream folds via [[runValidate]] / [[runDigest]] or
 * [[digestSink]]. Path and [[Chunk]] helpers are thin wrappers (fused mmap
 * where noted).
 *
 * {{{
 *   import zio.pdf.io.PdfIO
 *   PdfIO.reader(path).via(PdfEngine.decoded()).runCollect.provide(PdfEngine.live)
 *   PdfIO.reader(path).run(PdfEngine.digestSink.provide(PdfEngine.live))
 * }}}
 */

package zio.pdf

import java.nio.file.Path
import java.security.MessageDigest

import zio.*
import zio.prelude.Validation
import zio.pdf.io.PdfIO
import zio.stream.{ZPipeline, ZSink, ZStream}

trait PdfEngine:

  // --- composable core (ZStream[Byte] in) -----------------------------------

  def decoded(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, Decoded]
  def elements(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, Element]
  def streaming(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, StreamingDecoded]
  def extractTextPipeline(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, PageText]

  /** Incremental SHA-256 over raw bytes (no decode). Result in `Z`; no leftover. */
  val digestSink: ZSink[Any, Throwable, Byte, Nothing, Chunk[Byte]]

  // --- byte source --------------------------------------------------------

  def bytes(path: Path, chunkSize: Int = 64 * 1024): ZStream[Any, Throwable, Byte]

  // --- whole-stream runners (bytes → result) --------------------------------

  def runValidate(
    bytes: ZStream[Any, Throwable, Byte],
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PdfError, Unit]]

  def runPolicy(
    bytes: ZStream[Any, Throwable, Byte],
    rules: Policy,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PolicyViolation, Unit]]

  def runDigest(bytes: ZStream[Any, Throwable, Byte]): Task[Chunk[Byte]]

  def compareStreams(
    old: ZStream[Any, Throwable, Byte],
    updated: ZStream[Any, Throwable, Byte],
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[CompareError, Unit]]

  // --- fused fast path (path / Chunk — Hyperdrive, no ZChannel) ------------

  def decode(bytes: Chunk[Byte], opts: PdfEngine.Options): Task[Chunk[Decoded]]
  def decode(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): Task[Chunk[Decoded]]
  def sink(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default)(f: Decoded => Unit): Task[Long]

  def sinkZIO[R](path: Path, opts: PdfEngine.Options = PdfEngine.Options.default)(
    f: Decoded => ZIO[R, Throwable, Unit]
  ): ZIO[R, Throwable, Long]

  def digest(path: Path): Task[Chunk[Byte]]
  def decodeAndDigest(
    path: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[(Chunk[Decoded], Chunk[Byte])]

  def elements(bytes: Chunk[Byte], opts: PdfEngine.Options): Task[Chunk[Element]]
  def elements(path: Path, opts: PdfEngine.Options): Task[Chunk[Element]]
  def elementsSink(path: Path, opts: PdfEngine.Options)(f: Element => Unit): Task[Long]

  def elementsSinkZIO[R](path: Path, opts: PdfEngine.Options)(
    f: Element => ZIO[R, Throwable, Unit]
  ): ZIO[R, Throwable, Long]

  // --- path ergonomics (fused stream / pipeline runners) --------------------

  def stream(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): ZStream[Any, Throwable, Decoded]
  def elementsStream(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): ZStream[Any, Throwable, Element]

  def validate(
    path: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PdfError, Unit]]

  def policy(
    path: Path,
    rules: Policy,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PolicyViolation, Unit]]

  def extractText(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): ZStream[Any, Throwable, PageText]

  def compare(
    old: Path,
    updated: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[CompareError, Unit]]

object PdfEngine:

  final case class Options(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )

  object Options:
    val default: Options = Options()

  val default: Options = Options.default

  val live: ZLayer[Any, Nothing, PdfEngine] =
    ZLayer.succeed(Live)

  private object Live extends PdfEngine:

    def decoded(opts: Options): ZPipeline[Any, Throwable, Byte, Decoded] =
      PdfStream.decode(opts.enableDiagnostics, opts.config)

    def elements(opts: Options): ZPipeline[Any, Throwable, Byte, Element] =
      PdfStream.elements(opts.enableDiagnostics)

    def streaming(opts: Options): ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
      PdfStream.streamingDecode(opts.enableDiagnostics, opts.config)

    def extractTextPipeline(opts: Options): ZPipeline[Any, Throwable, Byte, PageText] =
      PdfStream.extractText(opts.enableDiagnostics)

    val digestSink: ZSink[Any, Throwable, Byte, Nothing, Chunk[Byte]] =
      ZSink
        .foldLeftChunksZIO(MessageDigest.getInstance("SHA-256")) { (md, chunk: Chunk[Byte]) =>
          ZIO.attemptBlocking {
            chunk.materialize match {
              case Chunk.ByteArray(arr, off, len) => md.update(arr, off, len)
              case _ =>
                val arr = chunk.toArray
                md.update(arr, 0, arr.length)
            }
            md
          }
        }
        .map(md => Chunk.fromArray(md.digest()))

    def bytes(path: Path, chunkSize: Int): ZStream[Any, Throwable, Byte] =
      PdfIO.reader(path, chunkSize)

    def runValidate(bytes: ZStream[Any, Throwable, Byte], opts: Options): Task[Validation[PdfError, Unit]] =
      ValidatePdf.fromDecoded(bytes.via(decoded(opts)))

    def runPolicy(
      bytes: ZStream[Any, Throwable, Byte],
      rules: Policy,
      opts: Options
    ): Task[Validation[PolicyViolation, Unit]] =
      PdfPolicy.fromDecoded(rules)(bytes.via(decoded(opts)))

    def runDigest(bytes: ZStream[Any, Throwable, Byte]): Task[Chunk[Byte]] =
      bytes.run(digestSink)

    def compareStreams(
      old: ZStream[Any, Throwable, Byte],
      updated: ZStream[Any, Throwable, Byte],
      opts: Options
    ): Task[Validation[CompareError, Unit]] =
      ComparePdfs.fromDecoded(old.via(decoded(opts)), updated.via(decoded(opts)))

    def decode(bytes: Chunk[Byte], opts: Options): Task[Chunk[Decoded]] =
      ZIO.attemptBlocking {
        PdfHyperdrive.decodeSync(bytes.toArray, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def decode(path: Path, opts: Options): Task[Chunk[Decoded]] =
      ZIO.attemptBlocking {
        PdfHyperdrive.decodeFromPath(path, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def sink(path: Path, opts: Options)(f: Decoded => Unit): Task[Long] =
      ZIO.attemptBlocking {
        PdfHyperdrive.decodeFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize)(f)
      }

    def sinkZIO[R](path: Path, opts: Options)(f: Decoded => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Long] =
      ZIO.runtime[R].flatMap { runtime =>
        ZIO.attemptBlockingInterrupt {
          import zio.Unsafe
          var count = 0L
          PdfHyperdrive.decodeFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize) { decoded =>
            count += 1
            Unsafe.unsafe { implicit u =>
              runtime.unsafe.run(f(decoded)).getOrThrow()
            }
          }
          count
        }
      }

    def digest(path: Path): Task[Chunk[Byte]] =
      runDigest(bytes(path))

    def decodeAndDigest(path: Path, opts: Options): Task[(Chunk[Decoded], Chunk[Byte])] =
      ZIO.attemptBlocking {
        val (decoded, dig) =
          PdfHyperdrive.decodeAndDigestFromPath(
            path,
            opts.enableDiagnostics,
            opts.config,
            opts.batchSize
          )
        (decoded, Chunk.fromArray(dig))
      }

    def elements(bytes: Chunk[Byte], opts: Options): Task[Chunk[Element]] =
      ZIO.attemptBlocking {
        PdfHyperdrive.elementsSync(bytes.toArray, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def elements(path: Path, opts: Options): Task[Chunk[Element]] =
      ZIO.attemptBlocking {
        PdfHyperdrive.elementsFromPath(path, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def elementsSink(path: Path, opts: Options)(f: Element => Unit): Task[Long] =
      ZIO.attemptBlocking {
        PdfHyperdrive.elementsFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize)(f)
      }

    def elementsSinkZIO[R](path: Path, opts: Options)(f: Element => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Long] =
      ZIO.runtime[R].flatMap { runtime =>
        ZIO.attemptBlockingInterrupt {
          import zio.Unsafe
          var count = 0L
          PdfHyperdrive.elementsFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize) { element =>
            count += 1
            Unsafe.unsafe { implicit u =>
              runtime.unsafe.run(f(element)).getOrThrow()
            }
          }
          count
        }
      }

    def stream(path: Path, opts: Options): ZStream[Any, Throwable, Decoded] =
      bytes(path).via(decoded(opts))

    def elementsStream(path: Path, opts: Options): ZStream[Any, Throwable, Element] =
      HyperdriveStream.elements(path, opts.enableDiagnostics, opts.config, opts.batchSize)

    def validate(path: Path, opts: Options): Task[Validation[PdfError, Unit]] =
      runValidate(bytes(path), opts)

    def policy(path: Path, rules: Policy, opts: Options): Task[Validation[PolicyViolation, Unit]] =
      runPolicy(bytes(path), rules, opts)

    def extractText(path: Path, opts: Options): ZStream[Any, Throwable, PageText] =
      bytes(path).via(extractTextPipeline(opts))

    def compare(old: Path, updated: Path, opts: Options): Task[Validation[CompareError, Unit]] =
      compareStreams(bytes(old), bytes(updated), opts)

  // --- service accessors: pipelines -----------------------------------------

  def decoded(): ZPipeline[PdfEngine, Throwable, Byte, Decoded] =
    decoded(Options.default)

  def decoded(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, Decoded] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.decoded(opts))

  def elements(): ZPipeline[PdfEngine, Throwable, Byte, Element] =
    elements(Options.default)

  def elements(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, Element] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.elements(opts))

  def streaming(): ZPipeline[PdfEngine, Throwable, Byte, StreamingDecoded] =
    streaming(Options.default)

  def streaming(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, StreamingDecoded] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.streaming(opts))

  def extractTextPipeline(): ZPipeline[PdfEngine, Throwable, Byte, PageText] =
    extractTextPipeline(Options.default)

  def extractTextPipeline(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, PageText] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.extractTextPipeline(opts))

  def digestSink: ZSink[PdfEngine, Throwable, Byte, Nothing, Chunk[Byte]] =
    ZSink.serviceWithSink[PdfEngine](_.digestSink)

  def bytes(path: Path, chunkSize: Int = 64 * 1024): ZStream[PdfEngine, Throwable, Byte] =
    ZStream.serviceWithStream[PdfEngine](_.bytes(path, chunkSize))

  // --- service accessors: runners / fused -----------------------------------

  def decode(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    decode(bytes, Options.default)

  def decode(bytes: Chunk[Byte], opts: Options): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    ZIO.serviceWithZIO[PdfEngine](_.decode(bytes, opts))

  def decode(path: Path, opts: Options = Options.default): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    ZIO.serviceWithZIO[PdfEngine](_.decode(path, opts))

  def stream(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, Decoded] =
    ZStream.serviceWithStream[PdfEngine](_.stream(path, opts))

  /** Fused mmap element stream (backpressure queue). Same events as [[elements]]. */
  def elementsStream(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, Element] =
    ZStream.serviceWithStream[PdfEngine](_.elementsStream(path, opts))

  /** @deprecated("Use [[elementsStream]]", "0.2.0") */
  def elementsFrom(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, Element] =
    elementsStream(path, opts)

  def runValidate(
    bytes: ZStream[Any, Throwable, Byte],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PdfError, Unit]] =
    ZIO.serviceWithZIO[PdfEngine](_.runValidate(bytes, opts))

  def runPolicy(
    bytes: ZStream[Any, Throwable, Byte],
    rules: Policy,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PolicyViolation, Unit]] =
    ZIO.serviceWithZIO[PdfEngine](_.runPolicy(bytes, rules, opts))

  def runDigest(bytes: ZStream[Any, Throwable, Byte]): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    ZIO.serviceWithZIO[PdfEngine](_.runDigest(bytes))

  def compareStreams(
    old: ZStream[Any, Throwable, Byte],
    updated: ZStream[Any, Throwable, Byte],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[CompareError, Unit]] =
    ZIO.serviceWithZIO[PdfEngine](_.compareStreams(old, updated, opts))

  def sink(path: Path, opts: Options = Options.default)(f: Decoded => Unit): ZIO[PdfEngine, Throwable, Long] =
    ZIO.serviceWithZIO[PdfEngine](_.sink(path, opts)(f))

  def sinkZIO[R](path: Path, opts: Options = Options.default)(
    f: Decoded => ZIO[R, Throwable, Unit]
  ): ZIO[R & PdfEngine, Throwable, Long] =
    ZIO.serviceWithZIO[PdfEngine](_.sinkZIO(path, opts)(f))

  def digest(path: Path): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    ZIO.serviceWithZIO[PdfEngine](_.digest(path))

  def decodeAndDigest(
    path: Path,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, (Chunk[Decoded], Chunk[Byte])] =
    ZIO.serviceWithZIO[PdfEngine](_.decodeAndDigest(path, opts))

  def elements(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    elements(bytes, Options.default)

  def elements(bytes: Chunk[Byte], opts: Options): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    ZIO.serviceWithZIO[PdfEngine](_.elements(bytes, opts))

  def elements(path: Path): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    elements(path, Options.default)

  def elements(path: Path, opts: Options): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    ZIO.serviceWithZIO[PdfEngine](_.elements(path, opts))

  def elementsSink(path: Path, opts: Options = Options.default)(f: Element => Unit): ZIO[PdfEngine, Throwable, Long] =
    ZIO.serviceWithZIO[PdfEngine](_.elementsSink(path, opts)(f))

  def elementsSinkZIO[R](path: Path, opts: Options = Options.default)(
    f: Element => ZIO[R, Throwable, Unit]
  ): ZIO[R & PdfEngine, Throwable, Long] =
    ZIO.serviceWithZIO[PdfEngine](_.elementsSinkZIO(path, opts)(f))

  def validate(path: Path, opts: Options = Options.default): ZIO[PdfEngine, Throwable, Validation[PdfError, Unit]] =
    ZIO.serviceWithZIO[PdfEngine](_.validate(path, opts))

  def compare(
    old: Path,
    updated: Path,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[CompareError, Unit]] =
    ZIO.serviceWithZIO[PdfEngine](_.compare(old, updated, opts))

  def policy(
    path: Path,
    rules: Policy,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PolicyViolation, Unit]] =
    ZIO.serviceWithZIO[PdfEngine](_.policy(path, rules, opts))

  def extractText(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, PageText] =
    ZStream.serviceWithStream[PdfEngine](_.extractText(path, opts))
