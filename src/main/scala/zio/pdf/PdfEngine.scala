/*
 * ZIO service façade for PDF decode / ingest.
 *
 * Public byte bags are [[Chunk]] — never [[Array]]. Hot paths convert to
 * arrays only inside [[PdfHyperdrive]] / [[zio.pdf.pipe.HyperFuse]].
 *
 * {{{
 *   PdfEngine.decode(path).provide(PdfEngine.live)
 * }}}
 */

package zio.pdf

import java.nio.file.Path

import zio.*
import zio.prelude.Validation
import zio.stream.ZStream

trait PdfEngine:
  /** In-memory decode — pass [[PdfEngine.Options.default]] when opts are unused. */
  def decode(bytes: Chunk[Byte], opts: PdfEngine.Options): Task[Chunk[Decoded]]
  def decode(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): Task[Chunk[Decoded]]
  def stream(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): ZStream[Any, Throwable, Decoded]
  def elements(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default): ZStream[Any, Throwable, Element]
  def sink(path: Path, opts: PdfEngine.Options = PdfEngine.Options.default)(f: Decoded => Unit): Task[Long]

  /** Per-event effectful sink — runs `f` on the fuse thread (backpressures decode). */
  def sinkZIO[R](path: Path, opts: PdfEngine.Options = PdfEngine.Options.default)(
    f: Decoded => ZIO[R, Throwable, Unit]
  ): ZIO[R, Throwable, Long]

  def digest(path: Path): Task[Chunk[Byte]]
  def decodeAndDigest(
    path: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[(Chunk[Decoded], Chunk[Byte])]
  def validate(
    path: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PdfError, Unit]]
  def compare(
    old: Path,
    updated: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[CompareError, Unit]]
  def policy(
    path: Path,
    rules: Policy,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PolicyViolation, Unit]]
  def extractText(
    path: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZStream[Any, Throwable, PageText]

object PdfEngine:

  final case class Options(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  )

  object Options:
    val default: Options = Options()

  val default: Options = Options.default

  val live: ZLayer[Any, Nothing, PdfEngine] =
    ZLayer.succeed(Live)

  private object Live extends PdfEngine:
    def decode(bytes: Chunk[Byte], opts: Options): Task[Chunk[Decoded]] =
      ZIO.attemptBlocking {
        PdfHyperdrive.decodeSync(bytes.toArray, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def decode(path: Path, opts: Options = Options.default): Task[Chunk[Decoded]] =
      ZIO.attemptBlocking {
        PdfHyperdrive.decodeFromPath(path, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def stream(path: Path, opts: Options = Options.default): ZStream[Any, Throwable, Decoded] =
      HyperdriveStream.decoded(
        path,
        opts.enableDiagnostics,
        opts.config,
        opts.batchSize,
        opts.queueCapacity
      )

    def elements(path: Path, opts: Options = Options.default): ZStream[Any, Throwable, Element] =
      HyperdriveStream.elements(
        path,
        opts.enableDiagnostics,
        opts.config,
        opts.batchSize,
        opts.queueCapacity
      )

    def sink(path: Path, opts: Options = Options.default)(f: Decoded => Unit): Task[Long] =
      ZIO.attemptBlocking {
        PdfHyperdrive.decodeFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize)(f)
      }

    def sinkZIO[R](path: Path, opts: Options = Options.default)(
      f: Decoded => ZIO[R, Throwable, Unit]
    ): ZIO[R, Throwable, Long] =
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
      ZIO.attemptBlocking(Chunk.fromArray(PdfHyperdrive.digestFromPath(path)))

    def decodeAndDigest(
      path: Path,
      opts: Options = Options.default
    ): Task[(Chunk[Decoded], Chunk[Byte])] =
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

    def validate(
      path: Path,
      opts: Options = Options.default
    ): Task[Validation[PdfError, Unit]] =
      ValidatePdf.fromDecoded(stream(path, opts))

    def compare(
      old: Path,
      updated: Path,
      opts: Options = Options.default
    ): Task[Validation[CompareError, Unit]] =
      ComparePdfs.fromDecoded(stream(old, opts), stream(updated, opts))

    def policy(
      path: Path,
      rules: Policy,
      opts: Options = Options.default
    ): Task[Validation[PolicyViolation, Unit]] =
      PdfPolicy.fromDecoded(rules)(stream(path, opts))

    def extractText(
      path: Path,
      opts: Options = Options.default
    ): ZStream[Any, Throwable, PageText] =
      ZStream.unwrap {
        elements(path, opts)
          .runFold(TextExtract.Acc())(TextExtract.fold)
          .map(acc => ZStream.fromChunk(TextExtract.finish(acc)))
      }

  def decode(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    decode(bytes, Options.default)

  def decode(
    bytes: Chunk[Byte],
    opts: Options
  ): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    ZIO.serviceWithZIO[PdfEngine](_.decode(bytes, opts))

  def decode(
    path: Path,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    ZIO.serviceWithZIO[PdfEngine](_.decode(path, opts))

  def stream(
    path: Path,
    opts: Options = Options.default
  ): ZStream[PdfEngine, Throwable, Decoded] =
    ZStream.serviceWithStream[PdfEngine](_.stream(path, opts))

  def elements(
    path: Path,
    opts: Options = Options.default
  ): ZStream[PdfEngine, Throwable, Element] =
    ZStream.serviceWithStream[PdfEngine](_.elements(path, opts))

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

  def validate(
    path: Path,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PdfError, Unit]] =
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

  def extractText(
    path: Path,
    opts: Options = Options.default
  ): ZStream[PdfEngine, Throwable, PageText] =
    ZStream.serviceWithStream[PdfEngine](_.extractText(path, opts))
