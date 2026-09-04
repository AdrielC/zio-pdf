/*
 * ZIO service façade for PDF decode / ingest.
 *
 * The source selects the execution strategy, not a mode flag: `Path` and
 * [[zio.Chunk]] and caller-owned `ZStream[Byte]` inputs both feed the same
 * bounded incremental decoder session. The result can then be collected,
 * consumed incrementally, or sent to a callback.
 *
 * {{{
 *   import zio.pdf.io.PdfIO
 *   PdfEngine.decode(PdfIO.reader(path)).runCollect.provide(PdfEngine.live)
 *   PdfIO.reader(path).run(PdfEngine.digestSink.provide(PdfEngine.live))
 * }}}
 */

package zio.pdf

import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicLong

import zio.*
import zio.prelude.Validation
import zio.pdf.io.PdfIO
import zio.stream.{ZPipeline, ZSink, ZStream}

trait PdfEngine:

  // --- caller-owned byte-source adapters ------------------------------------

  def decoded(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, Decoded]
  def elements(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, Element]
  def streaming(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, StreamingDecoded]
  def extractTextPipeline(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, PageText]

  /** Incremental SHA-256 over raw bytes (no decode). Result in `Z`; no leftover. */
  val digestSink: ZSink[Any, Throwable, Byte, Nothing, Chunk[Byte]]

  // --- byte source --------------------------------------------------------

  def bytes(path: Path, chunkSize: Int = 64 * 1024): ZStream[Any, Throwable, Byte]

  // --- whole-stream runners (bytes → result) --------------------------------

  @deprecated("Use validate(source)", "0.2.0-RC1")
  def runValidate(
    bytes: ZStream[Any, Throwable, Byte],
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PdfError, Unit]]

  @deprecated("Use policy(source, rules)", "0.2.0-RC1")
  def runPolicy(
    bytes: ZStream[Any, Throwable, Byte],
    rules: Policy,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[PolicyViolation, Unit]]

  @deprecated("Use digest(source)", "0.2.0-RC1")
  def runDigest(bytes: ZStream[Any, Throwable, Byte]): Task[Chunk[Byte]]

  @deprecated("Use compare(old, updated)", "0.2.0-RC1")
  def compareStreams(
    old: ZStream[Any, Throwable, Byte],
    updated: ZStream[Any, Throwable, Byte],
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[CompareError, Unit]]

  // --- Path / Chunk decode (fused, with an adaptive owned-byte fast path) ---

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

  // --- Path output shape -----------------------------------------------------

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

  /** Full provenance bundle through one fused decode and raw-byte digest pass. */
  def evidence(
    bytes: Chunk[Byte],
    plan: PdfEvidence.Plan,
    opts: PdfEngine.Options
  ): Task[PdfEvidence.Bundle]

  /** Full provenance bundle through one fused file read, decode, and digest pass. */
  def evidence(
    path: Path,
    plan: PdfEvidence.Plan,
    opts: PdfEngine.Options
  ): Task[PdfEvidence.Bundle]

  def compare(
    old: Path,
    updated: Path,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): Task[Validation[CompareError, Unit]]

object PdfEngine:

  final case class Options(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    maxInputBytes: Long = Long.MaxValue,
    maxMaterializedDocumentBytes: ByteLimit = ByteLimit.DefaultDocumentMaterialization
  ):
    require(batchSize > 0, "batchSize must be positive")
    require(maxInputBytes > 0L, "maxInputBytes must be positive")

  /** Raised before decode when an input exceeds [[Options.maxInputBytes]]. */
  final case class InputTooLarge(maxInputBytes: Long, observedBytes: Long)
      extends RuntimeException(
        s"PDF input is $observedBytes bytes, above the configured $maxInputBytes-byte limit"
      )

  /** Raised only by APIs whose return value retains a complete document timeline. */
  final case class MaterializedDocumentLimitExceeded(maxBytes: ByteLimit, observedBytes: Long)
      extends RuntimeException(
        s"PDF input is $observedBytes bytes, above the configured ${maxBytes.bytes}-byte materialized-document limit"
      )

  object Options:
    val default: Options = Options()

  val default: Options = Options.default

  /** Bound any API whose result retains the complete decoded document. */
  private[pdf] def materializedInputLimit(opts: Options): ZPipeline[Any, Throwable, Byte, Byte] =
    ZPipeline.fromFunction[Any, Throwable, Byte, Byte] { input =>
      val seen = new AtomicLong(0L)
      input.mapChunksZIO { chunk =>
        val observed = seen.addAndGet(chunk.size.toLong)
        if observed > opts.maxMaterializedDocumentBytes.toLong then
          ZIO.fail(MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, observed))
        else ZIO.succeed(chunk)
      }
    }

  val live: ZLayer[Any, Nothing, PdfEngine] =
    ZLayer.succeed(Live)

  private object Live extends PdfEngine:

    private def enforceInputLimit(observedBytes: Long, opts: Options): Unit =
      if observedBytes > opts.maxInputBytes then
        throw InputTooLarge(opts.maxInputBytes, observedBytes)

    private def enforceMaterializationLimit(observedBytes: Long, opts: Options): Unit =
      if observedBytes > opts.maxMaterializedDocumentBytes.toLong then
        throw MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, observedBytes)

    /** A fresh counter per execution; never silently truncates input. */
    private def inputLimit(opts: Options): ZPipeline[Any, Throwable, Byte, Byte] =
      ZPipeline.fromFunction[Any, Throwable, Byte, Byte] { input =>
        val seen = new AtomicLong(0L)
        input.mapChunksZIO { chunk =>
          val observed = seen.addAndGet(chunk.size.toLong)
          if observed > opts.maxInputBytes then ZIO.fail(InputTooLarge(opts.maxInputBytes, observed))
          else ZIO.succeed(chunk)
        }
      }

    private def checkPath(path: Path, opts: Options): Task[Unit] =
      ZIO.attemptBlocking(enforceInputLimit(Files.size(path), opts))

    private def checkMaterializedPath(path: Path, opts: Options): Task[Unit] =
      ZIO.attemptBlocking {
        val size = Files.size(path)
        enforceInputLimit(size, opts)
        enforceMaterializationLimit(size, opts)
      }

    private def boundedArray(bytes: Chunk[Byte], opts: Options): Task[Array[Byte]] =
      ZIO.attemptBlocking {
        enforceInputLimit(bytes.size.toLong, opts)
        enforceMaterializationLimit(bytes.size.toLong, opts)
        bytes match {
          case Chunk.ByteArray(array, 0, length) if length == array.length => array
          case Chunk.ByteArray(array, offset, length) =>
            java.util.Arrays.copyOfRange(array, offset, offset + length)
          case _ => bytes.toArray
        }
      }

    def decoded(opts: Options): ZPipeline[Any, Throwable, Byte, Decoded] =
      inputLimit(opts) >>> PdfStream.decode(opts.enableDiagnostics, opts.config, opts.batchSize)

    def elements(opts: Options): ZPipeline[Any, Throwable, Byte, Element] =
      inputLimit(opts) >>> PdfStream.elements(opts.enableDiagnostics, opts.config, opts.batchSize)

    def streaming(opts: Options): ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
      inputLimit(opts) >>> PdfStream.streamingDecode(opts.enableDiagnostics, opts.config, opts.batchSize)

    def extractTextPipeline(opts: Options): ZPipeline[Any, Throwable, Byte, PageText] =
      inputLimit(opts) >>> PdfStream.extractText(opts.enableDiagnostics, opts.config, opts.batchSize)

    val digestSink: ZSink[Any, Throwable, Byte, Nothing, Chunk[Byte]] =
      ZSink
        .foldLeftChunksZIO(MessageDigest.getInstance("SHA-256")) { (md, chunk: Chunk[Byte]) =>
          ZIO.attemptBlocking {
            DigestChunk.update(md, chunk)
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
      boundedArray(bytes, opts).flatMap { array =>
        val fused = ZIO.succeed(PdfHyperdrive.decodeSync(array, opts.enableDiagnostics, opts.config, opts.batchSize))
        ZIO
          .attempt(StructuralIndex.index(array))
          .map(_.filter(_ => !opts.enableDiagnostics && array.length >= StructuralIndex.MinimumParallelBytes))
          .flatMap(_.fold(fused)(_.decode(array)))
          .catchAll(_ => fused)
      }

    def decode(path: Path, opts: Options): Task[Chunk[Decoded]] =
      checkMaterializedPath(path, opts) *> ZIO.attemptBlocking {
        PdfHyperdrive.decodeFromPath(path, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def evidence(bytes: Chunk[Byte], plan: PdfEvidence.Plan, opts: Options): Task[PdfEvidence.Bundle] =
      ZIO.succeed(EvidenceDigest.create()).flatMap { digest =>
        val tapped = ZStream.fromChunk(bytes).mapChunks { chunk =>
          digest.update(chunk)
          chunk
        }
        PdfEvidence.fromDecoded(tapped.via(decoded(opts)), plan, digest.finish())
      }

    def evidence(path: Path, plan: PdfEvidence.Plan, opts: Options): Task[PdfEvidence.Bundle] =
      checkPath(path, opts) *> ZIO.attemptBlocking {
        val accumulator = PdfEvidence.accumulator(plan)
        val (_, digest) = PdfHyperdrive.decodeAndDigestFromPathSink(
          path,
          opts.enableDiagnostics,
          opts.config,
          opts.batchSize
        )(accumulator.add)
        accumulator.result(Chunk.fromArray(digest))
      }

    def sink(path: Path, opts: Options)(f: Decoded => Unit): Task[Long] =
      checkPath(path, opts) *> ZIO.attemptBlocking {
        PdfHyperdrive.decodeFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize)(f)
      }

    def sinkZIO[R](path: Path, opts: Options)(f: Decoded => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Long] =
      decodedFromPath(path, opts).runFoldZIO(0L) { (count, decoded) =>
        f(decoded).as(count + 1L)
      }

    def digest(path: Path): Task[Chunk[Byte]] =
      ZIO.attemptBlocking(Chunk.fromArray(PdfHyperdrive.digestFromPath(path)))

    def decodeAndDigest(path: Path, opts: Options): Task[(Chunk[Decoded], Chunk[Byte])] =
      checkMaterializedPath(path, opts) *> ZIO.attemptBlocking {
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
      boundedArray(bytes, opts).flatMap { array =>
        ZIO.attemptBlocking {
          PdfHyperdrive.elementsSync(array, opts.enableDiagnostics, opts.config, opts.batchSize)
        }
      }

    def elements(path: Path, opts: Options): Task[Chunk[Element]] =
      checkMaterializedPath(path, opts) *> ZIO.attemptBlocking {
        PdfHyperdrive.elementsFromPath(path, opts.enableDiagnostics, opts.config, opts.batchSize)
      }

    def elementsSink(path: Path, opts: Options)(f: Element => Unit): Task[Long] =
      checkPath(path, opts) *> ZIO.attemptBlocking {
        PdfHyperdrive.elementsFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize)(f)
      }

    def elementsSinkZIO[R](path: Path, opts: Options)(f: Element => ZIO[R, Throwable, Unit]): ZIO[R, Throwable, Long] =
      elementsFromPath(path, opts).runFoldZIO(0L) { (count, element) =>
        f(element).as(count + 1L)
      }

    private def assembledFromPath(path: Path, opts: Options): Task[Validation[AssemblyError, ValidatedPdf]] =
      checkMaterializedPath(path, opts) *> ZIO.attemptBlocking {
        val accumulator = AssemblePdf.accumulator()
        PdfHyperdrive.decodeFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize) { decoded =>
          accumulator.add(decoded)
        }
        accumulator.result
      }

    private def textFromPath(path: Path, opts: Options): Task[Chunk[PageText]] =
      checkMaterializedPath(path, opts) *> ZIO.attemptBlocking {
        var accumulator = TextExtract.Acc()
        PdfHyperdrive.elementsFromPathSink(path, opts.enableDiagnostics, opts.config, opts.batchSize) { element =>
          accumulator = TextExtract.fold(accumulator, element)
        }
        TextExtract.finish(accumulator)
      }

    private def decodedFromPath(path: Path, opts: Options): ZStream[Any, Throwable, Decoded] =
      ZStream.unwrap {
        checkPath(path, opts).as {
          HyperdriveStream.decoded(path, opts.enableDiagnostics, opts.config, opts.batchSize)
        }
      }

    private def elementsFromPath(path: Path, opts: Options): ZStream[Any, Throwable, Element] =
      ZStream.unwrap {
        checkPath(path, opts).as {
          HyperdriveStream.elements(path, opts.enableDiagnostics, opts.config, opts.batchSize)
        }
      }

    def stream(path: Path, opts: Options): ZStream[Any, Throwable, Decoded] =
      decodedFromPath(path, opts)

    def elementsStream(path: Path, opts: Options): ZStream[Any, Throwable, Element] =
      elementsFromPath(path, opts)

    def validate(path: Path, opts: Options): Task[Validation[PdfError, Unit]] =
      assembledFromPath(path, opts).map(ValidatePdf.fromAssembly)

    def policy(path: Path, rules: Policy, opts: Options): Task[Validation[PolicyViolation, Unit]] =
      assembledFromPath(path, opts).map(PdfPolicy.fromAssembly(rules))

    def extractText(path: Path, opts: Options): ZStream[Any, Throwable, PageText] =
      ZStream
        .fromZIO(textFromPath(path, opts))
        .flatMap(pageTexts => ZStream.fromChunk(pageTexts))

    def compare(old: Path, updated: Path, opts: Options): Task[Validation[CompareError, Unit]] =
      assembledFromPath(old, opts).zipPar(assembledFromPath(updated, opts)).map { case (oldV, newV) =>
        ComparePdfs.fromAssemblies(oldV, newV)
      }

  // --- service accessors: pipelines -----------------------------------------

  def decoded(): ZPipeline[PdfEngine, Throwable, Byte, Decoded] =
    decoded(Options.default)

  def decoded(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, Decoded] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.decoded(opts))

  /**
   * Decode a caller-owned byte source. `Path` and `Chunk` overloads use the
   * fused decoder; this overload is for sockets, stdin, and other sources
   * whose chunking and lifetime are owned by the caller.
   */
  def decode[R](source: ZStream[R, Throwable, Byte]): ZStream[R & PdfEngine, Throwable, Decoded] =
    decode(source, Options.default)

  def decode[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZStream[R & PdfEngine, Throwable, Decoded] =
    source.via(decoded(opts))

  /**
   * Stream a schema-aware PDF diff. Each result is an exact LCS edit script
   * for a bounded component window; the inputs are never assembled in full.
   */
  def diff[R1, R2](
    old: ZStream[R1, Throwable, Byte],
    updated: ZStream[R2, Throwable, Byte],
    config: PdfDiff.Config = PdfDiff.Config.default,
    opts: Options = Options.default
  ): ZStream[R1 & R2 & PdfEngine, Throwable, PdfDiff.Window] =
    PdfDiff.fromDecoded(decode(old, opts), decode(updated, opts), config)

  def elements(): ZPipeline[PdfEngine, Throwable, Byte, Element] =
    elements(Options.default)

  def elements(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, Element] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.elements(opts))

  /** Classify a caller-owned byte source without selecting a separate API. */
  def elements[R](source: ZStream[R, Throwable, Byte]): ZStream[R & PdfEngine, Throwable, Element] =
    elements(source, Options.default)

  def elements[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZStream[R & PdfEngine, Throwable, Element] =
    source.via(elements(opts))

  /**
   * Run a composable inspection plan while elements are decoded. Plans
   * short-circuit positive observations and policy violations without first
   * collecting the document.
   */
  def inspect[R](
    source: ZStream[R, Throwable, Byte],
    plan: PdfInspection.Plan
  ): ZIO[R & PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(source, plan, Options.default)

  def inspect[R](
    source: ZStream[R, Throwable, Byte],
    plan: PdfInspection.Plan,
    opts: Options
  ): ZIO[R & PdfEngine, Throwable, PdfInspection.Outcome] =
    PdfInspection.run(elements(source, opts), plan)

  def streaming(): ZPipeline[PdfEngine, Throwable, Byte, StreamingDecoded] =
    streaming(Options.default)

  def streaming(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, StreamingDecoded] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.streaming(opts))

  /** Raw streaming events for a caller-owned byte source. */
  def streaming[R](source: ZStream[R, Throwable, Byte]): ZStream[R & PdfEngine, Throwable, StreamingDecoded] =
    streaming(source, Options.default)

  def streaming[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZStream[R & PdfEngine, Throwable, StreamingDecoded] =
    source.via(streaming(opts))

  def extractTextPipeline(): ZPipeline[PdfEngine, Throwable, Byte, PageText] =
    extractTextPipeline(Options.default)

  def extractTextPipeline(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, PageText] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.extractTextPipeline(opts))

  /** Extract literal page text from a caller-owned byte source. */
  def extractText[R](source: ZStream[R, Throwable, Byte]): ZStream[R & PdfEngine, Throwable, PageText] =
    extractText(source, Options.default)

  def extractText[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZStream[R & PdfEngine, Throwable, PageText] =
    source.via(extractTextPipeline(opts))

  /**
   * Decode a caller-owned source once and merge all evidence observers over
   * that event stream. The digest tap forwards the original chunks unchanged,
   * so it does not introduce a second source read or decoder pipeline.
   */
  def evidence[R](source: ZStream[R, Throwable, Byte]): ZIO[R & PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(source, PdfEvidence.Plan.complete, Options.default)

  def evidence[R](
    source: ZStream[R, Throwable, Byte],
    plan: PdfEvidence.Plan
  ): ZIO[R & PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(source, plan, Options.default)

  def evidence[R](
    source: ZStream[R, Throwable, Byte],
    plan: PdfEvidence.Plan,
    opts: Options
  ): ZIO[R & PdfEngine, Throwable, PdfEvidence.Bundle] =
    ZIO.succeed(EvidenceDigest.create()).flatMap { digest =>
      val tapped = source.mapChunks { chunk =>
        digest.update(chunk)
        chunk
      }
      PdfEvidence.fromDecoded(decode(tapped, opts), plan, digest.finish())
    }

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

  /** Full bundle for owned bytes through the JVM fused decoder. */
  def evidence(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(bytes, PdfEvidence.Plan.complete, Options.default)

  def evidence(bytes: Chunk[Byte], plan: PdfEvidence.Plan): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(bytes, plan, Options.default)

  def evidence(
    bytes: Chunk[Byte],
    plan: PdfEvidence.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    ZIO.serviceWithZIO[PdfEngine](_.evidence(bytes, plan, opts))

  /** Full bundle for a JVM file through one bounded read/decode/digest pass. */
  def evidence(path: Path): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(path, PdfEvidence.Plan.complete, Options.default)

  def evidence(path: Path, plan: PdfEvidence.Plan): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(path, plan, Options.default)

  def evidence(
    path: Path,
    plan: PdfEvidence.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    ZIO.serviceWithZIO[PdfEngine](_.evidence(path, plan, opts))

  def stream(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, Decoded] =
    ZStream.serviceWithStream[PdfEngine](_.stream(path, opts))

  /** Incremental path element stream. Same events as [[elements]]. */
  def elementsStream(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, Element] =
    ZStream.serviceWithStream[PdfEngine](_.elementsStream(path, opts))

  /** @deprecated("Use [[elementsStream]]", "0.2.0") */
  def elementsFrom(path: Path, opts: Options = Options.default): ZStream[PdfEngine, Throwable, Element] =
    elementsStream(path, opts)

  @deprecated("Use validate(source)", "0.2.0-RC1")
  def runValidate(
    bytes: ZStream[Any, Throwable, Byte],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PdfError, Unit]] =
    validate(bytes, opts)

  @deprecated("Use policy(source, rules)", "0.2.0-RC1")
  def runPolicy(
    bytes: ZStream[Any, Throwable, Byte],
    rules: Policy,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PolicyViolation, Unit]] =
    policy(bytes, rules, opts)

  @deprecated("Use digest(source)", "0.2.0-RC1")
  def runDigest(bytes: ZStream[Any, Throwable, Byte]): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    digest(bytes)

  @deprecated("Use compare(old, updated)", "0.2.0-RC1")
  def compareStreams(
    old: ZStream[Any, Throwable, Byte],
    updated: ZStream[Any, Throwable, Byte],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[CompareError, Unit]] =
    compare(old, updated, opts)

  /** Validate a caller-owned byte source. */
  def validate[R](source: ZStream[R, Throwable, Byte]): ZIO[R & PdfEngine, Throwable, Validation[PdfError, Unit]] =
    validate(source, Options.default)

  def validate[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZIO[R & PdfEngine, Throwable, Validation[PdfError, Unit]] =
    ValidatePdf.fromDecoded(decode(source, opts))

  /** Apply a policy to a caller-owned byte source. */
  def policy[R](
    source: ZStream[R, Throwable, Byte],
    rules: Policy
  ): ZIO[R & PdfEngine, Throwable, Validation[PolicyViolation, Unit]] =
    policy(source, rules, Options.default)

  def policy[R](
    source: ZStream[R, Throwable, Byte],
    rules: Policy,
    opts: Options
  ): ZIO[R & PdfEngine, Throwable, Validation[PolicyViolation, Unit]] =
    PdfPolicy.fromDecoded(rules)(decode(source, opts))

  /** Hash a caller-owned byte source without decoding it. */
  def digest[R](source: ZStream[R, Throwable, Byte]): ZIO[R & PdfEngine, Throwable, Chunk[Byte]] =
    source.run(digestSink)

  /** Structurally compare two caller-owned byte sources. */
  def compare[R](
    old: ZStream[R, Throwable, Byte],
    updated: ZStream[R, Throwable, Byte]
  ): ZIO[R & PdfEngine, Throwable, Validation[CompareError, Unit]] =
    compare(old, updated, Options.default)

  def compare[R](
    old: ZStream[R, Throwable, Byte],
    updated: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZIO[R & PdfEngine, Throwable, Validation[CompareError, Unit]] =
    ComparePdfs.fromDecoded(decode(old, opts), decode(updated, opts))

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

  /** Run a plan through the fused incremental path decoder. */
  def inspect(path: Path, plan: PdfInspection.Plan): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(path, plan, Options.default)

  def inspect(
    path: Path,
    plan: PdfInspection.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    PdfInspection.run(elementsStream(path, opts), plan)

  /** Run a plan over an in-memory byte source without materializing elements. */
  def inspect(bytes: Chunk[Byte], plan: PdfInspection.Plan): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(bytes, plan, Options.default)

  def inspect(
    bytes: Chunk[Byte],
    plan: PdfInspection.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    PdfInspection.run(ZStream.fromChunk(bytes).via(elements(opts)), plan)

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

  // --- encode / merge / append / linearize -----------------------------------

  /** Encode a part stream to PDF bytes (symmetric with [[decode]]). */
  def write(parts: ZStream[Any, Throwable, Part[Trailer]]): ZStream[Any, Throwable, Byte] =
    parts.via(WritePdf.parts).mapConcatChunk(chunk => Chunk.fromArray(chunk.toArray))

  def write(parts: Chunk[Part[Trailer]]): ZStream[Any, Throwable, Byte] =
    write(ZStream.fromChunk(parts))

  def writeBytes(parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    write(parts).runCollect

  /** Merge filings in path order into one PDF. */
  def merge(
    paths: NonEmptyChunk[java.nio.file.Path],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    paths.toList match {
      case head :: tail =>
        ZIO
          .foreach(head :: tail)(path => decode(path, opts))
          .tap(chunks => ZIO.foreachDiscard(chunks)(decoded => ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded))))
          .flatMap(chunks => PdfMerge.bytes(NonEmptyChunk(chunks.head, chunks.tail*)))
      case Nil =>
        ZIO.fail(new IllegalArgumentException("merge requires at least one path"))
    }

  /** Merge caller-owned PDF bytes in source order. */
  def mergeBytes(
    sources: NonEmptyChunk[Chunk[Byte]],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfMerge.fromBytes(sources, opts)

  /** Append a sign/append revision after an existing PDF prefix. */
  def appendRevision(base: Chunk[Byte], revision: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfAppend.append(base, revision)

  /** Produce a linearized PDF tuned for fast first-page web display. */
  def linearize(trailerData: Prim.Dict, parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfLinearize.bytes(trailerData, parts)

  /** Linearize an existing PDF while preserving top-level object bytes. */
  def linearize(bytes: Chunk[Byte], opts: Options = Options.default): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfLinearize.fromBytes(bytes, opts)

  /** Add placeholder `/Thumb` image XObjects to page parts (no renderer). */
  def withThumbnails(
    parts: Chunk[Part[Trailer]],
    thumbStartNumber: Long,
    options: PdfThumbnail.Options = PdfThumbnail.Options()
  ): ZIO[Any, Throwable, Chunk[Part[Trailer]]] =
    ZIO.fromEither(PdfThumbnail.enrichParts(parts, thumbStartNumber, options).left.map(new RuntimeException(_)))

  /** Add `/Thumb` to an existing PDF (incremental for first page on large docs). */
  def withThumbnailsBytes(bytes: Chunk[Byte], options: PdfThumbnail.Options = PdfThumbnail.Options()): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfThumbnail.enrichBytes(bytes, options)

  /** Graft byte-identical objects from a donor PDF into preencoded parts. */
  def graftObjects(donor: Chunk[Byte], objectNumbers: Set[Long]): ZIO[Any, Throwable, Chunk[Part.Preencoded]] =
    PdfGraft.graft(donor, objectNumbers)

  /** Flatten AcroForm widgets, baking `/AP` appearances into page content. */
  def flattenForms(bytes: Chunk[Byte], opts: Options = Options.default): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decode(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded)) *> PdfAcroForm.flatten(decoded)
    }

  /** Extract a 1-based inclusive page range into a new PDF. */
  def extractPages(
    bytes: Chunk[Byte],
    fromPage: Int,
    toPage: Int,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfSplit.extractBytes(bytes, fromPage, toPage, opts)

  /** Split every page into its own PDF, preserving page order. */
  def splitPages(
    bytes: Chunk[Byte],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, NonEmptyChunk[Chunk[Byte]]] =
    PdfSplit.splitBytes(bytes, opts)

  /** Add a multiple-of-90 `/Rotate` to a 1-based inclusive page range. */
  def rotatePages(
    bytes: Chunk[Byte],
    degrees: Int,
    fromPage: Int,
    toPage: Int,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfSplit.rotateBytes(bytes, degrees, fromPage, toPage, opts)
