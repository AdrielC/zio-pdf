package zio.pdf

import zio.*
import zio.prelude.Validation
import zio.stream.{ZPipeline, ZSink, ZStream}

/**
 * Scala.js PDF façade.
 *
 * The composable decoding and policy surface is identical to the JVM engine.
 * Use [[PdfSource]] for browser and Node byte sources; `Path` and `PdfIO`
 * remain JVM-only source adapters.
 */
trait PdfEngine:

  def decoded(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, Decoded]
  def elements(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, Element]
  def streaming(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, StreamingDecoded]
  def extractTextPipeline(opts: PdfEngine.Options): ZPipeline[Any, Throwable, Byte, PageText]
  /** A fresh sink for each run: browser SHA-256 state is mutable and finalized by digest(). */
  def digestSink: ZSink[Any, Throwable, Byte, Nothing, Chunk[Byte]]

  def decode(source: PdfSource, opts: PdfEngine.Options): Task[Chunk[Decoded]]
  def elements(source: PdfSource, opts: PdfEngine.Options): Task[Chunk[Element]]
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

  final case class InputTooLarge(maxInputBytes: Long, observedBytes: Long)
      extends RuntimeException(
        s"PDF input is $observedBytes bytes, above the configured $maxInputBytes-byte limit"
      )

  final case class MaterializedDocumentLimitExceeded(maxBytes: ByteLimit, observedBytes: Long)
      extends RuntimeException(
        s"PDF input is $observedBytes bytes, above the configured ${maxBytes.bytes}-byte materialized-document limit"
      )

  object Options:
    val default: Options = Options()

  val default: Options = Options.default
  val live: ULayer[PdfEngine] = ZLayer.succeed(Live)

  /** Bound any API whose result retains the complete decoded document. */
  private[pdf] def materializedInputLimit(opts: Options): ZPipeline[Any, Throwable, Byte, Byte] =
    ZPipeline.fromFunction[Any, Throwable, Byte, Byte] { input =>
      var seen = 0L
      input.mapChunksZIO { chunk =>
        val observed = seen + chunk.size.toLong
        if observed > opts.maxMaterializedDocumentBytes.toLong then
          ZIO.fail(MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, observed))
        else
          seen = observed
          ZIO.succeed(chunk)
      }
    }

  private object Live extends PdfEngine:

    private def inputLimit(opts: Options): ZPipeline[Any, Throwable, Byte, Byte] =
      ZPipeline.fromFunction[Any, Throwable, Byte, Byte] { input =>
        var seen = 0L
        input.mapChunksZIO { chunk =>
          val observed = seen + chunk.size.toLong
          if observed > opts.maxInputBytes then ZIO.fail(InputTooLarge(opts.maxInputBytes, observed))
          else
            seen = observed
            ZIO.succeed(chunk)
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

    def digestSink: ZSink[Any, Throwable, Byte, Nothing, Chunk[Byte]] =
      val initial: NobleSha256State = NobleSha256.create()
      ZSink
        .foldLeftChunks(initial) { (digest: NobleSha256State, chunk: Chunk[Byte]) =>
          DigestChunk.update(digest, chunk)
          digest
        }
        .map(digest => Chunk.fromArray(JsBinary.bytes(digest.digest())))

    def decode(source: PdfSource, opts: Options): Task[Chunk[Decoded]] =
      source.bytes.via(materializedInputLimit(opts)).via(decoded(opts)).runCollect

    def elements(source: PdfSource, opts: Options): Task[Chunk[Element]] =
      source.bytes.via(materializedInputLimit(opts)).via(elements(opts)).runCollect

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

  def decoded(): ZPipeline[PdfEngine, Throwable, Byte, Decoded] =
    decoded(Options.default)

  def decoded(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, Decoded] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.decoded(opts))

  /** Decode a caller-owned byte source without introducing another decoder API. */
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

  def elements[R](source: ZStream[R, Throwable, Byte]): ZStream[R & PdfEngine, Throwable, Element] =
    elements(source, Options.default)

  def elements[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZStream[R & PdfEngine, Throwable, Element] =
    source.via(elements(opts))

  /** Run a composable typed inspection plan while elements are decoded. */
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

  /**
   * Decode a caller-owned byte stream once and merge all requested evidence
   * observers over that decoded event stream.
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

  def streaming(): ZPipeline[PdfEngine, Throwable, Byte, StreamingDecoded] =
    streaming(Options.default)

  def streaming(opts: Options): ZPipeline[PdfEngine, Throwable, Byte, StreamingDecoded] =
    ZPipeline.serviceWithPipeline[PdfEngine](_.streaming(opts))

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

  def extractText[R](source: ZStream[R, Throwable, Byte]): ZStream[R & PdfEngine, Throwable, PageText] =
    extractText(source, Options.default)

  def extractText[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZStream[R & PdfEngine, Throwable, PageText] =
    source.via(extractTextPipeline(opts))

  /** Extract literal text from a reusable browser or Node source. */
  def extractText(
    source: PdfSource,
    opts: Options = Options.default
  ): ZStream[PdfEngine, Throwable, PageText] =
    extractText(source.bytes, opts)

  def digestSink: ZSink[PdfEngine, Throwable, Byte, Nothing, Chunk[Byte]] =
    ZSink.serviceWithSink[PdfEngine](_.digestSink)

  def decode(source: PdfSource, opts: Options = Options.default): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    ZIO.serviceWithZIO[PdfEngine](_.decode(source, opts))

  def decode(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    decode(bytes, Options.default)

  def decode(bytes: Chunk[Byte], opts: Options): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    decode(PdfSource.fromChunk(bytes), opts)

  def elements(source: PdfSource, opts: Options = Options.default): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    ZIO.serviceWithZIO[PdfEngine](_.elements(source, opts))

  def elements(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    elements(bytes, Options.default)

  def elements(bytes: Chunk[Byte], opts: Options): ZIO[PdfEngine, Throwable, Chunk[Element]] =
    elements(PdfSource.fromChunk(bytes), opts)

  /** Browser / Node source adapter for the same shared inspection plans. */
  def inspect(source: PdfSource, plan: PdfInspection.Plan): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(source, plan, Options.default)

  def inspect(
    source: PdfSource,
    plan: PdfInspection.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(source.bytes, plan, opts)

  def inspect(bytes: Chunk[Byte], plan: PdfInspection.Plan): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(bytes, plan, Options.default)

  def inspect(
    bytes: Chunk[Byte],
    plan: PdfInspection.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfInspection.Outcome] =
    inspect(PdfSource.fromChunk(bytes), plan, opts)

  /** One browser / Node source read for a complete evidence bundle. */
  def evidence(source: PdfSource): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(source.bytes)

  def evidence(source: PdfSource, plan: PdfEvidence.Plan): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(source.bytes, plan)

  def evidence(
    source: PdfSource,
    plan: PdfEvidence.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(source.bytes, plan, opts)

  def evidence(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(PdfSource.fromChunk(bytes))

  def evidence(bytes: Chunk[Byte], plan: PdfEvidence.Plan): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(PdfSource.fromChunk(bytes), plan)

  def evidence(
    bytes: Chunk[Byte],
    plan: PdfEvidence.Plan,
    opts: Options
  ): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    evidence(PdfSource.fromChunk(bytes), plan, opts)

  /** Validate a reusable browser or Node source without exposing its byte stream. */
  def validate(
    source: PdfSource,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PdfError, Unit]] =
    validate(source.bytes, opts)

  /** Apply a policy to a reusable browser or Node source. */
  def policy(
    source: PdfSource,
    rules: Policy,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[PolicyViolation, Unit]] =
    policy(source.bytes, rules, opts)

  /** Hash a reusable browser or Node source without buffering it. */
  def digest(source: PdfSource): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    digest(source.bytes)

  /** Compare reusable browser or Node sources with the shared structural comparator. */
  def compare(
    old: PdfSource,
    updated: PdfSource,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Validation[CompareError, Unit]] =
    compare(old.bytes, updated.bytes, opts)

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

  def validate[R](source: ZStream[R, Throwable, Byte]): ZIO[R & PdfEngine, Throwable, Validation[PdfError, Unit]] =
    validate(source, Options.default)

  def validate[R](
    source: ZStream[R, Throwable, Byte],
    opts: Options
  ): ZIO[R & PdfEngine, Throwable, Validation[PdfError, Unit]] =
    ValidatePdf.fromDecoded(decode(source, opts))

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

  def digest[R](source: ZStream[R, Throwable, Byte]): ZIO[R & PdfEngine, Throwable, Chunk[Byte]] =
    source.run(digestSink)

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

  def write(parts: ZStream[Any, Throwable, Part[Trailer]]): ZStream[Any, Throwable, Byte] =
    parts.via(WritePdf.parts).mapConcatChunk(chunk => Chunk.fromArray(chunk.toArray))

  def write(parts: Chunk[Part[Trailer]]): ZStream[Any, Throwable, Byte] =
    write(ZStream.fromChunk(parts))

  def writeBytes(parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    write(parts).runCollect

  def mergeBytes(
    sources: NonEmptyChunk[Chunk[Byte]],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfMerge.fromBytes(sources, opts)

  def appendRevision(base: Chunk[Byte], revision: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfAppend.append(base, revision)

  def linearize(trailerData: Prim.Dict, parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfLinearize.bytes(trailerData, parts)

  def linearize(bytes: Chunk[Byte], opts: Options = Options.default): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfLinearize.fromBytes(bytes, opts)

  def withThumbnailsBytes(
    bytes: Chunk[Byte],
    options: PdfThumbnail.Options = PdfThumbnail.Options()
  ): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfThumbnail.enrichBytes(bytes, options)

  def graftObjects(donor: Chunk[Byte], objectNumbers: Set[Long]): ZIO[Any, Throwable, Chunk[Part.Preencoded]] =
    PdfGraft.graft(donor, objectNumbers)

  /** Flatten AcroForm widgets, baking `/AP` appearances into page content. */
  def flattenForms(bytes: Chunk[Byte], opts: Options = Options.default): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decode(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded)) *> PdfAcroForm.flatten(decoded)
    }

  def extractPages(
    bytes: Chunk[Byte],
    fromPage: Int,
    toPage: Int,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfSplit.extractBytes(bytes, fromPage, toPage, opts)

  def splitPages(
    bytes: Chunk[Byte],
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, NonEmptyChunk[Chunk[Byte]]] =
    PdfSplit.splitBytes(bytes, opts)

  def rotatePages(
    bytes: Chunk[Byte],
    degrees: Int,
    fromPage: Int,
    toPage: Int,
    opts: Options = Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfSplit.rotateBytes(bytes, degrees, fromPage, toPage, opts)
