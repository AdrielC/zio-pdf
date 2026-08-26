/*
 * One incremental decode kernel shared by caller-owned ZStreams and JVM path
 * readers. ZChannel owns the ordered cursor as its recursive result, while
 * synchronous paths thread the same value directly.
 */

package zio.pdf

import zio.{Cause, Chunk, ZIO}
import zio.prelude.Identity
import zio.prelude.fx.ZPure
import zio.stream.{ZChannel, ZPipeline}

/** Ordered state transitions plus the fused [[ZPipeline]] interpreter. */
private[pdf] object FusedDecoder {

  final case class State(
    parser: StreamingDecode.FinalState,
    expansion: DecodedFromStreaming.Acc,
    finished: Boolean
  )

  /** One pure state transition: output is the program result, not a side log. */
  type Program = ZPure[Nothing, State, State, Any, Throwable, Chunk[Decoded]]

  private[pdf] final case class Result(next: State, emitted: Chunk[Decoded])

  /**
   * A deep, in-process snapshot at the next unread byte offset. Checkpoints
   * retain partial tokens and stream buffers, so resume does not need to seek
   * backward to an object boundary.
   */
  final case class Checkpoint private[pdf] (
    nextByteOffset: Long,
    config: StreamingDecode.Config,
    private[pdf] state: State
  )

  /**
   * A configuration-scoped monoid of ordered byte work. Its path-dependent
   * `Segment` type prevents mixing plans compiled with different parser rules.
   */
  final class Plan private[pdf] (val config: StreamingDecode.Config) {

    final case class Segment private[pdf] (byteCount: Long, private[pdf] program: Program) {
      infix def ++(that: Segment): Segment =
        new Segment(
          Math.addExact(byteCount, that.byteCount),
          program.flatMap(left => that.program.map(right => left ++ right))
        )
    }

    val empty: Segment = new Segment(0L, ZPure.succeed(Chunk.empty))

    given Identity[Segment] = Identity.make(empty, _ ++ _)

    def fromChunk(chunk: Chunk[Byte]): Segment =
      new Segment(chunk.size.toLong, feed(chunk, config))

    /** Advance a checkpoint through this plan's own parse configuration. */
    def advance(
      checkpoint: Checkpoint,
      segment: Segment
    ): Either[Throwable, (Checkpoint, Chunk[Decoded])] =
      restore(checkpoint, config).flatMap { state =>
        run(state, segment.program).map { result =>
          val nextByteOffset = Math.addExact(checkpoint.nextByteOffset, segment.byteCount)
          (FusedDecoder.checkpoint(result.next, nextByteOffset, config), result.emitted)
        }
      }
  }

  def plan(config: StreamingDecode.Config): Plan = new Plan(config)

  /** Every run needs a fresh duplicate filter and stream-expansion accumulator. */
  def initial: State =
    State(StreamingDecode.initialFinalState, DecodedFromStreaming.accInitial, finished = false)

  /** Snapshot state at `nextByteOffset`; later decode work cannot mutate it. */
  def checkpoint(
    state: State,
    nextByteOffset: Long,
    config: StreamingDecode.Config
  ): Checkpoint = {
    require(nextByteOffset >= 0L, "nextByteOffset must be non-negative")
    new Checkpoint(nextByteOffset, config, snapshot(state))
  }

  /** Restore an independent cursor. A changed streaming config would be unsound. */
  def restore(
    checkpoint: Checkpoint,
    config: StreamingDecode.Config
  ): Either[IllegalArgumentException, State] =
    if checkpoint.config != config then
      Left(new IllegalArgumentException("decoder checkpoint requires the original StreamingDecode.Config"))
    else Right(snapshot(checkpoint.state))

  /**
   * Small input chunks are fine, but parsing at a one-byte window makes large
   * indirect-object carries pathological. The decoder therefore owns a sane
   * lower bound for every file reader and ZStream rechunk operation.
   */
  val MinimumChunkSize = 64 * 1024
  val DefaultChunkSize = 10 * 1024 * 1024

  def normalizedChunkSize(requested: Int): Int = {
    require(requested > 0, "chunkSize must be positive")
    math.max(MinimumChunkSize, requested)
  }

  /** Feed one already-materialised upstream chunk. */
  def feed(chunk: Chunk[Byte], config: StreamingDecode.Config): Program =
    transition { state =>
      if state.finished then throw new IllegalStateException("fused decoder is already complete")
      else if chunk.isEmpty then Result(state, Chunk.empty)
      else {
        val (events, nextParser) = StreamingDecode.stepChunk(config, state.parser, chunk)
        val (decoded, nextExpansion) =
          DecodedFromStreaming.foldSync(state.expansion, events, config.maxMaterializedStreamBytes)
        Result(state.copy(parser = nextParser, expansion = nextExpansion), decoded)
      }
    }

  /** The caller owns `bytes` and must not mutate it while this program runs. */
  def feedBytes(
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    config: StreamingDecode.Config
  ): Program =
    transition { state =>
      if state.finished then throw new IllegalStateException("fused decoder is already complete")
      else if length == 0 then Result(state, Chunk.empty)
      else {
        val (events, nextParser) = StreamingDecode.stepChunkBytes(config, state.parser, bytes, offset, length)
        val (decoded, nextExpansion) =
          DecodedFromStreaming.foldSync(state.expansion, events, config.maxMaterializedStreamBytes)
        Result(state.copy(parser = nextParser, expansion = nextExpansion), decoded)
      }
    }

  /** Emit metadata and validate that no content-stream expansion is incomplete. */
  def finish(
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): Program =
    transition { state =>
      if state.finished then throw new IllegalStateException("fused decoder is already complete")
      else {
        StreamingDecode.validateFinalState(state.parser).fold(throw _, identity)
        val meta = StreamingDecode.finalizeToMetaSync(enableDiagnostics, state.parser)
        val (decoded, nextExpansion) =
          DecodedFromStreaming.foldSync(state.expansion, meta, config.maxMaterializedStreamBytes)
        val tail = DecodedFromStreaming.finalizeSync(nextExpansion)
        Result(state.copy(expansion = nextExpansion, finished = true), decoded ++ tail)
      }
    }

  /** Interpret one ZPure transition against its caller-owned cursor. */
  private[pdf] def run(state: State, program: Program): Either[Throwable, Result] = {
    val (_, result) = program.runAll(state)
    result.map { case (next, emitted) => Result(next, emitted) }
  }

  /**
   * Rechunk before parsing: fragmented sources otherwise repeatedly revisit
   * parser carry for large indirect-object headers and stream payloads.
   */
  def decodePipeline(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    chunkSize: Int = DefaultChunkSize
  ): ZPipeline[Any, Throwable, Byte, Decoded] = {
    val effectiveChunkSize = normalizedChunkSize(chunkSize)
    ZPipeline
      .rechunk[Byte](effectiveChunkSize)
      .andThen(ZPipeline.fromChannel(loop(config, initial).flatMap(emitFinal(enableDiagnostics, config, _))))
  }

  def elementsPipeline(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    chunkSize: Int = DefaultChunkSize
  ): ZPipeline[Any, Throwable, Byte, Element] =
    decodePipeline(enableDiagnostics, config, chunkSize) >>> Elements.pipe

  private def loop(
    config: StreamingDecode.Config,
    state: State
  ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Decoded], State] =
    ZChannel.readWithCause[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[Decoded], State](
      chunk =>
        run(state, feed(chunk, config)) match {
          case Left(error) => ZChannel.fail(error)
          case Right(Result(next, emitted)) =>
            if emitted.isEmpty then loop(config, next)
            else ZChannel.write(emitted) *> loop(config, next)
        },
      (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
      (_: Any) => ZChannel.succeed(state)
    )

  private def emitFinal(
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config,
    state: State
  ): ZChannel[Any, Any, Any, Any, Throwable, Chunk[Decoded], Unit] =
    ZChannel
      .fromZIO(
        ZIO.fromEither(run(state, finish(enableDiagnostics, config))).map { result => ZChannel.write(result.emitted) }
      )
      .flatten

  private def transition(f: State => Result): Program =
    ZPure
      .modify[State, State, Either[Throwable, Chunk[Decoded]]] { state =>
        try {
          val result = f(state)
          (Right(result.emitted), result.next)
        } catch {
          case error: Throwable => (Left(error), state)
        }
      }
      .flatMap(_.fold(ZPure.fail, ZPure.succeed))

  private def snapshot(state: State): State =
    state.copy(
      parser = StreamingDecode.snapshotFinalState(state.parser),
      expansion = DecodedFromStreaming.snapshot(state.expansion)
    )
}
