/*
 * Memory-bounded streaming decoder:
 *   ZPipeline[Any, Throwable, Byte, StreamingDecoded]
 *
 * Duplicate indirect objects before the first xref are suppressed
 * (same rules as [[FilterDuplicates]]), using a fixed-size bit table
 * ([[DuplicateFilterState]]) instead of an unbounded set. End-of-stream
 * logging reports a suppression count only.
 *
 * Small content streams (length <= config.inlineMaxBytes) are
 * buffered once and emitted as [[StreamingDecoded.ContentObjStart]]
 * with `inlinePayload = Some(...)`. Larger streams use chunked
 * `ContentObjBytes` + `ContentObjEnd`.
 */

package zio.pdf

import _root_.scodec.{Attempt, DecodeResult, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.{Cause, Chunk, NonEmptyChunk, ZIO}
import zio.stream.{ZChannel, ZPipeline}

object StreamingDecode {

  private val MaxInputWindowBytes = 64 * 1024

  /** @param inlineMaxBytes
    *   Raw stream payloads of this size or smaller are materialised
    *   once on `ContentObjStart.inlinePayload`; larger streams chunk.
    */
  final case class Config(
    inlineMaxBytes: Long,
    emitObjectEnds: Boolean = false,
    maxCarryBytes: Option[Int] = None,
    emitContentEvents: Boolean = true,
    maxMaterializedStreamBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ) {
    require(inlineMaxBytes >= 0L, "inlineMaxBytes must be non-negative")
    require(maxCarryBytes.forall(_ > 0), "maxCarryBytes must be positive when defined")
    require(
      inlineMaxBytes <= maxMaterializedStreamBytes.toLong,
      "inlineMaxBytes must not exceed maxMaterializedStreamBytes"
    )
  }

  object Config {
    val default: Config = Config(inlineMaxBytes = 256 * 1024L)
  }

  final case class CarryLimitExceeded(maxBytes: Int, observedBytes: Long)
      extends RuntimeException(s"PDF parser carry exceeded configured limit of $maxBytes bytes (observed $observedBytes)")

  final case class UnresolvedIndirectStreamLength(index: Obj.Index, reference: Prim.Ref)
      extends RuntimeException(
        s"stream object ${index.number} has indirect /Length ${reference.number} ${reference.generation} R; resolve it through xref random access before boundary scanning"
      )

  final case class InvalidDeclaredStreamLength(index: Obj.Index, detail: String)
      extends RuntimeException(s"stream object ${index.number} has no usable direct /Length: $detail")

  final case class UnexpectedEndOfInput(context: String, remainingBytes: Long)
      extends RuntimeException(s"unexpected end of PDF input while $context ($remainingBytes bytes remain)")

  private type HeaderEvent = PdfByteLexer.HeaderEvent
  import PdfByteLexer.HeaderEvent

  private val streamTrailer: _root_.scodec.Codec[Unit] = IndirectObj.streamTrailer

  private[pdf] sealed trait State
  private[pdf] final case class WaitingHeader(carry: BitVector) extends State
  private[pdf] final case class ForwardingBytes(index: Obj.Index, remaining: Long, carry: BitVector) extends State
  private[pdf] final case class BufferingBytes(
    obj: Obj,
    bytesTotal: Int,
    filled: Int,
    carry: BitVector,
    acc: Array[Byte]
  ) extends State
  private[pdf] final case class SkippingStreamPayload(index: Obj.Index, remaining: Long, carry: BitVector) extends State
  private[pdf] final case class ConsumingTrailer(index: Obj.Index, carry: BitVector) extends State

  private inline def boundaryOnly(cfg: Config): Boolean =
    cfg.emitObjectEnds && !cfg.emitContentEvents

  private def headerToEvent(
    cfg: Config,
    event: HeaderEvent,
    remainingBits: BitVector,
    dup: DuplicateFilterState.Mutable
  ): (Chunk[StreamingDecoded], State) = event match {
    case HeaderEvent.V(v) =>
      if boundaryOnly(cfg) then (Chunk.empty, WaitingHeader(remainingBits))
      else (Chunk.single(StreamingDecoded.VersionT(v)), WaitingHeader(remainingBits))
    case HeaderEvent.C(b) =>
      if boundaryOnly(cfg) then (Chunk.empty, WaitingHeader(remainingBits))
      else (Chunk.single(StreamingDecoded.CommentT(b)), WaitingHeader(remainingBits))
    case HeaderEvent.S(s) =>
      DuplicateFilterState.enterUpdateMode(dup)
      if boundaryOnly(cfg) then (Chunk.empty, WaitingHeader(remainingBits))
      else (Chunk.single(StreamingDecoded.StartXrefT(s)), WaitingHeader(remainingBits))
    case HeaderEvent.X(x) =>
      DuplicateFilterState.enterUpdateMode(dup)
      if boundaryOnly(cfg) then (Chunk.empty, WaitingHeader(remainingBits))
      else (Chunk.single(StreamingDecoded.XrefT(x)), WaitingHeader(remainingBits))
    case HeaderEvent.W(_) =>
      (Chunk.empty, WaitingHeader(remainingBits))
    case HeaderEvent.ObjHead(index, streamLen) =>
      enterObject(cfg, index, streamLen, None, remainingBits, dup)
    case HeaderEvent.H(IndirectObj.IndirectObjHeader(obj, streamLen)) =>
      enterObject(cfg, obj.index, streamLen, Some(obj), remainingBits, dup)
  }

  private def enterObject(
    cfg: Config,
    index: Obj.Index,
    streamLen: Option[Long],
    obj: Option[Obj],
    remainingBits: BitVector,
    dup: DuplicateFilterState.Mutable
  ): (Chunk[StreamingDecoded], State) = {
    val suppress = DuplicateFilterState.shouldSuppress(dup, index.number)
    if (suppress)
      streamLen match {
        case None         => (Chunk.empty, ConsumingTrailerNoStream(index, remainingBits))
        case Some(length) => (Chunk.empty, SkippingStreamPayload(index, length, remainingBits))
      }
    else
      streamLen match {
        case None =>
          val events =
            if boundaryOnly(cfg) then Chunk.empty
            else obj match {
              case Some(o) => Chunk.single(StreamingDecoded.DataObj(o))
              case None    => Chunk.empty
            }
          (events, ConsumingTrailerNoStream(index, remainingBits))
        case Some(length) =>
          if !cfg.emitContentEvents then
            if length == 0L then (Chunk.empty, ConsumingTrailer(index, remainingBits))
            else (Chunk.empty, ForwardingBytes(index, length, remainingBits))
          else
            obj match {
              case Some(o) if length <= cfg.inlineMaxBytes && length <= Int.MaxValue && length > 0L =>
                (
                  Chunk.empty,
                  BufferingBytes(
                    o,
                    bytesTotal = length.toInt,
                    filled     = 0,
                    carry      = remainingBits,
                    acc        = new Array[Byte](length.toInt)
                  )
                )
              case Some(o) if length == 0L =>
                (
                  Chunk.single(StreamingDecoded.ContentObjStart(o, 0L, Some(BitVector.empty))),
                  ConsumingTrailer(index, remainingBits)
                )
              case Some(o) =>
                (
                  Chunk.single(StreamingDecoded.ContentObjStart(o, length, None)),
                  ForwardingBytes(index, length, remainingBits)
                )
              case None if length == 0L =>
                (Chunk.empty, ConsumingTrailer(index, remainingBits))
              case None =>
                (Chunk.empty, ForwardingBytes(index, length, remainingBits))
            }
      }
  }

  private[pdf] final case class ConsumingTrailerNoStream(index: Obj.Index, carry: BitVector) extends State

  private val endobjTrailer: _root_.scodec.Codec[Unit] = IndirectObj.endobj

  private def tryConsumeTrailer(
    state: ConsumingTrailer | ConsumingTrailerNoStream,
    carry: BitVector
  ): Either[BitVector, Either[Throwable, BitVector]] = state match {
    case ConsumingTrailer(_, _) =>
      streamTrailer.decode(carry) match {
        case Attempt.Successful(DecodeResult(_, rest))      => Right(Right(rest))
        case Attempt.Failure(_: Err.InsufficientBits)        => Left(carry)
        case Attempt.Failure(comp: Err.Composite)
            if comp.errs.exists(_.isInstanceOf[Err.InsufficientBits]) => Left(carry)
        case Attempt.Failure(other) =>
          Right(Left(new RuntimeException(s"stream trailer: ${other.messageWithContext}")))
      }
    case ConsumingTrailerNoStream(_, _) =>
      endobjTrailer.decode(carry) match {
        case Attempt.Successful(DecodeResult(_, rest))      => Right(Right(rest))
        case Attempt.Failure(_: Err.InsufficientBits)        => Left(carry)
        case Attempt.Failure(comp: Err.Composite)
            if comp.errs.exists(_.isInstanceOf[Err.InsufficientBits]) => Left(carry)
        case Attempt.Failure(other) =>
          Right(Left(new RuntimeException(s"endobj trailer: ${other.messageWithContext}")))
      }
  }

  private def stepAll(
    cfg: Config,
    state: State,
    dup: DuplicateFilterState.Mutable,
    bytesSeen: Long,
    in: Chunk[StreamingDecoded] = Chunk.empty
  ): (Chunk[StreamingDecoded], State) = state match {

    case fb @ ForwardingBytes(index, remaining, carry) =>
      if (remaining == 0L)
        val nextEvents = if cfg.emitContentEvents then in :+ StreamingDecoded.ContentObjEnd else in
        stepAll(cfg, ConsumingTrailer(index, carry), dup, bytesSeen, nextEvents)
      else if (carry.isEmpty)
        (in, fb)
      else {
        val carryBytes = carry.bytes
        val take       = math.min(remaining, carryBytes.size).toInt
        val rest       = carry.drop(take.toLong * 8)
        val nextEvents =
          if cfg.emitContentEvents then
            val emitArr = new Array[Byte](take)
            carryBytes.copyToArray(emitArr, 0, 0L, take)
            in :+ StreamingDecoded.ContentObjBytes(Chunk.fromArray(emitArr))
          else in
        stepAll(
          cfg,
          ForwardingBytes(index, remaining - take.toLong, rest),
          dup,
          bytesSeen,
          nextEvents
        )
      }

    case buf @ BufferingBytes(obj, bytesTotal, filled, carry, acc) =>
      if (filled == bytesTotal) {
        val ev =
          StreamingDecoded.ContentObjStart(obj, bytesTotal.toLong, Some(BitVector(acc)))
        stepAll(cfg, ConsumingTrailer(obj.index, carry), dup, bytesSeen, in :+ ev)
      }
      else if (carry.isEmpty)
        (in, buf)
      else {
        val carryBytes = carry.bytes
        val need       = bytesTotal - filled
        val take       = math.min(need, carryBytes.size).toInt
        var i          = 0
        while (i < take) {
          acc(filled + i) = carryBytes(i)
          i += 1
        }
        val rest = carry.drop(take.toLong * 8)
        stepAll(cfg, BufferingBytes(obj, bytesTotal, filled + take, rest, acc), dup, bytesSeen, in)
      }

    case sb @ SkippingStreamPayload(index, remaining, carry) =>
      if (remaining == 0L)
        stepAll(cfg, ConsumingTrailer(index, carry), dup, bytesSeen, in)
      else if (carry.isEmpty)
        (in, sb)
      else {
        val carryBytes = carry.bytes
        val take       = math.min(remaining, carryBytes.size).toInt
        val rest       = carry.drop(take.toLong * 8)
        stepAll(cfg, SkippingStreamPayload(index, remaining - take.toLong, rest), dup, bytesSeen, in)
      }

    case ct: (ConsumingTrailer | ConsumingTrailerNoStream) =>
      val (index, carry) = ct match {
        case ConsumingTrailer(i, c)         => (i, c)
        case ConsumingTrailerNoStream(i, c) => (i, c)
      }
      tryConsumeTrailer(ct, carry) match {
        case Left(needMore) =>
          (in, ct match {
            case _: ConsumingTrailer         => ConsumingTrailer(index, needMore)
            case _: ConsumingTrailerNoStream => ConsumingTrailerNoStream(index, needMore)
          })
        case Right(Right(rest)) =>
          val withBoundary =
            if cfg.emitObjectEnds then
              val nextByteOffset = Math.subtractExact(bytesSeen, rest.bytes.size)
              in :+ StreamingDecoded.ObjectEnd(index, nextByteOffset)
            else in
          stepAll(cfg, WaitingHeader(rest), dup, bytesSeen, withBoundary)
        case Right(Left(err))   => throw err
      }

    case wh @ WaitingHeader(carry) =>
      PdfByteLexer.next(
        carry.bytes,
        allowRawDictionaryStream = cfg.emitObjectEnds && !cfg.emitContentEvents
      ) match {
        case PdfByteLexer.LexResult.NeedMore =>
          (in, wh)
        case PdfByteLexer.LexResult.Failed(error) =>
          throw error
        case PdfByteLexer.LexResult.Ok(event, rest) =>
          val restBits = if (rest.isEmpty) BitVector.empty else rest.bits
          val (events, next) = headerToEvent(cfg, event, restBits, dup)
          stepAll(cfg, next, dup, bytesSeen, in ++ events)
      }
  }

  private def feedBytes(
    cfg: Config,
    state: State,
    dup: DuplicateFilterState.Mutable,
    buf: Array[Byte],
    offset: Int,
    length: Int,
    bytesSeen: Long
  ): (Chunk[StreamingDecoded], State) = {
    val incoming =
      if (offset == 0) BitVector.view(buf, length.toLong * 8L)
      else BitVector(ByteVector.view(buf, offset, length))
    def appendCarry(c: BitVector): BitVector =
      if (c.isEmpty) incoming else c ++ incoming
    val newCarry = state match {
      case WaitingHeader(c)              => appendCarry(c)
      case ForwardingBytes(_, _, c)      => appendCarry(c)
      case BufferingBytes(_, _, _, c, _) => appendCarry(c)
      case SkippingStreamPayload(_, _, c) => appendCarry(c)
      case ConsumingTrailer(_, c)         => appendCarry(c)
      case ConsumingTrailerNoStream(_, c) => appendCarry(c)
    }
    val withCarry: State = state match {
      case WaitingHeader(c)            => WaitingHeader(newCarry)
      case ForwardingBytes(i, r, _)    => ForwardingBytes(i, r, newCarry)
      case BufferingBytes(o, t, f, _, a) => BufferingBytes(o, t, f, newCarry, a)
      case SkippingStreamPayload(i, r, _) => SkippingStreamPayload(i, r, newCarry)
      case ConsumingTrailer(i, _)         => ConsumingTrailer(i, newCarry)
      case ConsumingTrailerNoStream(i, _) => ConsumingTrailerNoStream(i, newCarry)
    }
    val result = stepAll(cfg, withCarry, dup, bytesSeen)
    cfg.maxCarryBytes.foreach { max =>
      val carryBytes = stateCarry(result._2).bytes.size
      if carryBytes > max.toLong then throw CarryLimitExceeded(max, carryBytes)
    }
    val retainedState =
      if cfg.maxCarryBytes.isDefined then compactStateCarry(result._2)
      else result._2
    (result._1, retainedState)
  }

  private def stateCarry(state: State): BitVector = state match {
    case WaitingHeader(carry)                       => carry
    case ForwardingBytes(_, _, carry)               => carry
    case BufferingBytes(_, _, _, carry, _)          => carry
    case SkippingStreamPayload(_, _, carry)         => carry
    case ConsumingTrailer(_, carry)                 => carry
    case ConsumingTrailerNoStream(_, carry)         => carry
  }

  private def compactStateCarry(state: State): State = {
    val carry = stateCarry(state)
    if (carry.isEmpty) state
    else {
      val bytes = carry.bytes
      val owned = new Array[Byte](bytes.size.toInt)
      var index = 0
      while (index < owned.length) {
        owned(index) = bytes(index.toLong)
        index += 1
      }
      val compact = BitVector.view(owned)
      state match {
        case WaitingHeader(_)                         => WaitingHeader(compact)
        case ForwardingBytes(index, remaining, _)    => ForwardingBytes(index, remaining, compact)
        case BufferingBytes(obj, total, filled, _, a) => BufferingBytes(obj, total, filled, compact, a)
        case SkippingStreamPayload(index, remaining, _) => SkippingStreamPayload(index, remaining, compact)
        case ConsumingTrailer(index, _)               => ConsumingTrailer(index, compact)
        case ConsumingTrailerNoStream(index, _)       => ConsumingTrailerNoStream(index, compact)
      }
    }
  }

  /** Mutable parse state + xref/version accumulators (used by [[pipeline]] and sync drivers). */
  final case class FinalState(
    state: State,
    dupFilter: DuplicateFilterState.Mutable,
    xrefs: List[Xref],
    version: Option[Version],
    bytesSeen: Long
  )

  private def initial: FinalState =
    FinalState(WaitingHeader(BitVector.empty), DuplicateFilterState.initial, Nil, None, 0L)

  /** Starting state for a decode run (fresh duplicate filter, empty carry). */
  def initialFinalState: FinalState = initial

  /** Fail closed when the source ends inside a token, payload, or object trailer. */
  def validateFinalState(fs: FinalState): Either[UnexpectedEndOfInput, Unit] =
    fs.state match
      case WaitingHeader(carry) if carry.isEmpty => Right(())
      case WaitingHeader(carry) => Left(UnexpectedEndOfInput("reading a top-level token", carry.bytes.size))
      case ForwardingBytes(_, remaining, _) => Left(UnexpectedEndOfInput("forwarding a stream payload", remaining))
      case BufferingBytes(_, total, filled, _, _) =>
        Left(UnexpectedEndOfInput("buffering a stream payload", total.toLong - filled.toLong))
      case SkippingStreamPayload(_, remaining, _) =>
        Left(UnexpectedEndOfInput("skipping a stream payload", remaining))
      case ConsumingTrailer(_, carry) =>
        Left(UnexpectedEndOfInput("reading endstream/endobj", carry.bytes.size))
      case ConsumingTrailerNoStream(_, carry) =>
        Left(UnexpectedEndOfInput("reading endobj", carry.bytes.size))

  /** Bytes currently retained for incomplete structural parsing. */
  private[pdf] def structuralCarryBytes(fs: FinalState): Long =
    stateCarry(fs.state).bytes.size

  /**
   * Copy every mutable buffer in a parser cursor so a resumed decode cannot
   * mutate the checkpoint it started from.
   */
  private[pdf] def snapshotFinalState(fs: FinalState): FinalState =
    fs.copy(
      state = snapshotState(fs.state),
      dupFilter = DuplicateFilterState.snapshot(fs.dupFilter)
    )

  private def snapshotState(state: State): State = state match {
    case WaitingHeader(carry) => WaitingHeader(carry)
    case ForwardingBytes(index, remaining, carry) => ForwardingBytes(index, remaining, carry)
    case BufferingBytes(obj, bytesTotal, filled, carry, acc) =>
      BufferingBytes(obj, bytesTotal, filled, carry, acc.clone())
    case SkippingStreamPayload(index, remaining, carry) => SkippingStreamPayload(index, remaining, carry)
    case ConsumingTrailer(index, carry)                  => ConsumingTrailer(index, carry)
    case ConsumingTrailerNoStream(index, carry)          => ConsumingTrailerNoStream(index, carry)
  }

  /**
   * Synchronous byte step: feed one chunk through the streaming state machine
   * and refresh xref/version accumulators from emitted events.
   */
  def stepChunk(
    config: Config,
    fs: FinalState,
    chunk: Chunk[Byte]
  ): (Chunk[StreamingDecoded], FinalState) =
    chunk match {
      case Chunk.ByteArray(arr, off, len) =>
        stepChunkBytes(config, fs, arr, off, len)
      case _ =>
        var parser   = fs
        var offset   = 0
        val emitted  = Chunk.newBuilder[StreamingDecoded]
        while (offset < chunk.length) {
          val length = math.min(MaxInputWindowBytes, chunk.length - offset)
          val window = new Array[Byte](length)
          var index  = 0
          while (index < length) {
            window(index) = chunk(offset + index)
            index += 1
          }
          val (events, next) = stepChunkBytes(config, parser, window, 0, length)
          emitted ++= events
          parser = next
          offset += length
        }
        (emitted.result(), parser)
    }

  /** Zero-copy slice when the caller already owns a read buffer. */
  def stepChunkBytes(
    config: Config,
    fs: FinalState,
    buf: Array[Byte],
    offset: Int,
    length: Int
  ): (Chunk[StreamingDecoded], FinalState) = {
    val nextBytesSeen    = Math.addExact(fs.bytesSeen, length.toLong)
    val (out, nextState) = feedBytes(config, fs.state, fs.dupFilter, buf, offset, length, nextBytesSeen)
    val updatedBase      = fs.copy(state = nextState, bytesSeen = nextBytesSeen)
    val updated          =
      if boundaryOnly(config) || out.isEmpty then updatedBase
      else out.foldLeft(updatedBase)(updateAccumulators)
    (out, updated)
  }

  /**
   * After the last byte chunk, emit optional duplicate-debug log and the
   * trailing [[StreamingDecoded.Meta]] (same as [[pipeline]]'s channel).
   */
  def finalizeToMeta(enableDiagnostics: Boolean, fs: FinalState): ZIO[Any, Throwable, Chunk[StreamingDecoded]] =
    ZIO.fromEither(validateFinalState(fs)) *>
      ZPureLog.drainToZio(finalizeToMetaDiagnostics(enableDiagnostics, fs)) *>
      ZIO.succeed(finalizeToMetaChunk(fs))

  /** Trailing [[StreamingDecoded.Meta]] chunk (no diagnostics). */
  def finalizeToMetaChunk(fs: FinalState): Chunk[StreamingDecoded] = {
    val xs        = fs.xrefs.reverse
    val trailers  = xs.map(_.trailer)
    val sanitised = NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
    Chunk.single(StreamingDecoded.Meta(xs, sanitised, fs.version))
  }

  /** Diagnostic lines from `ZPure.log` when duplicate suppression ran. */
  def finalizeToMetaDiagnostics(enableDiagnostics: Boolean, fs: FinalState): Chunk[ZPureLogEntry] =
    if (enableDiagnostics && fs.dupFilter.duplicateCount > 0)
      ZPureLog.lines(
        s"duplicate indirect objects suppressed before first xref (count: ${fs.dupFilter.duplicateCount})"
      )
    else ZPureLog.empty

  /** Same as [[finalizeToMeta]] without ZIO (diagnostics go to stderr when enabled). */
  def finalizeToMetaSync(enableDiagnostics: Boolean, fs: FinalState): Chunk[StreamingDecoded] = {
    validateFinalState(fs).fold(throw _, identity)
    ZPureLog.drainSync(finalizeToMetaDiagnostics(enableDiagnostics, fs))
    finalizeToMetaChunk(fs)
  }

  private def updateAccumulators(fs: FinalState, ev: StreamingDecoded): FinalState = ev match {
    case StreamingDecoded.VersionT(v) => fs.copy(version = Some(v))
    case StreamingDecoded.XrefT(x)    => fs.copy(xrefs = x :: fs.xrefs)
    case _                            => fs
  }

  private def loop(
    cfg: Config,
    enableDiagnostics: Boolean,
    fs: FinalState
  ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[StreamingDecoded], FinalState] =
    ZChannel.readWithCause[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[StreamingDecoded], FinalState](
      (chunk: Chunk[Byte]) => {
        val (out, updated) = stepChunk(cfg, fs, chunk)
        if (out.isEmpty) loop(cfg, enableDiagnostics, updated)
        else ZChannel.write(out) *> loop(cfg, enableDiagnostics, updated)
      },
      (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
      (_: Any) => ZChannel.succeed(fs)
    )

  private def emitMeta(
    enableDiagnostics: Boolean,
    fs: FinalState
  ): ZChannel[Any, Any, Any, Any, Throwable, Chunk[StreamingDecoded], Unit] =
    ZChannel.fromZIO(finalizeToMeta(enableDiagnostics, fs).map(ZChannel.write(_))).flatten

  def pipeline(
    enableDiagnostics: Boolean = false,
    config: Config = Config.default
  ): ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    ZPipeline.fromChannel(loop(config, enableDiagnostics, initial).flatMap(emitMeta(enableDiagnostics, _)))
}
