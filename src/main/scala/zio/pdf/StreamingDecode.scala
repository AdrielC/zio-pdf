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

import _root_.scodec.{Attempt, DecodeResult, Decoder, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.{Cause, Chunk, NonEmptyChunk}
import zio.stream.{ZChannel, ZPipeline}

object StreamingDecode {

  /** @param inlineMaxBytes
    *   Raw stream payloads of this size or smaller are materialised
    *   once on `ContentObjStart.inlinePayload`; larger streams chunk.
    */
  final case class Config(inlineMaxBytes: Long)

  object Config {
    val default: Config = Config(inlineMaxBytes = 256 * 1024L)
  }

  private sealed trait HeaderEvent
  private object HeaderEvent {
    final case class V(v: Version)                      extends HeaderEvent
    final case class C(b: ByteVector)                 extends HeaderEvent
    final case class S(s: StartXref)                  extends HeaderEvent
    final case class X(x: Xref)                       extends HeaderEvent
    final case class W(b: Byte)                       extends HeaderEvent
    final case class H(o: IndirectObj.IndirectObjHeader) extends HeaderEvent
  }

  private val headerDecoder: Decoder[HeaderEvent] =
    Decoder.choiceDecoder(
      Version.codec.map(HeaderEvent.V(_)),
      summon[_root_.scodec.Codec[Xref]].map(HeaderEvent.X(_)),
      StartXref.codec.map(HeaderEvent.S(_)),
      IndirectObj.headerOnly.map(HeaderEvent.H(_)),
      (Comment.start ~> Comment.line).map(HeaderEvent.C(_)),
      Decoder { bits =>
        if (bits.size < 8L) Attempt.failure(Err.InsufficientBits(8L, bits.size, Nil))
        else {
          val (head, rest) = bits.splitAt(8L)
          val byte         = head.bytes.head
          if (byte == ' '.toByte || byte == '\n'.toByte || byte == '\r'.toByte || byte == '\t'.toByte)
            Attempt.successful(DecodeResult(HeaderEvent.W(byte): HeaderEvent, rest))
          else
            Attempt.failure(Err(s"streaming top-level: unrecognised byte ${byte.toInt & 0xff}"))
        }
      }
    )

  private val streamingHeaderDecoder: Decoder[HeaderEvent] =
    Decoder { bits =>
      headerDecoder.decode(bits) match {
        case s @ Attempt.Successful(_) => s
        case Attempt.Failure(e)        => Attempt.failure(Err.InsufficientBits(0, 0, e.context))
      }
    }

  private val streamTrailer: _root_.scodec.Codec[Unit] = IndirectObj.streamTrailer

  private sealed trait State
  private final case class WaitingHeader(carry: BitVector) extends State
  private final case class ForwardingBytes(remaining: Long, carry: BitVector) extends State
  private final case class BufferingBytes(
    obj: Obj,
    bytesTotal: Int,
    filled: Int,
    carry: BitVector,
    acc: Array[Byte]
  ) extends State
  private final case class SkippingStreamPayload(remaining: Long, carry: BitVector) extends State
  private final case class ConsumingTrailer(carry: BitVector) extends State

  private def headerToEvent(
    cfg: Config,
    event: HeaderEvent,
    remainingBits: BitVector,
    dup: DuplicateFilterState.Mutable
  ): (Chunk[StreamingDecoded], State) = event match {
    case HeaderEvent.V(v) =>
      (Chunk.single(StreamingDecoded.VersionT(v)), WaitingHeader(remainingBits))
    case HeaderEvent.C(b) =>
      (Chunk.single(StreamingDecoded.CommentT(b)), WaitingHeader(remainingBits))
    case HeaderEvent.S(s) =>
      DuplicateFilterState.enterUpdateMode(dup)
      (Chunk.single(StreamingDecoded.StartXrefT(s)), WaitingHeader(remainingBits))
    case HeaderEvent.X(x) =>
      DuplicateFilterState.enterUpdateMode(dup)
      (Chunk.single(StreamingDecoded.XrefT(x)), WaitingHeader(remainingBits))
    case HeaderEvent.W(_) =>
      (Chunk.empty, WaitingHeader(remainingBits))
    case HeaderEvent.H(IndirectObj.IndirectObjHeader(obj, streamLen)) =>
      val suppress = DuplicateFilterState.shouldSuppress(dup, obj.index.number)
      if (suppress)
        streamLen match {
          case None =>
            (Chunk.empty, ConsumingTrailerNoStream(remainingBits))
          case Some(length) =>
            (Chunk.empty, SkippingStreamPayload(length, remainingBits))
        }
      else
        streamLen match {
          case None =>
            (Chunk.single(StreamingDecoded.DataObj(obj)), ConsumingTrailerNoStream(remainingBits))
          case Some(length) =>
            if (length <= cfg.inlineMaxBytes && length <= Int.MaxValue && length > 0L)
              (
                Chunk.empty,
                BufferingBytes(
                  obj,
                  bytesTotal = length.toInt,
                  filled     = 0,
                  carry      = remainingBits,
                  acc        = new Array[Byte](length.toInt)
                )
              )
            else if (length == 0L)
              (
                Chunk.single(StreamingDecoded.ContentObjStart(obj, 0L, Some(BitVector.empty))),
                ConsumingTrailer(remainingBits)
              )
            else
              (
                Chunk.single(StreamingDecoded.ContentObjStart(obj, length, None)),
                ForwardingBytes(length, remainingBits)
              )
        }
  }

  private final case class ConsumingTrailerNoStream(carry: BitVector) extends State

  private val endobjTrailer: _root_.scodec.Codec[Unit] = IndirectObj.endobj

  private def tryConsumeTrailer(
    state: State,
    carry: BitVector
  ): Either[BitVector, Either[Throwable, BitVector]] = state match {
    case ConsumingTrailer(_) =>
      streamTrailer.decode(carry) match {
        case Attempt.Successful(DecodeResult(_, rest))      => Right(Right(rest))
        case Attempt.Failure(_: Err.InsufficientBits)        => Left(carry)
        case Attempt.Failure(comp: Err.Composite)
            if comp.errs.exists(_.isInstanceOf[Err.InsufficientBits]) => Left(carry)
        case Attempt.Failure(other) =>
          Right(Left(new RuntimeException(s"stream trailer: ${other.messageWithContext}")))
      }
    case ConsumingTrailerNoStream(_) =>
      endobjTrailer.decode(carry) match {
        case Attempt.Successful(DecodeResult(_, rest))      => Right(Right(rest))
        case Attempt.Failure(_: Err.InsufficientBits)        => Left(carry)
        case Attempt.Failure(comp: Err.Composite)
            if comp.errs.exists(_.isInstanceOf[Err.InsufficientBits]) => Left(carry)
        case Attempt.Failure(other) =>
          Right(Left(new RuntimeException(s"endobj trailer: ${other.messageWithContext}")))
      }
    case _ => sys.error("not a trailer state")
  }

  private def stepAll(
    cfg: Config,
    state: State,
    dup: DuplicateFilterState.Mutable,
    in: Chunk[StreamingDecoded] = Chunk.empty
  ): (Chunk[StreamingDecoded], State) = state match {

    case fb @ ForwardingBytes(remaining, carry) =>
      if (remaining == 0L)
        stepAll(cfg, ConsumingTrailer(carry), dup, in :+ StreamingDecoded.ContentObjEnd)
      else if (carry.isEmpty)
        (in, fb)
      else {
        val carryBytes = carry.bytes
        val take       = math.min(remaining, carryBytes.size).toInt
        val emit       = Chunk.fromArray(carryBytes.take(take.toLong).toArray)
        val rest       = carry.drop(take.toLong * 8)
        stepAll(
          cfg,
          ForwardingBytes(remaining - take.toLong, rest),
          dup,
          in :+ StreamingDecoded.ContentObjBytes(emit)
        )
      }

    case buf @ BufferingBytes(obj, bytesTotal, filled, carry, acc) =>
      if (filled == bytesTotal) {
        val ev =
          StreamingDecoded.ContentObjStart(obj, bytesTotal.toLong, Some(BitVector(acc)))
        stepAll(cfg, ConsumingTrailer(carry), dup, in :+ ev)
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
        stepAll(cfg, BufferingBytes(obj, bytesTotal, filled + take, rest, acc), dup, in)
      }

    case sb @ SkippingStreamPayload(remaining, carry) =>
      if (remaining == 0L)
        stepAll(cfg, ConsumingTrailer(carry), dup, in)
      else if (carry.isEmpty)
        (in, sb)
      else {
        val carryBytes = carry.bytes
        val take       = math.min(remaining, carryBytes.size).toInt
        val rest       = carry.drop(take.toLong * 8)
        stepAll(cfg, SkippingStreamPayload(remaining - take.toLong, rest), dup, in)
      }

    case ct: (ConsumingTrailer | ConsumingTrailerNoStream) =>
      val carry = ct match {
        case ConsumingTrailer(c)         => c
        case ConsumingTrailerNoStream(c) => c
      }
      tryConsumeTrailer(ct, carry) match {
        case Left(needMore) =>
          (in, ct match {
            case _: ConsumingTrailer         => ConsumingTrailer(needMore)
            case _: ConsumingTrailerNoStream => ConsumingTrailerNoStream(needMore)
          })
        case Right(Right(rest)) => stepAll(cfg, WaitingHeader(rest), dup, in)
        case Right(Left(err))   => throw err
      }

    case wh @ WaitingHeader(carry) =>
      streamingHeaderDecoder.decode(carry) match {
        case Attempt.Successful(DecodeResult(event, rest)) =>
          val (events, next) = headerToEvent(cfg, event, rest, dup)
          stepAll(cfg, next, dup, in ++ events)
        case Attempt.Failure(_) =>
          (in, wh)
      }
  }

  private def feed(
    cfg: Config,
    state: State,
    dup: DuplicateFilterState.Mutable,
    chunk: Chunk[Byte]
  ): (Chunk[StreamingDecoded], State) = {
    val incoming = BitVector.view(chunk.toArray)
    val newCarry = state match {
      case WaitingHeader(c)            => c ++ incoming
      case ForwardingBytes(r, c)       => c ++ incoming
      case BufferingBytes(_, _, _, c, _) => c ++ incoming
      case SkippingStreamPayload(r, c) => c ++ incoming
      case ConsumingTrailer(c)         => c ++ incoming
      case ConsumingTrailerNoStream(c) => c ++ incoming
    }
    val withCarry: State = state match {
      case WaitingHeader(c)            => WaitingHeader(newCarry)
      case ForwardingBytes(r, _)       => ForwardingBytes(r, newCarry)
      case BufferingBytes(o, t, f, _, a) => BufferingBytes(o, t, f, newCarry, a)
      case SkippingStreamPayload(r, _) => SkippingStreamPayload(r, newCarry)
      case _: ConsumingTrailer         => ConsumingTrailer(newCarry)
      case _: ConsumingTrailerNoStream => ConsumingTrailerNoStream(newCarry)
    }
    try stepAll(cfg, withCarry, dup)
    catch { case _: NoSuchElementException => sys.error("unreachable") }
  }

  private final case class FinalState(
    state: State,
    dupFilter: DuplicateFilterState.Mutable,
    xrefs: List[Xref],
    version: Option[Version]
  )

  private def initial: FinalState =
    FinalState(WaitingHeader(BitVector.empty), DuplicateFilterState.initial, Nil, None)

  private def updateAccumulators(fs: FinalState, ev: StreamingDecoded): FinalState = ev match {
    case StreamingDecoded.VersionT(v) => fs.copy(version = Some(v))
    case StreamingDecoded.XrefT(x)    => fs.copy(xrefs = x :: fs.xrefs)
    case _                            => fs
  }

  private def loop(
    cfg: Config,
    log: Log,
    fs: FinalState
  ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[StreamingDecoded], FinalState] =
    ZChannel.readWithCause[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[StreamingDecoded], FinalState](
      (chunk: Chunk[Byte]) => {
        val (out, nextState) = feed(cfg, fs.state, fs.dupFilter, chunk)
        val updatedBase      = fs.copy(state = nextState)
        val updated          = out.foldLeft(updatedBase)(updateAccumulators)
        if (out.isEmpty) loop(cfg, log, updated)
        else ZChannel.write(out) *> loop(cfg, log, updated)
      },
      (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
      (_: Any) =>
        if (fs.dupFilter.duplicateCount > 0)
          ZChannel
            .fromZIO(
              log.debug(
                s"duplicate indirect objects suppressed before first xref (approximate count: ${fs.dupFilter.duplicateCount})"
              )
            )
            .as(fs)
        else
          ZChannel.succeed(fs)
    )

  private def emitMeta(fs: FinalState): ZChannel[Any, Any, Any, Any, Throwable, Chunk[StreamingDecoded], Unit] = {
    val xs        = fs.xrefs.reverse
    val trailers  = xs.map(_.trailer)
    val sanitised = NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
    ZChannel.write(Chunk.single(StreamingDecoded.Meta(xs, sanitised, fs.version)))
  }

  def pipeline(
    log: Log = Log.noop,
    config: Config = Config.default
  ): ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    ZPipeline.fromChannel(loop(config, log, initial).flatMap(emitMeta))
}
