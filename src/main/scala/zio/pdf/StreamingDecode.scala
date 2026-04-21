/*
 * Memory-bounded streaming decoder pipeline:
 *
 *   ZPipeline[Any, Throwable, Byte, StreamingDecoded]
 *
 * Drives a state-machine over the raw byte stream, alternating
 * between two modes:
 *
 *   - WaitingHeader: try to decode one TopLevel-shaped chunk
 *     (version / comment / xref / startxref / indirect-object
 *     HEADER, *not* the stream payload). On success emit the
 *     matching StreamingDecoded event. If it's an object that
 *     starts a content stream, switch to ForwardingBytes.
 *
 *   - ForwardingBytes(remaining): emit bytes as
 *     ContentObjBytes(...) until `remaining` reaches zero, then
 *     consume the literal `\nendstream\nendobj\n` trailer and
 *     switch back to WaitingHeader.
 *
 * Peak memory is bounded by the upstream chunk size + the carry
 * buffer for one TopLevel-shaped header. The actual content
 * stream payload is *forwarded chunk by chunk*; it never lives
 * in a single buffer.
 */

package zio.pdf

import _root_.scodec.{Attempt, DecodeResult, Decoder, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.{Cause, Chunk, NonEmptyChunk}
import zio.stream.{ZChannel, ZPipeline}

object StreamingDecode {

  // ---------------------------------------------------------------
  // The header-mode choice decoder. Same shape as TopLevel's, but
  // the indirect-object case is replaced with `IndirectObj.headerOnly`
  // so we never read past the `stream\n` keyword.
  // ---------------------------------------------------------------

  private sealed trait HeaderEvent
  private object HeaderEvent {
    final case class V(v: Version)                         extends HeaderEvent
    final case class C(b: ByteVector)                       extends HeaderEvent
    final case class S(s: StartXref)                        extends HeaderEvent
    final case class X(x: Xref)                             extends HeaderEvent
    final case class W(b: Byte)                             extends HeaderEvent
    final case class H(o: IndirectObj.IndirectObjHeader)    extends HeaderEvent
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

  /** Force header-decode failures to look like InsufficientBits so
    * the loop pulls more input instead of giving up. */
  private val streamingHeaderDecoder: Decoder[HeaderEvent] =
    Decoder { bits =>
      headerDecoder.decode(bits) match {
        case s @ Attempt.Successful(_) => s
        case Attempt.Failure(e)        => Attempt.failure(Err.InsufficientBits(0, 0, e.context))
      }
    }

  /** Codec for the stream trailer: the trailing `endstream\nendobj\n`
    * after the payload bytes. */
  private val streamTrailer: _root_.scodec.Codec[Unit] = IndirectObj.streamTrailer

  // ---------------------------------------------------------------
  // The state machine.
  // ---------------------------------------------------------------

  private sealed trait State
  private final case class WaitingHeader(carry: BitVector)              extends State
  private final case class ForwardingBytes(remaining: Long, carry: BitVector) extends State
  private final case class ConsumingTrailer(carry: BitVector)           extends State

  /** Convert a header event into a SAX-style emission plus the next state. */
  private def headerToEvent(
    event: HeaderEvent,
    state: WaitingHeader,
    remainingBits: BitVector
  ): (Chunk[StreamingDecoded], State) = event match {
    case HeaderEvent.V(v)                    => (Chunk.single(StreamingDecoded.VersionT(v)), WaitingHeader(remainingBits))
    case HeaderEvent.C(b)                    => (Chunk.single(StreamingDecoded.CommentT(b)), WaitingHeader(remainingBits))
    case HeaderEvent.S(s)                    => (Chunk.single(StreamingDecoded.StartXrefT(s)), WaitingHeader(remainingBits))
    case HeaderEvent.X(x)                    => (Chunk.single(StreamingDecoded.XrefT(x)), WaitingHeader(remainingBits))
    case HeaderEvent.W(_)                    => (Chunk.empty, WaitingHeader(remainingBits))
    case HeaderEvent.H(IndirectObj.IndirectObjHeader(obj, None)) =>
      // No-stream object - the streamStartKeyword wasn't there, so
      // the only thing left to consume is the `endobj\n` keyword.
      // We dispatch to the trailer-consumer state with no payload.
      // For symmetry with the streaming case we emit DataObj here
      // and ConsumingTrailer afterwards.
      (Chunk.single(StreamingDecoded.DataObj(obj)), ConsumingTrailerNoStream(remainingBits))
    case HeaderEvent.H(IndirectObj.IndirectObjHeader(obj, Some(length))) =>
      (
        Chunk.single(StreamingDecoded.ContentObjHeader(obj, length)),
        ForwardingBytes(length, remainingBits)
      )
  }

  /** A second flavour of trailer-consumer: just `endobj\n` (no `endstream`). */
  private final case class ConsumingTrailerNoStream(carry: BitVector) extends State

  // The endobj-only trailer (used by no-stream objects, since
  // headerOnly stops just BEFORE the `endobj` keyword).
  private val endobjTrailer: _root_.scodec.Codec[Unit] =
    IndirectObj.endobj

  /** Try to consume the appropriate trailer from the current carry,
    * returning `Right(remainingBits)` on success and `Left(carry)`
    * on insufficient bits. Other failures bubble up as `Throwable`. */
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

  /** Process as many state transitions as possible against the
    * current carry, emitting events and advancing state. Returns
    * (events, newState, carry-not-yet-consumed). When carry is
    * exhausted, the caller pulls more from upstream and recurses. */
  private def stepAll(
    state: State,
    in: Chunk[StreamingDecoded] = Chunk.empty
  ): (Chunk[StreamingDecoded], State) = state match {

    // ---- Forwarding payload bytes ---------------------------------
    case fb @ ForwardingBytes(remaining, carry) =>
      if (remaining == 0L)
        // Payload done; next step is to consume the `endstream\nendobj` trailer.
        stepAll(ConsumingTrailer(carry), in :+ StreamingDecoded.ContentObjEnd)
      else if (carry.isEmpty)
        (in, fb)
      else {
        val carryBytes = carry.bytes
        val take       = math.min(remaining, carryBytes.size).toInt
        val emit       = Chunk.fromArray(carryBytes.take(take.toLong).toArray)
        val rest       = carry.drop(take.toLong * 8)
        stepAll(
          ForwardingBytes(remaining - take.toLong, rest),
          in :+ StreamingDecoded.ContentObjBytes(emit)
        )
      }

    // ---- Consuming the trailing endstream/endobj ------------------
    case ct: (ConsumingTrailer | ConsumingTrailerNoStream) =>
      val carry = ct match {
        case ConsumingTrailer(c)         => c
        case ConsumingTrailerNoStream(c) => c
      }
      tryConsumeTrailer(ct, carry) match {
        case Left(needMore)               => (in, ct match {
          case _: ConsumingTrailer         => ConsumingTrailer(needMore)
          case _: ConsumingTrailerNoStream => ConsumingTrailerNoStream(needMore)
        })
        case Right(Right(rest))           => stepAll(WaitingHeader(rest), in)
        case Right(Left(err))             => throw err
      }

    // ---- Waiting for the next header event ------------------------
    case wh @ WaitingHeader(carry) =>
      streamingHeaderDecoder.decode(carry) match {
        case Attempt.Successful(DecodeResult(event, rest)) =>
          val (events, next) = headerToEvent(event, wh, rest)
          stepAll(next, in ++ events)
        case Attempt.Failure(_) =>
          // Out of bits - need to pull more.
          (in, wh)
      }
  }

  /** Append an upstream chunk's bytes to whatever carry the current
    * state has, then run the state machine to exhaustion. */
  private def feed(
    state: State,
    chunk: Chunk[Byte]
  ): (Chunk[StreamingDecoded], State) = {
    val incoming = BitVector.view(chunk.toArray)
    val newCarry = state match {
      case WaitingHeader(c)             => c ++ incoming
      case ForwardingBytes(r, c)        => c ++ incoming
      case ConsumingTrailer(c)          => c ++ incoming
      case ConsumingTrailerNoStream(c)  => c ++ incoming
    }
    val withCarry: State = state match {
      case _: WaitingHeader               => WaitingHeader(newCarry)
      case ForwardingBytes(r, _)          => ForwardingBytes(r, newCarry)
      case _: ConsumingTrailer            => ConsumingTrailer(newCarry)
      case _: ConsumingTrailerNoStream    => ConsumingTrailerNoStream(newCarry)
    }
    try stepAll(withCarry)
    catch { case _: NoSuchElementException => sys.error("unreachable") }
  }

  // ---------------------------------------------------------------
  // The pipeline.
  // ---------------------------------------------------------------

  private final case class FinalState(
    state: State,
    xrefs: List[Xref],
    version: Option[Version]
  )

  private val initial: FinalState = FinalState(WaitingHeader(BitVector.empty), Nil, None)

  /** Update the version/xrefs accumulators for a single emitted event. */
  private def updateAccumulators(fs: FinalState, ev: StreamingDecoded): FinalState = ev match {
    case StreamingDecoded.VersionT(v) => fs.copy(version = Some(v))
    case StreamingDecoded.XrefT(x)    => fs.copy(xrefs = x :: fs.xrefs)
    case _                             => fs
  }

  private def loop(
    fs: FinalState
  ): ZChannel[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[StreamingDecoded], FinalState] =
    ZChannel.readWithCause[Any, Throwable, Chunk[Byte], Any, Throwable, Chunk[StreamingDecoded], FinalState](
      (chunk: Chunk[Byte]) => {
        val (out, next) = feed(fs.state, chunk)
        val updated     = out.foldLeft(fs.copy(state = next))(updateAccumulators)
        if (out.isEmpty) loop(updated)
        else ZChannel.write(out) *> loop(updated)
      },
      (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
      (_: Any) => ZChannel.succeed(fs)
    )

  /** Emit the final Meta event from the accumulated state. */
  private def emitMeta(fs: FinalState): ZChannel[Any, Any, Any, Any, Throwable, Chunk[StreamingDecoded], Unit] = {
    val trailers  = fs.xrefs.map(_.trailer)
    val sanitised = NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
    ZChannel.write(Chunk.single(StreamingDecoded.Meta(fs.xrefs, sanitised, fs.version)))
  }

  /** Memory-bounded streaming decoder pipeline. Equivalent in
    * coverage to `Decode.fromTopLevel >>> ` (xref + trailer
    * accumulation, terminating Meta) but emits content stream
    * payloads as a sequence of `ContentObjBytes` chunks instead of
    * one big `BitVector`. */
  val pipeline: ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    ZPipeline.fromChannel(loop(initial).flatMap(emitMeta))
}
