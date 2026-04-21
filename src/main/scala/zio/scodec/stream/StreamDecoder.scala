/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (see LICENSE for details).
 *
 * ZIO port of `scodec.stream.StreamDecoder`. The original FS2-based
 * implementation is interpreted into a `ZChannel`, which is the proper
 * primitive for stream-shape transformations in ZIO. Each
 * [[StreamDecoder]] is interpreted by [[StreamDecoder.runStep]] into a
 * single `ZChannel` whose `OutDone` carries the leftover [[BitVector]]
 * (bits the decoder pulled from upstream but did not consume). This is
 * what makes `++` composition (`Append`) and `isolate` straightforward,
 * because the leftover always flows through the channel done channel
 * instead of being dropped on the floor.
 */

package zio.scodec.stream

import _root_.scodec.{Attempt, DecodeResult, Decoder, Err}
import _root_.scodec.bits.BitVector
import zio.*
import zio.prelude.fx.ZPure
import zio.stream.*

/**
 * Streaming binary decoder backed by a `ZChannel`.
 *
 * The main reason to reach for [[StreamDecoder]] over a plain
 * [[Decoder]] is that decoded values are emitted as soon as they are
 * available, instead of being buffered until the underlying
 * bit-stream completes. This makes it usable for incremental decoding
 * of large (or unbounded) inputs.
 */
final class StreamDecoder[+A] private (private[stream] val step: StreamDecoder.Step[A]) { self =>
  import StreamDecoder.*

  /** Compile this decoder into a `ZPipeline` from `BitVector` to `A`. */
  def toPipeline: ZPipeline[Any, Throwable, BitVector, A] =
    ZPipeline.fromChannel(toChannel.unit)

  /** Compile this decoder into a `ZPipeline` from `Byte` to `A`. */
  def toBytePipeline: ZPipeline[Any, Throwable, Byte, A] =
    bytesToBits >>> toPipeline

  /**
   * Apply this decoder to the given bit-stream, emitting decoded
   * values as they become available.
   */
  def decode[R](source: ZStream[R, Throwable, BitVector]): ZStream[R, Throwable, A] =
    source.via(toPipeline)

  /**
   * Compile this decoder into a `ZChannel`. The channel reads
   * `Chunk[BitVector]` from upstream, writes `Chunk[A]` downstream,
   * and completes with the leftover [[BitVector]] (bits that were
   * pulled from upstream but not consumed by this decoder).
   */
  def toChannel: BitChannel[A] = runStep(step, BitVector.empty)

  /**
   * Decode the entire input as a single value. This used to spin up
   * a `Runtime` and run the channel via `unsafe.run` — which is
   * wildly heavyweight for code that has no I/O at all once the
   * `BitVector` is in memory. We now interpret the algebra directly
   * in pure code via [[StreamDecoder.runStrict]], which gives us
   * stack-safe decoding with no fiber, no executor, no allocation
   * per step beyond an accumulator and the carry buffer.
   */
  def strict: Decoder[Chunk[A]] =
    new Decoder[Chunk[A]] {
      def decode(bits: BitVector): Attempt[DecodeResult[Chunk[A]]] =
        StreamDecoder.runStrict(self.step, bits, Chunk.empty[A]) match {
          case Right((acc, leftover))    => Attempt.successful(DecodeResult(acc, leftover))
          case Left(CodecError(err))     => Attempt.failure(err)
        }
    }

  /**
   * Sequence this decoder with `f`. After this decoder produces an
   * `A`, `f` is invoked to obtain the next decoder, which continues
   * decoding from where this one left off.
   */
  def flatMap[B](f: A => StreamDecoder[B]): StreamDecoder[B] =
    new StreamDecoder[B](
      self.step match {
        case Empty                       => Empty
        case Result(a)                   => f(a).step
        case Failed(cause)               => Failed(cause)
        case Decode(s, once, failOnErr)  =>
          // ZPure.map at the type level gives us `f` over the inner
          // value, then we lift that back into a sub-StreamDecoder
          // by chaining `flatMap(f)`. This is the structural reason
          // the algebra wants `DecoderStep` and not a raw function:
          // map composes natively through `ZPure`.
          Decode(s.map(_.flatMap(f)), once, failOnErr)
        case Isolate(bits, decoder)      => Isolate(bits, decoder.flatMap(f))
        case Append(x, y)                => Append(x.flatMap(f), () => y().flatMap(f))
        case FromPureChunked(pure)       =>
          // Re-express the chunked variant by lifting the
          // batched PureDecoder into a regular StreamDecoder of
          // chunks, flattening the chunks, and then applying f.
          val flattened: StreamDecoder[A] =
            new StreamDecoder[Chunk[A]](FromPure(pure))
              .flatMap(c => StreamDecoder.emits(c))
          flattened.flatMap(f).step
        case FromPure(pure)              =>
          // Collapse `FromPure(p).flatMap(f)` into a `Decode` step
          // built from a `DecoderStep`. The step runs the pure
          // decoder once per call:
          //
          //   - if it needs more bits and emitted nothing yet,
          //     report `CodecError(InsufficientBits)` so the
          //     surrounding Decode loop pulls more input and retries
          //   - otherwise emit `emits(log).flatMap(f)` and either
          //     splice in a self-recursive call (NeedMore + non-empty
          //     log) or stop (Done / DoneTryAgain).
          lazy val self: StreamDecoder[B] =
            new StreamDecoder[A](FromPure(pure)).flatMap(f)
          val pureStep: PureDecoder.DecoderStep[StreamDecoder[B]] =
            ZPure.get[BitVector].flatMap { buffer =>
              val (log, result) = pure.run.runAll(buffer)
              result match {
                case Left(err) =>
                  if (log.isEmpty)
                    ZPure.fail(err)
                  else
                    ZPure.set[BitVector](BitVector.empty) *>
                      ZPure.succeed(StreamDecoder.emits(log).flatMap(f) ++ StreamDecoder.raiseError(err))
                case Right((leftover, status)) =>
                  status match {
                    case PureDecoder.Status.NeedMore if log.isEmpty =>
                      ZPure.fail(CodecError(Err.InsufficientBits(0L, buffer.size, Nil)))
                    case PureDecoder.Status.NeedMore =>
                      ZPure.set[BitVector](leftover) *>
                        ZPure.succeed(StreamDecoder.emits(log).flatMap(f) ++ self)
                    case _ =>
                      ZPure.set[BitVector](leftover) *>
                        ZPure.succeed(StreamDecoder.emits(log).flatMap(f))
                  }
              }
            }
          Decode(pureStep, once = true, failOnErr = true)
      }
    )

  /** Map the supplied function over each output of this decoder. */
  def map[B](f: A => B): StreamDecoder[B] = flatMap(a => StreamDecoder.emit(f(a)))

  /**
   * Run `this` followed by `that`. `that` continues decoding from the
   * leftover bits produced by `this`. Recursive use should be guarded
   * carefully against `InsufficientBits` decoders, otherwise the
   * stream may loop forever feeding the same buffer back to itself.
   */
  def ++[A2 >: A](that: => StreamDecoder[A2]): StreamDecoder[A2] =
    new StreamDecoder(Append[A2](this, () => that))

  /** Alias for `StreamDecoder.isolate(bits)(this)`. */
  def isolate(bits: Long): StreamDecoder[A] = StreamDecoder.isolate(bits)(this)
}

object StreamDecoder {

  // -------------------------------------------------------------------
  // Algebra
  // -------------------------------------------------------------------

  private[stream] sealed trait Step[+A]
  private[stream] case object Empty                                extends Step[Nothing]
  private[stream] case class Result[A](value: A)                   extends Step[A]
  private[stream] case class Failed(cause: Throwable)              extends Step[Nothing]
  private[stream] case class Decode[A](
    step: PureDecoder.DecoderStep[StreamDecoder[A]],
    once: Boolean,
    failOnErr: Boolean
  ) extends Step[A]
  private[stream] case class Isolate[A](bits: Long, decoder: StreamDecoder[A]) extends Step[A]
  private[stream] case class Append[A](x: StreamDecoder[A], y: () => StreamDecoder[A]) extends Step[A]
  private[stream] case class FromPure[A](pure: PureDecoder[A]) extends Step[A]
  private[stream] case class FromPureChunked[A](pure: PureDecoder[Chunk[A]]) extends Step[A]

  /**
   * Type alias used internally for the channels we build: read
   * `Chunk[BitVector]`, write `Chunk[A]`, finish with leftover
   * `BitVector` (the bits that were peeked from upstream but not used).
   */
  type BitChannel[+A] =
    ZChannel[Any, Throwable, Chunk[BitVector], Any, Throwable, Chunk[A], BitVector]

  /** Pipeline that converts `Byte` chunks into `BitVector` chunks. */
  private val bytesToBits: ZPipeline[Any, Nothing, Byte, BitVector] =
    ZPipeline.mapChunks[Byte, BitVector](chunk => Chunk.single(BitVector.view(chunk.toArray)))

  // -------------------------------------------------------------------
  // Constructors
  // -------------------------------------------------------------------

  /** Decoder that emits no elements and consumes no input. */
  val empty: StreamDecoder[Nothing] = new StreamDecoder[Nothing](Empty)

  /** Decoder that emits a single value and consumes no input. */
  def emit[A](a: A): StreamDecoder[A] = new StreamDecoder[A](Result(a))

  /** Decoder that emits all of `as` without consuming input. */
  def emits[A](as: Iterable[A]): StreamDecoder[A] =
    as.foldLeft(empty: StreamDecoder[A])((acc, a) => acc ++ emit(a))

  /**
   * Lift a `scodec.Decoder[A]` into a `DecoderStep` that produces a
   * `StreamDecoder[A]` (which simply emits the decoded value). This
   * is the only place where we cross the
   * `BitVector => Attempt[DecodeResult[…]]` boundary: every other
   * Decode step in the system is built from a `DecoderStep`.
   */
  private def liftEmitting[A](decoder: Decoder[A]): PureDecoder.DecoderStep[StreamDecoder[A]] =
    PureDecoder.fromDecoder(decoder).map(emit[A])

  /** Decode exactly one value with the given decoder. */
  def once[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](Decode(liftEmitting(decoder), once = true, failOnErr = true))

  /** Repeatedly decode values with the given decoder. */
  def many[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](Decode(liftEmitting(decoder), once = false, failOnErr = true))

  /**
   * Try to decode one value. If decoding fails, no input is consumed
   * and no value is emitted.
   */
  def tryOnce[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](Decode(liftEmitting(decoder), once = true, failOnErr = false))

  /**
   * Repeatedly decode values until decoding fails. On failure the
   * read bits are not consumed and the decoder terminates, having
   * emitted any successfully decoded values up to that point.
   */
  def tryMany[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](Decode(liftEmitting(decoder), once = false, failOnErr = false))

  /** A decoder that fails immediately with the given exception. */
  def raiseError(cause: Throwable): StreamDecoder[Nothing] = new StreamDecoder(Failed(cause))

  /** A decoder that fails immediately with the given scodec [[Err]]. */
  def raiseError(err: Err): StreamDecoder[Nothing] = raiseError(CodecError(err))

  /**
   * Read `bits` bits from the input and decode them with `decoder`.
   * Any remainder produced by the inner decoder is discarded.
   */
  def isolate[A](bits: Long)(decoder: StreamDecoder[A]): StreamDecoder[A] =
    new StreamDecoder(Isolate(bits, decoder))

  /** A decoder that ignores `bits` bits of input. */
  def ignore(bits: Long): StreamDecoder[Nothing] =
    once(_root_.scodec.codecs.ignore(bits)).flatMap(_ => empty)

  /**
   * Lift a [[PureDecoder]] into the streaming side. This is the
   * bridge between the pure-step world (`zio-prelude.ZPure`) and
   * the streaming world (`zio.stream.ZChannel`).
   *
   * On every chunk pulled from upstream the buffer is appended to
   * the carry, the pure decoder is `runAll`-ed against the new
   * carry, every entry from its log is written downstream, and the
   * resulting state becomes the next carry. The pure decoder's
   * [[PureDecoder.Status]] decides whether to keep pulling
   * (`NeedMore` / `DoneTryAgain`) or to stop (`Done`).
   */
  def fromPure[A](pure: PureDecoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](FromPure(pure))

  /**
   * Lift a *batched* pure decoder. Where [[fromPure]] expects the
   * pure decoder to log one element per emission, `fromPureChunked`
   * expects each log entry to itself be a `Chunk[A]` (i.e. a batch
   * already prepared in pure code). The streaming layer flattens
   * those batches into the downstream chunk stream verbatim - one
   * downstream `ZChannel.write` per batch instead of per element.
   *
   * Combined with [[PureDecoder.manyChunked]] this is the fastest
   * path for byte-aligned, fixed-width formats: it amortises both
   * the streaming-channel cost *and* the per-element decoding cost.
   */
  def fromPureChunked[A](pure: PureDecoder[Chunk[A]]): StreamDecoder[A] =
    new StreamDecoder[A](FromPureChunked(pure))

  // -------------------------------------------------------------------
  // Interpreter
  // -------------------------------------------------------------------

  /**
   * Interpret `step` into a channel, threading the running buffer
   * `carry` through the algebra explicitly so that no bits are
   * dropped between sub-decoders.
   */
  private def runStep[A](step: Step[A], carry: BitVector): BitChannel[A] =
    step match {
      case Empty                       => ZChannel.succeed(carry)
      case Result(a)                   => ZChannel.write(Chunk.single(a)) *> ZChannel.succeed(carry)
      case Failed(cause)               => ZChannel.fail(cause)
      case Decode(f, once, failOnErr)  => decodeLoop(f, once, failOnErr, carry)
      case Isolate(bits, decoder)      => isolateChannel(bits, decoder, carry)
      case Append(x, y) =>
        runStep(x.step, carry).flatMap(leftover => runStep(y().step, leftover))
      case FromPure(pure)              => fromPureChannel(pure, carry)
      case FromPureChunked(pure)       => fromPureChunkedChannel(pure, carry)
    }

  /**
   * Drive a [[PureDecoder]] from the streaming side. On every chunk
   * pulled from upstream, append it to the carry, `runAll` the pure
   * decoder, write its log entries downstream, take its updated
   * state as the new carry, and decide whether to keep going based
   * on the returned [[PureDecoder.Status]].
   */
  private def fromPureChannel[A](
    pure: PureDecoder[A],
    carry: BitVector
  ): BitChannel[A] = {

    def emitLog(log: Chunk[A], next: BitChannel[A]): BitChannel[A] =
      if (log.isEmpty) next
      else ZChannel.write(log) *> next

    def runOnce(buffer: BitVector): BitChannel[A] = {
      val (log, result) = pure.run.runAll(buffer)
      result match {
        case Left(err) =>
          emitLog(log, ZChannel.fail(err))
        case Right((leftover, status)) =>
          status match {
            case PureDecoder.Status.NeedMore     => emitLog(log, waitForMore(leftover))
            case PureDecoder.Status.Done         => emitLog(log, ZChannel.succeed(leftover))
            case PureDecoder.Status.DoneTryAgain =>
              if (leftover == buffer) emitLog(log, waitForMore(leftover))
              else emitLog(log, runOnce(leftover))
          }
      }
    }

    def waitForMore(buffer: BitVector): BitChannel[A] =
      ZChannel.readWithCause(
        (chunk: Chunk[BitVector]) => runOnce(buffer ++ concatChunk(chunk)),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => ZChannel.succeed(buffer)
      )

    if (carry.isEmpty) waitForMore(carry) else runOnce(carry)
  }

  /**
   * Drive a *batched* `PureDecoder[Chunk[A]]` from the streaming
   * side. Each log entry is itself a `Chunk[A]` and is shipped
   * downstream verbatim. Combined with `PureDecoder.manyChunked`
   * this is the fastest streaming path: one upstream pull -> one
   * pure batch decode -> one downstream `ZChannel.write`.
   */
  private def fromPureChunkedChannel[A](
    pure: PureDecoder[Chunk[A]],
    carry: BitVector
  ): BitChannel[A] = {

    def emitLog(log: Chunk[Chunk[A]], next: BitChannel[A]): BitChannel[A] =
      if (log.isEmpty) next
      else if (log.size == 1) ZChannel.write(log(0)) *> next
      else ZChannel.write(log.flatten) *> next

    def runOnce(buffer: BitVector): BitChannel[A] = {
      val (log, result) = pure.run.runAll(buffer)
      result match {
        case Left(err) =>
          emitLog(log, ZChannel.fail(err))
        case Right((leftover, status)) =>
          status match {
            case PureDecoder.Status.NeedMore     => emitLog(log, waitForMore(leftover))
            case PureDecoder.Status.Done         => emitLog(log, ZChannel.succeed(leftover))
            case PureDecoder.Status.DoneTryAgain =>
              if (leftover == buffer) emitLog(log, waitForMore(leftover))
              else emitLog(log, runOnce(leftover))
          }
      }
    }

    def waitForMore(buffer: BitVector): BitChannel[A] =
      ZChannel.readWithCause(
        (chunk: Chunk[BitVector]) => runOnce(buffer ++ concatChunk(chunk)),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => ZChannel.succeed(buffer)
      )

    if (carry.isEmpty) waitForMore(carry) else runOnce(carry)
  }

  /**
   * Pull from upstream until we have at least `bits` bits buffered,
   * then split the buffer at `bits`. The first half is fed (as a
   * single-element ZStream) into `inner`; the second half (plus
   * anything left in upstream) becomes our leftover.
   */
  private def isolateChannel[A](bits: Long, inner: StreamDecoder[A], carry: BitVector): BitChannel[A] = {
    def collect(buffer: BitVector): BitChannel[A] =
      if (buffer.size < bits)
        ZChannel.readWithCause(
          (chunk: Chunk[BitVector]) => collect(buffer ++ concatChunk(chunk)),
          (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
          (_: Any) =>
            ZChannel.fail(CodecError(Err.InsufficientBits(bits, buffer.size, Nil)))
        )
      else {
        val (used, rest) = buffer.splitAt(bits)
        val innerSource: ZChannel[Any, Any, Any, Any, Throwable, Chunk[BitVector], Any] =
          ZChannel.write(Chunk.single(used))
        // Run inner against the isolated source; its leftover is discarded.
        (innerSource >>> inner.toChannel).flatMap(_ => ZChannel.succeed(rest))
      }
    collect(carry)
  }

  /**
   * The main decode loop. Reads chunks of [[BitVector]] from upstream
   * and accumulates them in `carry` until a value can be decoded.
   * Drives the supplied [[PureDecoder.DecoderStep]] in pure code per
   * iteration; the channel is only used to pull more input when the
   * step reports `InsufficientBits`.
   */
  private def decodeLoop[A](
    decodeStep: PureDecoder.DecoderStep[StreamDecoder[A]],
    once: Boolean,
    failOnErr: Boolean,
    carry: BitVector
  ): BitChannel[A] = {

    def attemptDecode(buffer: BitVector): BitChannel[A] =
      decodeStep.runAll(buffer) match {
        case (_, Right((remainder, subDecoder))) =>
          // Run the sub-decoder, threading `remainder` as its initial
          // carry so that no bits are dropped if the sub-decoder does
          // not pull from upstream (e.g. when it is `emit(_)`).
          val subChannel: BitChannel[A] = runStep(subDecoder.step, remainder)
          if (once)
            subChannel
          else
            subChannel.flatMap(subLeftover => decodeLoop(decodeStep, once = false, failOnErr, subLeftover))

        case (_, Left(CodecError(err))) =>
          if (isInsufficient(err)) waitForMore(buffer)
          else if (failOnErr) ZChannel.fail(CodecError(err))
          else ZChannel.succeed(buffer)
      }

    def waitForMore(buffer: BitVector): BitChannel[A] =
      ZChannel.readWithCause(
        (chunk: Chunk[BitVector]) => attemptDecode(buffer ++ concatChunk(chunk)),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) =>
          // Upstream is done. If there is buffered data we hand it
          // back as leftover so callers (e.g. `++`) can keep working
          // with it.
          ZChannel.succeed(buffer)
      )

    if (carry.isEmpty) waitForMore(carry) else attemptDecode(carry)
  }

  /** True iff `err` is (or contains) an `Err.InsufficientBits`. */
  private[stream] def isInsufficient(err: Err): Boolean = err match {
    case _: Err.InsufficientBits => true
    case comp: Err.Composite     => comp.errs.exists(isInsufficient)
    case _                       => false
  }

  private def concatChunk(chunk: Chunk[BitVector]): BitVector =
    if (chunk.isEmpty) BitVector.empty
    else if (chunk.size == 1) chunk(0)
    else chunk.foldLeft(BitVector.empty)(_ ++ _)

  // -------------------------------------------------------------------
  // Pure, Runtime-free interpreter used by `strict`.
  //
  // Walks the same algebra as `runStep` but in plain `Either`. There
  // is no fiber, no executor, no `Unsafe.unsafe`, no allocation per
  // step beyond the accumulator and the carry buffer. This matters
  // because `strict` is on the hot path of every `Codec` use site
  // that wants to embed a streaming decoder inside a non-streaming
  // codec - and there is genuinely no I/O to manage at that point.
  // -------------------------------------------------------------------

  private[stream] def runStrict[A](
    step: Step[A],
    bits: BitVector,
    acc: Chunk[A]
  ): Either[CodecError, (Chunk[A], BitVector)] = step match {

    case Empty         => Right((acc, bits))
    case Result(a)     => Right((acc :+ a, bits))
    case Failed(cause) =>
      // We can't smuggle an arbitrary Throwable through `Attempt`'s
      // failure channel, so wrap as Err.General when needed.
      cause match {
        case CodecError(err) => Left(CodecError(err))
        case other           => Left(CodecError(Err.General(Option(other.getMessage).getOrElse(other.toString), Nil)))
      }

    case Append(x, y) =>
      runStrict(x.step, bits, acc) match {
        case Right((acc1, leftover)) => runStrict(y().step, leftover, acc1)
        case left                    => left
      }

    case Decode(decodeStep, once, failOnErr) =>
      def loop(acc1: Chunk[A], buffer: BitVector): Either[CodecError, (Chunk[A], BitVector)] =
        decodeStep.runAll(buffer) match {
          case (_, Right((remainder, subDecoder))) =>
            runStrict(subDecoder.step, remainder, acc1) match {
              case Right((acc2, leftover)) =>
                if (once) Right((acc2, leftover))
                else loop(acc2, leftover)
              case left => left
            }
          case (_, Left(CodecError(err))) =>
            if (isInsufficient(err))
              // No more input is coming in strict mode, so
              // InsufficientBits means we are done - hand the
              // unconsumed buffer back.
              Right((acc1, buffer))
            else if (failOnErr) Left(CodecError(err))
            else Right((acc1, buffer))
        }
      loop(acc, bits)

    case Isolate(n, decoder) =>
      if (bits.size < n)
        Left(CodecError(Err.InsufficientBits(n, bits.size, Nil)))
      else {
        val (used, rest) = bits.splitAt(n)
        runStrict(decoder.step, used, acc) match {
          case Right((acc1, _)) => Right((acc1, rest))
          case left             => left
        }
      }

    case FromPure(pure) =>
      // The pure decoder does the loop itself - just `runAll` it.
      val (log, result) = pure.run.runAll(bits)
      result match {
        case Left(err)              => Left(err)
        case Right((leftover, _))   => Right((acc ++ log, leftover))
      }

    case FromPureChunked(pure) =>
      val (log, result) = pure.run.runAll(bits)
      result match {
        case Left(err)              => Left(err)
        case Right((leftover, _))   =>
          // Flatten the batches into a single accumulator chunk.
          val flat = log.foldLeft(acc)((a, c) => a ++ c)
          Right((flat, leftover))
      }
  }
}
