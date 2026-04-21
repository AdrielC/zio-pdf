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
   * Decode the entire input as a single value. Useful when a
   * `StreamDecoder` is composed inside an `Isolate` or as part of a
   * non-streaming use site (mainly tests).
   */
  def strict: Decoder[Chunk[A]] =
    new Decoder[Chunk[A]] {
      def decode(bits: BitVector): Attempt[DecodeResult[Chunk[A]]] = {
        val source: ZStream[Any, Throwable, BitVector] = ZStream.succeed(bits)
        val channel: ZChannel[Any, Any, Any, Any, Throwable, Chunk[A], BitVector] =
          source.channel >>> self.toChannel
        val program: ZIO[Any, Throwable, (Chunk[A], BitVector)] =
          ZIO.scoped[Any] {
            channel.toPull.flatMap { pull =>
              def loop(acc: Chunk[A]): ZIO[Any, Throwable, (Chunk[A], BitVector)] =
                pull.foldZIO(
                  err => ZIO.fail(err),
                  {
                    case Left(leftover) => ZIO.succeed((acc, leftover))
                    case Right(chunk)   => loop(acc ++ chunk)
                  }
                )
              loop(Chunk.empty)
            }
          }
        Unsafe.unsafe { implicit u =>
          Runtime.default.unsafe.run(program) match {
            case Exit.Success((acc, leftover)) => Attempt.successful(DecodeResult(acc, leftover))
            case Exit.Failure(cause) =>
              cause.failureOption match {
                case Some(CodecError(err)) => Attempt.failure(err)
                case Some(other) =>
                  Attempt.failure(Err.General(Option(other.getMessage).getOrElse(other.toString), Nil))
                case None =>
                  Attempt.failure(Err.General(s"unexpected defect: ${cause.prettyPrint}", Nil))
              }
          }
        }
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
        case Empty                      => Empty
        case Result(a)                  => f(a).step
        case Failed(cause)              => Failed(cause)
        case Decode(g, once, failOnErr) =>
          Decode(in => g(in).map(_.map(_.flatMap(f))), once, failOnErr)
        case Isolate(bits, decoder)     => Isolate(bits, decoder.flatMap(f))
        case Append(x, y)               => Append(x.flatMap(f), () => y().flatMap(f))
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
    f: BitVector => Attempt[DecodeResult[StreamDecoder[A]]],
    once: Boolean,
    failOnErr: Boolean
  ) extends Step[A]
  private[stream] case class Isolate[A](bits: Long, decoder: StreamDecoder[A]) extends Step[A]
  private[stream] case class Append[A](x: StreamDecoder[A], y: () => StreamDecoder[A]) extends Step[A]

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

  /** Decode exactly one value with the given decoder. */
  def once[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](
      Decode(in => decoder.decode(in).map(_.map(emit)), once = true, failOnErr = true)
    )

  /** Repeatedly decode values with the given decoder. */
  def many[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](
      Decode(in => decoder.decode(in).map(_.map(emit)), once = false, failOnErr = true)
    )

  /**
   * Try to decode one value. If decoding fails, no input is consumed
   * and no value is emitted.
   */
  def tryOnce[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](
      Decode(in => decoder.decode(in).map(_.map(emit)), once = true, failOnErr = false)
    )

  /**
   * Repeatedly decode values until decoding fails. On failure the
   * read bits are not consumed and the decoder terminates, having
   * emitted any successfully decoded values up to that point.
   */
  def tryMany[A](decoder: Decoder[A]): StreamDecoder[A] =
    new StreamDecoder[A](
      Decode(in => decoder.decode(in).map(_.map(emit)), once = false, failOnErr = false)
    )

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
   * Mirrors the structure of the original FS2 `Decode` interpreter.
   */
  private def decodeLoop[A](
    f: BitVector => Attempt[DecodeResult[StreamDecoder[A]]],
    once: Boolean,
    failOnErr: Boolean,
    carry: BitVector
  ): BitChannel[A] = {

    def attempt(buffer: BitVector): BitChannel[A] =
      f(buffer) match {
        case Attempt.Successful(DecodeResult(subDecoder, remainder)) =>
          // Run the sub-decoder, threading `remainder` as its initial
          // carry so that no bits are dropped if the sub-decoder does
          // not pull from upstream (e.g. when it is `emit(_)`).
          val subChannel: BitChannel[A] = runStep(subDecoder.step, remainder)
          if (once)
            subChannel
          else
            subChannel.flatMap(subLeftover => decodeLoop(f, once = false, failOnErr, subLeftover))

        case Attempt.Failure(_: Err.InsufficientBits) =>
          // Need more input - keep reading.
          waitForMore(buffer)

        case Attempt.Failure(comp: Err.Composite)
            if comp.errs.exists(_.isInstanceOf[Err.InsufficientBits]) =>
          waitForMore(buffer)

        case Attempt.Failure(e) =>
          if (failOnErr) ZChannel.fail(CodecError(e))
          else ZChannel.succeed(buffer)
      }

    def waitForMore(buffer: BitVector): BitChannel[A] =
      ZChannel.readWithCause(
        (chunk: Chunk[BitVector]) => attempt(buffer ++ concatChunk(chunk)),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) =>
          // Upstream is done. If there is buffered data we hand it
          // back as leftover so callers (e.g. `++`) can keep working
          // with it.
          ZChannel.succeed(buffer)
      )

    if (carry.isEmpty) waitForMore(carry) else attempt(carry)
  }

  private def concatChunk(chunk: Chunk[BitVector]): BitVector =
    if (chunk.isEmpty) BitVector.empty
    else if (chunk.size == 1) chunk(0)
    else chunk.foldLeft(BitVector.empty)(_ ++ _)
}
