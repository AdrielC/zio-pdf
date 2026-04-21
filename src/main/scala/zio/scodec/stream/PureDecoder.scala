/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (see LICENSE for details).
 */

package zio.scodec.stream

import _root_.scodec.bits.BitVector
import _root_.scodec.{Attempt, DecodeResult, Decoder, Err}
import zio.*
import zio.prelude.fx.ZPure

/**
 * A `PureDecoder[A]` is the *pure step function* of a streaming
 * decoder. It is implemented as a `ZPure` whose four "capabilities"
 * map directly onto the four things an incremental decoder needs:
 *
 *   - **State** `S1 = S2 = BitVector` is the carry buffer of bits
 *     pulled from upstream but not yet consumed.
 *   - **Log**   `W  = A` is the decoded values produced by this step.
 *     Using the log channel for emissions means `runAll` already
 *     gives us back a `Chunk[A]` of outputs — no extra accumulator
 *     in the call site.
 *   - **Error** `E  = CodecError` is reserved for *fatal* decoding
 *     failures. Recoverable "need more bits" is modelled in the
 *     success channel via [[PureDecoder.Status]] so callers can
 *     loop without paying for a `ZPure.fail` allocation.
 *   - **Reader** `R  = Any` because we have no environment to read
 *     for now (the legacy `Log` could plug in here later).
 *
 * `PureDecoder` is the *pure* half of the decoding story.
 * `StreamDecoder` (built on `ZChannel`) is the *I/O* half. They
 * compose: [[StreamDecoder.fromPure]] lifts a `PureDecoder` into the
 * streaming side. This is the proper division of labor between
 * `zio-prelude.ZPure` and `zio.stream.ZChannel`:
 *
 *   - `ZPure` = stack-safe, allocation-light per-step computation
 *     with explicit state/log/error.
 *   - `ZChannel` = the producer/consumer plumbing that pulls bits
 *     from upstream and pushes decoded values downstream.
 *
 * This means heavy stateful decoders (e.g. xref accumulation, PDF
 * trailer assembly) can be written and tested completely without a
 * `Runtime`, then dropped into the streaming pipeline at the end.
 */
final case class PureDecoder[+A](
  run: ZPure[A, BitVector, BitVector, Any, CodecError, PureDecoder.Status]
) { self =>
  import PureDecoder.*


  /** Decode a single in-memory `BitVector` strictly. */
  def decodeStrict(bits: BitVector): Either[CodecError, DecodeResult[Chunk[A]]] = {
    val (log, result) = run.runAll(bits)
    result match {
      case Left(err)              => Left(err)
      case Right((leftover, _))   => Right(DecodeResult(log, leftover))
    }
  }

  /** Decode strictly and unwrap into an [[Attempt]] over the emitted chunk. */
  def attemptStrict(bits: BitVector): Attempt[DecodeResult[Chunk[A]]] =
    decodeStrict(bits) match {
      case Right(r)              => Attempt.successful(r)
      case Left(CodecError(err)) => Attempt.failure(err)
    }

  /**
   * Map every emitted value. `ZPure` (this version) does not expose
   * a `mapLog`, so we replay the computation: `runAll` to capture
   * the log, then rebuild a fresh `ZPure` that re-emits the mapped
   * values, threads the produced state, and re-raises any failure.
   */
  def map[B](f: A => B): PureDecoder[B] =
    PureDecoder(
      ZPure.get[BitVector].flatMap { s0 =>
        val (log, result) = run.runAll(s0)
        result match {
          case Left(err) =>
            // Replay the (possibly partial) log so observers still see
            // everything that was emitted before the failure, then fail.
            val emitMapped: ZPure[B, BitVector, BitVector, Any, CodecError, Unit] =
              log.foldLeft(ZPure.unit[BitVector]: ZPure[B, BitVector, BitVector, Any, CodecError, Unit])(
                (acc, a) => acc *> ZPure.log[BitVector, B](f(a))
              )
            emitMapped *> ZPure.fail(err)
          case Right((s1, status)) =>
            val emitMapped: ZPure[B, BitVector, BitVector, Any, CodecError, Unit] =
              log.foldLeft(ZPure.unit[BitVector]: ZPure[B, BitVector, BitVector, Any, CodecError, Unit])(
                (acc, a) => acc *> ZPure.log[BitVector, B](f(a))
              )
            ZPure.set[BitVector](s1) *> emitMapped *> ZPure.succeed(status)
        }
      }
    )

  /**
   * Sequence two pure decoders. The leftover state from `this` is
   * the input state of `that`. Logs (i.e. emitted values) concatenate.
   */
  def ++[B >: A](that: => PureDecoder[B]): PureDecoder[B] =
    PureDecoder(
      // run self, then run that regardless of self's status
      self.run.flatMap(_ => that.run.asInstanceOf[ZPure[B, BitVector, BitVector, Any, CodecError, Status]])
    )

  /**
   * Convert this pure decoder into a streaming [[StreamDecoder]] by
   * lifting it through the `ZChannel`-based pipeline. Each upstream
   * chunk is appended to the carry buffer and the pure decoder is
   * re-`runAll`-ed; emitted log entries become a downstream chunk
   * and the resulting state becomes the new carry.
   */
  def toStreamDecoder: StreamDecoder[A] = StreamDecoder.fromPure(self)
}

object PureDecoder {

  /**
   * The result of a single pure decoding step. Modelled in the
   * success channel of `ZPure` so we don't waste a `ZPure.fail` on
   * the (very common) "ran out of bits, please call me again with
   * more" case.
   */
  sealed trait Status
  object Status {
    /** The decoder still wants more input bits to make progress. */
    case object NeedMore     extends Status
    /** The decoder has finished and should not be invoked again. */
    case object Done         extends Status
    /**
     * The decoder produced a value and is willing to be invoked
     * again on the leftover state. This is what `many` uses.
     */
    case object DoneTryAgain extends Status
  }

  /**
   * A *single* pure decoding step. This is what the legacy code
   * passed around as `BitVector => Attempt[DecodeResult[X]]` (and
   * what `scodec.Decoder[X]` essentially is). Both forms are exactly
   * the same thing as a `ZPure[W, BitVector, BitVector, Any,
   * CodecError, X]`: input state = buffer, output state = remainder,
   * error = decode error, success = the decoded value (or, when
   * `W = X` and we use the log, a stream of decoded values).
   *
   * Centralising this as a single named type lets every layer
   * (per-step decoders, `many`/`tryMany` loops, the `StreamDecoder`
   * interpreter) speak the same language without each one
   * re-inventing the same `BitVector => Attempt[…]` shape.
   */
  type DecoderStep[+A] = ZPure[Nothing, BitVector, BitVector, Any, CodecError, A]

  /**
   * Lift a `scodec.Decoder` into a [[DecoderStep]]. This is the
   * structural identity:
   *
   * {{{
   *   scodec.Decoder[A]         == BitVector => Attempt[DecodeResult[A]]
   *   PureDecoder.DecoderStep[A] == ZPure[Nothing, BitVector, BitVector, Any, CodecError, A]
   * }}}
   *
   * `Attempt.Failure(InsufficientBits)` is preserved as-is so the
   * streaming layer can recognise it and pull more bits.
   */
  def fromDecoder[A](decoder: Decoder[A]): DecoderStep[A] =
    ZPure.get[BitVector].flatMap { buffer =>
      decoder.decode(buffer) match {
        case Attempt.Successful(DecodeResult(a, remainder)) =>
          ZPure.set[BitVector](remainder) *> ZPure.succeed(a)
        case Attempt.Failure(err) =>
          ZPure.fail(CodecError(err))
      }
    }

  /** Run a `DecoderStep` strictly against an in-memory buffer. */
  def runStep[A](step: DecoderStep[A], bits: BitVector): Either[CodecError, DecodeResult[A]] =
    step.runAll(bits) match {
      case (_, Left(err))             => Left(err)
      case (_, Right((rem, value)))   => Right(DecodeResult(value, rem))
    }

  // -----------------------------------------------------------------
  // Tiny constructors
  // -----------------------------------------------------------------

  /** Decoder that produces nothing and signals it is done. */
  val done: PureDecoder[Nothing] = PureDecoder(ZPure.succeed(Status.Done))

  /** Decoder that produces nothing and signals it needs more bits. */
  val needMore: PureDecoder[Nothing] = PureDecoder(ZPure.succeed(Status.NeedMore))

  /** Decoder that fails immediately with the given scodec [[Err]]. */
  def fail(err: Err): PureDecoder[Nothing] = PureDecoder(ZPure.fail(CodecError(err)))

  /** Decoder that emits `a` and is done. */
  def emit[A](a: A): PureDecoder[A] =
    PureDecoder(ZPure.log[BitVector, A](a) *> ZPure.succeed(Status.Done))

  /** Decoder that emits all of `as` and is done. */
  def emits[A](as: Iterable[A]): PureDecoder[A] = {
    val emit: ZPure[A, BitVector, BitVector, Any, CodecError, Unit] =
      as.foldLeft(ZPure.unit[BitVector]: ZPure[A, BitVector, BitVector, Any, CodecError, Unit])(
        (acc, a) => acc *> ZPure.log[BitVector, A](a)
      )
    PureDecoder(emit *> ZPure.succeed(Status.Done))
  }

  /**
   * Decode exactly one `A` using the supplied scodec [[Decoder]].
   * Insufficient bits is reported via [[Status.NeedMore]] (no
   * failure). Other decode errors are reported via the error channel
   * if `failOnErr`, otherwise the decoder finishes with
   * [[Status.Done]] without emitting.
   */
  def once[A](decoder: Decoder[A], failOnErr: Boolean = true): PureDecoder[A] =
    decodeStep(decoder, repeat = false, failOnErr = failOnErr)

  /**
   * Repeatedly decode `A` values using the supplied scodec
   * [[Decoder]]. As long as the buffer can produce a value, this
   * decoder loops in pure-state and emits each one through the log.
   */
  def many[A](decoder: Decoder[A], failOnErr: Boolean = true): PureDecoder[A] =
    decodeStep(decoder, repeat = true, failOnErr = failOnErr)

  /** Alias for `many(decoder, failOnErr = false)`. */
  def tryMany[A](decoder: Decoder[A]): PureDecoder[A] = many(decoder, failOnErr = false)

  /** Alias for `once(decoder, failOnErr = false)`. */
  def tryOnce[A](decoder: Decoder[A]): PureDecoder[A] = once(decoder, failOnErr = false)

  // -------------------------------------------------------------------
  // Fast-path constructors
  //
  // The general-purpose `many(decoder)` path pays per-element cost
  // for the `ZPure.log` call and for the `BitVector.splitAt` that
  // happens inside `decoder.decode(buffer)`. When the codec is
  // byte-aligned and fixed-width (the common case for binary
  // formats - bytes, shorts, ints, packed records, etc.) we can
  // skip both costs entirely: read the underlying byte array once,
  // build a `Chunk[A]` directly via `Chunk.fromArray`, and ship the
  // whole batch through the log in one step.
  //
  // This lets the streaming layer hit the hand-written
  // `scodec.codecs.vector` baseline and frequently beat it (because
  // we build a `Chunk[Int]` instead of the boxed `Vector[Int]` that
  // `vector(uint8)` constructs).
  // -------------------------------------------------------------------

  /**
   * The general batch fast path: each invocation drains *all* the
   * complete bytes in the carry buffer and emits a single
   * `Chunk[A]` per step (instead of one log entry per element).
   * The caller supplies the per-batch decoder
   * `Array[Byte] => Chunk[A]`.
   *
   * `batch` is `inline` so the JIT sees the per-batch body
   * directly at the call site - no virtual dispatch, no method
   * call boundary.
   *
   * For `uint8` this looks like:
   * {{{
   *   PureDecoder.manyChunked[Int] { arr =>
   *     val out = new Array[Int](arr.length)
   *     var i = 0
   *     while (i < arr.length) { out(i) = arr(i) & 0xff; i += 1 }
   *     Chunk.fromArray(out)
   *   }
   * }}}
   */
  inline def manyChunked[A](
    inline batch: Array[Byte] => Chunk[A]
  ): PureDecoder[Chunk[A]] = {
    def step: ZPure[Chunk[A], BitVector, BitVector, Any, CodecError, Status] =
      ZPure.get[BitVector].flatMap { buffer =>
        val whole = buffer.size / 8L
        if (whole == 0L) ZPure.succeed(Status.NeedMore)
        else {
          val (used, rest) = buffer.splitAt(whole * 8L)
          val chunk        = batch(used.toByteArray)
          ZPure.set[BitVector](rest) *>
            ZPure.log[BitVector, Chunk[A]](chunk) *>
            ZPure.succeed(Status.NeedMore)
        }
      }
    PureDecoder(step)
  }

  // -----------------------------------------------------------------
  // Core ZPure step
  // -----------------------------------------------------------------

  /**
   * The shared decoding step, parameterised by whether we should
   * loop (`many` semantics) and whether non-`InsufficientBits`
   * failures should propagate.
   *
   * Inlines the `scodec.Attempt` match instead of [[fromDecoder]] + `foldM`:
   * the folded path added measurable overhead in tight loops (e.g.
   * `many(uint8)` over millions of values). [[fromDecoder]] stays the
   * canonical `DecoderStep` lift; behavior here matches it.
   */
  private def decodeStep[A](
    decoder: Decoder[A],
    repeat: Boolean,
    failOnErr: Boolean
  ): PureDecoder[A] = {

    def step: ZPure[A, BitVector, BitVector, Any, CodecError, Status] =
      ZPure.get[BitVector].flatMap { buffer =>
        decoder.decode(buffer) match {
          case Attempt.Successful(DecodeResult(value, remainder)) =>
            ZPure.set[BitVector](remainder) *>
              ZPure.log[BitVector, A](value) *>
              (if (repeat) step else ZPure.succeed[BitVector, Status](Status.Done))

          case Attempt.Failure(_: Err.InsufficientBits) =>
            ZPure.succeed[BitVector, Status](Status.NeedMore)

          case Attempt.Failure(comp: Err.Composite)
              if comp.errs.exists(_.isInstanceOf[Err.InsufficientBits]) =>
            ZPure.succeed[BitVector, Status](Status.NeedMore)

          case Attempt.Failure(e) =>
            if (failOnErr) ZPure.fail(CodecError(e))
            else ZPure.succeed[BitVector, Status](Status.Done)
        }
      }

    PureDecoder(step)
  }
}
