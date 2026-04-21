/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 *
 * Bridge from the pure world (zio-prelude.ZPure) to the streaming
 * world (zio.stream.ZChannel) for the *very* common pattern of
 * "for every input element, run a pure state-step that may emit
 * any number of outputs through the log channel".
 *
 * Architecturally this sits next to `StreamDecoder.fromPure`: that
 * one runs a single ZPure decoder against an accumulating BitVector
 * carry; this one runs a per-element ZPure step. Both share the
 * principle that the ZPure layer owns the pure state-and-log work
 * and the ZChannel layer just owns the I/O plumbing.
 */

package zio.scodec.stream

import zio.{Cause, Chunk, ZIO}
import zio.prelude.fx.ZPure
import zio.stream.{ZChannel, ZPipeline}

object StatefulPipe {

  /**
   * A pure step over an input `In`: takes the current state `S`,
   * may emit any number of outputs `Out` via the log channel, and
   * returns the next state. Errors go through `Throwable`.
   *
   * Structurally identical to `In => S => (Chunk[Out], Either[Throwable, S])`.
   */
  type Step[-In, S, +Out] =
    In => ZPure[Out, S, S, Any, Throwable, Unit]

  /**
   * Lift a per-element pure step into a `ZPipeline[In, Out]`.
   * Threads the state through the whole stream; at end-of-stream
   * runs the (also-pure) `finalize` step to emit any trailing
   * outputs (a Meta record, a generated xref, ...).
   *
   * Use the [[applyEffect]] overload when you genuinely need a
   * side-effecting hook at end-of-stream (e.g. logging). Pure
   * pipelines should always reach for this overload.
   */
  def apply[In, S, Out](
    initial: S,
    finalize: S => ZPure[Out, S, S, Any, Throwable, Unit] = (_: S) => ZPure.unit[S]
  )(step: Step[In, S, Out]): ZPipeline[Any, Throwable, In, Out] =
    applyEffect[In, S, Out](initial, finalize, _ => ZIO.unit)(step)

  /**
   * Same as [[apply]] but takes an additional `onDone: S => ZIO`
   * effect that runs once upstream is exhausted - *after* the pure
   * `finalize` has emitted its outputs. The pure layer stays pure;
   * the effect lives in ZIO where it belongs.
   */
  def applyEffect[In, S, Out](
    initial: S,
    finalize: S => ZPure[Out, S, S, Any, Throwable, Unit] = (_: S) => ZPure.unit[S],
    onDone: S => ZIO[Any, Throwable, Unit] = (_: S) => ZIO.unit
  )(step: Step[In, S, Out]): ZPipeline[Any, Throwable, In, Out] = {

    def loop(state: S): ZChannel[Any, Throwable, Chunk[In], Any, Throwable, Chunk[Out], S] =
      ZChannel.readWithCause[Any, Throwable, Chunk[In], Any, Throwable, Chunk[Out], S](
        (chunk: Chunk[In]) => {
          // Fold the whole input chunk through the per-element ZPure,
          // collecting every emitted log entry into a single downstream
          // chunk. `runAll(state)` propagates failures via Either so
          // they surface through the channel error channel below.
          val run = chunk.foldLeft[ZPure[Out, S, S, Any, Throwable, Unit]](ZPure.unit) {
            (acc, in) => acc *> step(in)
          }
          val (log, result) = run.runAll(state)
          result match {
            case Left(err) =>
              // Flush any partial log before failing.
              val emit: ZChannel[Any, Throwable, Any, Any, Throwable, Chunk[Out], Unit] =
                if (log.isEmpty) ZChannel.unit else ZChannel.write(log)
              emit *> ZChannel.fail(err)
            case Right((next, _)) =>
              if (log.isEmpty) loop(next)
              else ZChannel.write(log) *> loop(next)
          }
        },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => ZChannel.succeed(state)
      )

    val withFinal: ZChannel[Any, Throwable, Chunk[In], Any, Throwable, Chunk[Out], Unit] =
      loop(initial).flatMap { finalState =>
        val (log, result) = finalize(finalState).runAll(finalState)
        val emit: ZChannel[Any, Any, Any, Any, Throwable, Chunk[Out], Unit] =
          if (log.isEmpty) ZChannel.unit else ZChannel.write(log)
        result match {
          case Left(err) => emit *> ZChannel.fail(err)
          case Right(_)  => emit *> ZChannel.fromZIO(onDone(finalState)).unit
        }
      }

    ZPipeline.fromChannel(withFinal)
  }
}
