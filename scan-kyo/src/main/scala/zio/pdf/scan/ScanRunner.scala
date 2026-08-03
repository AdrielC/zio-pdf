/*
 * Wire up `Poll`, `Emit`, `Abort` for a `ScanProg.Of[I, O, E, S]`.
 *
 * Layer order matters:
 *
 *   1. `Abort.run`  innermost  -- catches `ScanSignal` into `Result.Fail`.
 *   2. `Emit.run`   middle     -- collects everything emitted *before*
 *                                  the abort fired, returning
 *                                  `(Chunk[O], Result[ScanSignal, Unit])`.
 *   3. `Poll.run`   outermost  -- drives the program by feeding inputs
 *                                  from a `Chunk`.
 *
 * When the program aborts, the `Emit` layer has already accumulated the
 * outputs. The leftover from the `ScanDone` payload is then appended,
 * giving the complete output sequence.
 */

package zio.pdf.scan

import kyo.*

object ScanRunner {

  /** Run a `ScanProg.Of[I, O, E, S]` against an iterable of inputs. Returns
    * the typed completion plus the full emitted-then-flushed output
    * sequence. */
  def run[I, O, E, S](inputs: Seq[I])(
      prog: ScanProg.Of[I, O, E, S]
  )(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): (ScanDone[O, E], Chunk[O]) < S =

    // Innermost: catch the abort.
    val withAbort: (Chunk[O], Result[ScanSignal, Unit]) < (Poll[I] & S) =
      Emit.run[O](Abort.run[ScanSignal](prog))

    val withPoll: (Chunk[O], Result[ScanSignal, Unit]) < S =
      Poll.run[I](Chunk.from(inputs))(withAbort)

    withPoll.map { case (emitted, result) =>
      // The Abort.run yielded a Result. Convert to typed ScanDone[O, E].
      result match {
        case Result.Success(_) =>
          // Program completed without aborting -- treat as clean success.
          (ScanDone.success[O], emitted)
        case Result.Failure(signal) =>
          val typed = signal.asInstanceOf[ScanDone[O, E]]
          val leftover = typed.leftoverSeq
          (typed, emitted.concat(Chunk.from(leftover)))
        case Result.Panic(thr) =>
          // Wrap an unexpected panic as a typed Failure so callers don't
          // have to deal with throwables. This is a tiny indulgence; if
          // the user wants the exception they can pattern-match the
          // ScanDone.Failure[O, Throwable] case.
          (
            ScanDone.failWith[O, E](thr.asInstanceOf[E], Seq.empty),
            emitted
          )
      }
    }
  end run

  /** Run a stateful scan, providing the initial state. The returned program
    * has the `Var[VS]` effect handled. */
  def runStateful[I, O, E, VS, S](inputs: Seq[I], seed: VS)(
      prog: ScanProg.Of[I, O, E, Var[VS] & S]
  )(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      varTag: Tag[Var[VS]],
      frame: Frame
  ): (ScanDone[O, E], Chunk[O]) < S =
    run[I, O, E, S](inputs)(Var.run(seed)(prog))
}
