/*
 * Public façade. The two interesting entry points:
 *
 *   - `Scan.runDirect(scan, inputs)` -- pure, no Kyo dependency. Drives
 *     a compiled stepper directly. Useful in tests and for environments
 *     that don't want the Kyo runtime.
 *
 *   - `Scan.runKyo(scan, inputs)`    -- lowers a `FreeScan[I, O]` into
 *     a `Unit < (Poll[I] & Emit[O] & Abort[ScanSignal])` and runs it
 *     against the supplied inputs. Returns
 *     `(ScanDone[O, Any], Chunk[O]) < Any` -- a pure value once the
 *     Kyo computation is evaluated.
 *
 * Either way the underlying execution model is the same: spine flatten,
 * fuse pure stages, run the surviving stepper. The Kyo path adds the
 * standard Poll/Emit/Abort effect row so the scan can be composed with
 * other Kyo effects (e.g. `Sync` for actually performing IO inside a
 * primitive interpretation).
 */

package zio.pdf.scan

import kyo.*

object Scan {

  /** Build a single-primitive scan. */
  def lift[I, O](p: ScanPrim[I, StepOut.StepOut[O]]): FreeScan[I, O] = FreeScan.lift(p)

  /** A pure-function scan. */
  def arr[I, O](f: I => O): FreeScan[I, O] = FreeScan.arr(f)

  /** The identity scan. */
  def id[A]: FreeScan[A, A] = FreeScan.id[A]

  // -------- Primitive helpers --------

  def map[I, O](f: I => O): FreeScan[I, O]                    = FreeScan.lift(ScanPrim.Map(f))
  def filter[A](p: A => Boolean): FreeScan[A, A]              = FreeScan.lift(ScanPrim.Filter(p))
  def take[A](n: Int): FreeScan[A, A]                         = FreeScan.lift(ScanPrim.Take(n))
  def drop[A](n: Int): FreeScan[A, A]                         = FreeScan.lift(ScanPrim.Drop(n))
  def fold[I, S](seed: S)(step: (S, I) => S): FreeScan[I, S]  =
    FreeScan.lift(ScanPrim.Fold(seed, step))
  def hash(algo: HashAlgo): FreeScan[Byte, Byte]              = FreeScan.lift(ScanPrim.Hash(algo))
  def countBytes: FreeScan[Byte, Byte]                        = FreeScan.lift(ScanPrim.CountBytes)
  def bombGuard(maxBytes: Long): FreeScan[Byte, Byte]         = FreeScan.lift(ScanPrim.BombGuard(maxBytes))
  /** Content-defined chunker. Each emitted unit is a `Chunk[Byte]`. */
  def fastCdc(min: Int, avg: Int, max: Int): FreeScan[Byte, Chunk[Byte]] =
    FreeScan.lift(ScanPrim.FastCDC(min, avg, max))

  /** Fixed-size chunker. Each emitted unit is a `Chunk[Byte]`. */
  def fixedChunk(n: Int): FreeScan[Byte, Chunk[Byte]] =
    FreeScan.lift(ScanPrim.FixedChunk(n))

  // -------- Runners --------

  /** Pure synchronous driver. */
  def runDirect[I, O, E](scan: FreeScan[I, O], inputs: Iterable[I]): (ScanDone[O, E], Vector[O]) =
    SinglePassInterp.runDirect(scan, inputs)

  /** Kyo-effect-typed runner. The returned computation requires no
    * additional effects -- all of Poll/Emit/Abort are handled inside, the
    * underlying stepper is pure, and only the Kyo machinery for
    * suspension/resumption shows up in the type. */
  def runKyo[I, O, E](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): (ScanDone[O, E], Chunk[O]) < Any = {
    val prog: ScanProg.Of[I, O, E, Any] = stepperToProg(SinglePassInterp.compile(scan))
    ScanRunner.run[I, O, E, Any](inputs)(prog)
  }

  /** Lower a `Stepper` into a `ScanProg.Of`. The driver loop polls one
    * input at a time; when the stepper signals `done`, the abort fires
    * with that signal. Trailing emits arising from `Stepper.end` are
    * pushed before the abort. */
  private[scan] def stepperToProg[I, O, E](
      initial: Stepper[I, O, E]
  )(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): ScanProg.Of[I, O, E, Any] = {
    Loop(initial: Stepper[I, O, E]) { (current: Stepper[I, O, E]) =>
      Poll.andMap[I] {
        case Absent =>
          // End-of-stream -- drain the stepper.
          val eff = current.end
          val emitAll: Unit < Emit[O] =
            Kyo.foreachDiscard(eff.out)(o => Emit.value(o))
          val signal: ScanDone[O, E] = eff.done.getOrElse(ScanDone.success[O])
          emitAll.andThen(
            Abort.fail[ScanSignal](signal).andThen(Loop.done(current))
          )
        case Present(i) =>
          val (eff, next) = current.step(i)
          val emitAll: Unit < Emit[O] =
            Kyo.foreachDiscard(eff.out)(o => Emit.value(o))
          eff.done match {
            case None =>
              emitAll.andThen(Loop.continue(next))
            case Some(sig) =>
              emitAll.andThen(
                Abort.fail[ScanSignal](sig).andThen(Loop.done(next))
              )
          }
      }
    }.unit
  }
}
