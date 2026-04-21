/*
 * The unified scan abstraction.
 *
 * ------------------------------------------------------------------
 * Shape
 * ------------------------------------------------------------------
 *
 *   trait Scan[-I, +R, +O, +E] {
 *     type S
 *     def init: S < R
 *     def step[EE](in: I < EE)
 *       : Unit < (Var[S] & Emit[O] & Abort[E] & R & EE)
 *     def end:       Unit < (Var[S] & Emit[O] & Abort[E] & R)
 *   }
 *
 * Every capability a scan needs is tracked in the signature:
 *
 *   - `R`       -- ambient effects required to initialise / run the scan.
 *                  Pure `arr f` has `R = Any`. A chunker with an off-
 *                  heap buffer has `R = Scope`. A sniff-and-decompress
 *                  step has `R = Sync & Scope & Abort[SniffError]`.
 *   - `Var[S]`  -- per-scan mutable state, hidden behind `type S`.
 *   - `Emit[O]` -- outputs. A step may call `Emit.value` zero, one, or
 *                  many times per input. Cardinality is the scan's own
 *                  business; it is *also* reflected statically by the
 *                  `ScanPrim[I, StepOut[O]]` tag so the fusion pass can
 *                  see through `One`-emitting stages.
 *   - `Abort[E]`-- typed completion / failure. Early stop (`take(n)`)
 *                  and `Success(leftover)` ride on this channel via
 *                  `ScanSignal`.
 *   - `EE`      -- the *caller's* effect row. Because the input is an
 *                  effectful value `I < EE`, a pure `Int`-consuming scan
 *                  widens automatically to accept `Int < (Sync & Abort[X])`;
 *                  Kyo variance does the work.
 *
 * This is the unified shape the rest of the pipeline machinery targets.
 * Core effects are `Var`, `Emit`, `Poll`, `Abort` -- exactly the four
 * capabilities a chunk-native streaming pipe needs.
 */

package zio.pdf.scan

import kyo.*

// =====================================================================
// The trait
// =====================================================================

/** The unified scan shape. One per-stage object; state is internal.
  *
  * Variance notes (Kyo `<[+A, -S]` is contravariant in its effect row,
  * so capability-like parameters must also be contravariant):
  *
  *   - `-I` : a scan accepting a wider input type can serve a caller
  *            with a narrower one.
  *   - `-R` : a scan needing *fewer* ambient effects is a subtype of
  *            one needing more. `Scan[I, Any, O, E] <: Scan[I, Sync, O, E]`.
  *   - `+O` : a scan emitting a narrower output is a subtype of one
  *            emitting a wider one.
  *   - `+E` : a scan failing with a narrower error is a subtype of one
  *            failing with a wider one.
  */
trait Scan[-I, -R, +O, +E] extends Serializable { self =>

  /** Per-scan mutable state, hidden. Runners materialise it via `init`
    * and carry it in a `Var[S]` for the scan's lifetime. */
  type S

  /** Tag used to handle `Var[S]` at the runner boundary. Implementations
    * return the tag built with their concrete `S`; callers never see `S`. */
  def stateTag: Tag[Var[S]]

  /** Build the initial state. Allowed to request ambient effects `R`. */
  def init(using Frame): S < R

  /** Feed one input. Input is a Kyo-pending value so upstream effects
    * ride through without the scan having to know about them. The scan
    * may `Emit.value` zero, one, or many times. */
  def step[EE](in: I < EE)(using Frame)
      : Unit < (Var[S] & Emit[O] & Abort[E] & R & EE)

  /** End-of-stream flush. Same capability row as `step` minus `EE`. */
  def end(using Frame): Unit < (Var[S] & Emit[O] & Abort[E] & R)
}

object Scan {

  // ------------------------------------------------------------------
  // Effect-row alias used throughout the runtime.
  // ------------------------------------------------------------------

  /** The capability row of a fully-assembled scan before the caller
    * threads their own effects `EE` into `step`. */
  type Caps[S, O, E, R] = Var[S] & Emit[O] & Abort[E] & R

  // ------------------------------------------------------------------
  // Smart constructors
  // ------------------------------------------------------------------

  /** Build a scan from an explicit `State` plus step/end bodies. The
    * `State` leaks at the constructor but not in the returned type -- the
    * path-dependent member `type S = State` hides it. */
  def make[I, R, O, E, State](
      initial: State < R,
      stepFn: [EE] => (I < EE, Frame) => Unit < (Var[State] & Emit[O] & Abort[E] & R & EE),
      endFn: Frame => Unit < (Var[State] & Emit[O] & Abort[E] & R)
  )(using stateT: Tag[Var[State]]): Scan[I, R, O, E] { type S = State } =
    new Scan[I, R, O, E] {
      type S = State
      def stateTag: Tag[Var[S]]                                       = stateT
      def init(using Frame): S < R                                    = initial
      def step[EE](in: I < EE)(using fr: Frame)
          : Unit < (Var[S] & Emit[O] & Abort[E] & R & EE)             = stepFn[EE](in, fr)
      def end(using fr: Frame): Unit < (Var[S] & Emit[O] & Abort[E] & R) = endFn(fr)
    }

  /** Build a stateless scan. `S = Unit` so Kyo's `Var[Unit]` elides at
    * runtime. */
  def stateless[I, R, O, E](
      stepFn: [EE] => (I < EE, Frame) => Unit < (Emit[O] & Abort[E] & R & EE),
      endFn: Frame => Unit < (Emit[O] & Abort[E] & R) = (_: Frame) => (): Unit < Any
  )(using stateT: Tag[Var[Unit]]): Scan[I, R, O, E] { type S = Unit } =
    new Scan[I, R, O, E] {
      type S = Unit
      def stateTag: Tag[Var[S]]    = stateT
      def init(using Frame): S < R = (): Unit
      def step[EE](in: I < EE)(using fr: Frame)
          : Unit < (Var[S] & Emit[O] & Abort[E] & R & EE) =
        stepFn[EE](in, fr)
      def end(using fr: Frame): Unit < (Var[S] & Emit[O] & Abort[E] & R) =
        endFn(fr)
    }

  // ------------------------------------------------------------------
  // Pure-function lifts. These are the base cases for `Arr` / `Map`.
  // ------------------------------------------------------------------

  /** Lift a pure `I => O` into a one-emitting stateless `Scan`. */
  def liftArr[I, O](f: I => O)(using
      emitTag: Tag[Emit[O]],
      stateT: Tag[Var[Unit]]
  ): Scan[I, Any, O, Nothing] { type S = Unit } =
    stateless[I, Any, O, Nothing](
      stepFn = [EE] => (in: I < EE, fr: Frame) => {
        given Frame = fr
        in.map(i => Emit.value(f(i)))
      }
    )

  /** Lift an effectful `I => O < R1`. The scan's `R` grows by `R1`. */
  def liftArrEff[I, R1, O](f: I => O < R1)(using
      emitTag: Tag[Emit[O]],
      stateT: Tag[Var[Unit]]
  ): Scan[I, R1, O, Nothing] { type S = Unit } =
    stateless[I, R1, O, Nothing](
      stepFn = [EE] => (in: I < EE, fr: Frame) => {
        given Frame = fr
        in.map(i => f(i).map(o => Emit.value(o)))
      }
    )

  /** Lift an `I => Maybe[O]` filter into a zero-or-one-emitting Scan. */
  def liftOpt[I, O](f: I => Maybe[O])(using
      emitTag: Tag[Emit[O]],
      stateT: Tag[Var[Unit]]
  ): Scan[I, Any, O, Nothing] { type S = Unit } =
    stateless[I, Any, O, Nothing](
      stepFn = [EE] => (in: I < EE, fr: Frame) => {
        given Frame = fr
        in.map(i =>
          f(i) match {
            case Present(o) => Emit.value(o)
            case Absent     => (): Unit
          }
        )
      }
    )

  /** Lift a pure batch function `Chunk[I] => Chunk[O]`. A single input is
    * wrapped and sent through; a batch is processed in one call. Emits
    * the whole output chunk in one `Emit.value` (Kyo's `Emit[O]` is a
    * per-element stream, so we foreach). */
  def liftChunk[I, O](f: Chunk[I] => Chunk[O])(using
      emitTag: Tag[Emit[O]],
      stateT: Tag[Var[Unit]]
  ): Scan[I, Any, O, Nothing] { type S = Unit } =
    stateless[I, Any, O, Nothing](
      stepFn = [EE] => (in: I < EE, fr: Frame) => {
        given Frame = fr
        in.map(i => Kyo.foreachDiscard(f(Chunk(i)))(o => Emit.value(o)))
      }
    )

  /** The identity scan. */
  def identityScan[A](using
      emitTag: Tag[Emit[A]],
      stateT: Tag[Var[Unit]]
  ): Scan[A, Any, A, Nothing] { type S = Unit } =
    liftArr[A, A](a => a)

  // ===================================================================
  // Legacy façade (retained).
  //
  // The rest of the module builds `FreeScan[I, O]` values, which are the
  // describable-as-data surface. `FreeScan.compile` lowers them into
  // `Scan[I, R, O, E]` instances of this trait. These helpers keep the
  // existing call-sites (`Scan.map`, `Scan.fastCdc`, ...) working and
  // will be migrated in follow-up commits as more primitives grow
  // honest capability types.
  // ===================================================================

  /** Build a single-primitive scan. */
  def lift[I, O](p: ScanPrim[I, StepOut.StepOut[O]]): FreeScan[I, O] = FreeScan.lift(p)

  /** A pure-function scan. */
  def arr[I, O](f: I => O): FreeScan[I, O] = FreeScan.arr(f)

  /** The identity FreeScan. */
  def id[A]: FreeScan[A, A] = FreeScan.id[A]

  def map[I, O](f: I => O): FreeScan[I, O]                   = FreeScan.lift(ScanPrim.Map(f))
  def filter[A](p: A => Boolean): FreeScan[A, A]             = FreeScan.lift(ScanPrim.Filter(p))
  def take[A](n: Int): FreeScan[A, A]                        = FreeScan.lift(ScanPrim.Take(n))
  def drop[A](n: Int): FreeScan[A, A]                        = FreeScan.lift(ScanPrim.Drop(n))
  def fold[I, St](seed: St)(step: (St, I) => St): FreeScan[I, St] =
    FreeScan.lift(ScanPrim.Fold(seed, step))
  def hash(algo: HashAlgo): FreeScan[Byte, Byte]             = FreeScan.lift(ScanPrim.Hash(algo))
  def countBytes: FreeScan[Byte, Byte]                       = FreeScan.lift(ScanPrim.CountBytes)
  def bombGuard(maxBytes: Long): FreeScan[Byte, Byte]        = FreeScan.lift(ScanPrim.BombGuard(maxBytes))
  def fastCdc(min: Int, avg: Int, max: Int): FreeScan[Byte, Chunk[Byte]] =
    FreeScan.lift(ScanPrim.FastCDC(min, avg, max))
  def fixedChunk(n: Int): FreeScan[Byte, Chunk[Byte]] =
    FreeScan.lift(ScanPrim.FixedChunk(n))

  def swap[A, B]: FreeScan[(A, B), (B, A)]                  = FreeScan.swap
  def mirror[A, B]: FreeScan[Either[A, B], Either[B, A]]    = FreeScan.mirror
  def diag[A]: FreeScan[A, (A, A)]                          = FreeScan.diag
  def merge[A]: FreeScan[Either[A, A], A]                   = FreeScan.merge
  def injectLeft[A, B]: FreeScan[A, Either[A, B]]           = FreeScan.injectLeft
  def injectRight[A, B]: FreeScan[B, Either[A, B]]          = FreeScan.injectRight
  def test[A, B](p: A => Boolean)(yes: FreeScan[A, B])(no: FreeScan[A, B]): FreeScan[A, B] =
    FreeScan.test(p)(yes)(no)
  def const[A, B](b: B): FreeScan[A, B]                     = FreeScan.const(b)
  def fst[A, B]: FreeScan[(A, B), A]                        = FreeScan.fst
  def snd[A, B]: FreeScan[(A, B), B]                        = FreeScan.snd
  def void[A, B](self: FreeScan[A, B]): FreeScan[A, Unit]   = FreeScan.void(self)

  // --- Runners: delegate to the legacy single-pass interpreter for now ---

  /** Pure synchronous driver. */
  def runDirect[I, O, E](scan: FreeScan[I, O], inputs: Iterable[I]): (ScanDone[O, E], Vector[O]) =
    SinglePassInterp.runDirect(scan, inputs)

  /** Kyo-effect-typed runner. */
  def runKyo[I, O, E](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): (ScanDone[O, E], Chunk[O]) < Any = {
    val prog: ScanProg.Of[I, O, E, Any] =
      Fusion.tryFuse(scan) match {
        case Some(f) => fusedFnToProg(f)
        case None    => stepperToProg(SinglePassInterp.compile(scan))
      }
    ScanRunner.run[I, O, E, Any](inputs)(prog)
  }

  private[scan] def fusedFnToProg[I, O](f: I => O)(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): ScanProg.Of[I, O, Nothing, Any] =
    Loop.foreach {
      Poll.andMap[I] {
        case Absent =>
          Abort.fail[ScanSignal](ScanDone.success[O]).andThen(Loop.done)
        case Present(i) =>
          Emit.value(f(i)).andThen(Loop.continue)
      }
    }

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
