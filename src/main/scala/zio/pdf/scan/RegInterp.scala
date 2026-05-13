/*
 * Register-based interpreter for `FreeScan`.
 *
 * Scope:
 *
 *   1. `Fusion.tryFuse` first -- if the spine is purely `Arr`/`Prim(Map)`,
 *      the fast path is unchanged (one `f(_)` per input, no stepper).
 *      RegStepper does not improve this case; the legacy `runDirect`
 *      already inlines.
 *
 *   2. Otherwise flatten the spine. If every node is one of the
 *      "spine primitives" handled by `RegSteppers` (Map / Filter /
 *      Take / Drop / Fold / Hash / CountBytes / BombGuard /
 *      FixedChunk / FastCDC, plus pure `Arr`) we lower to a chain of
 *      `RegSteppers.AndThen` and drive it with `runOnce`.
 *
 *   3. If the spine contains a Fanout/Choice we *fall back* to the
 *      legacy `SinglePassInterp.runDirect`. Those two shapes have a
 *      different output protocol (tuple-of-outputs / Either-routing)
 *      and don't benefit from register state without a tuple-aware
 *      output buffer -- a follow-up.
 *
 * Schema-driven layout:
 *
 *   Direct callers that build a `RegSteppers.Fold[I, S]` themselves
 *   (or use the `RegInterp.runFoldUnboxed` helper) get the layout
 *   from `RegSchema[S]` -- primitive `S` lives in a long slot,
 *   reference `S` in an object slot, *no boxing per step*. The
 *   `FreeScan` AST erases `S` inside `ScanPrim.Fold`, so the lowering
 *   path goes through the erased-object schema and matches the
 *   legacy behaviour; the unboxed fast path is reachable from the
 *   register API directly.
 */

package zio.pdf.scan

import StepOut.StepOut

object RegInterp {

  /** Run a typed fold through the register lane, choosing a
    * **specialised** stepper at compile time when the accumulator
    * type is primitive. The selection is done by an `inline match` on
    * `S`:
    *
    *   - `S = Long`   -> `RegSteppers.FoldLong[I]`   (long-slot, no box)
    *   - `S = Int`    -> `RegSteppers.FoldInt[I]`    (long-slot, no box)
    *   - `S = Double` -> `RegSteppers.FoldDouble[I]` (long-slot, no box)
    *   - default      -> `RegSteppers.Fold[I, S]`    (object slot, falls
    *                                                  back to the typeclass)
    *
    * This is the compile-time-decisioning entry point: the spec
    * picks a monomorphic class for primitive `S` so the JIT sees
    * direct `getLong`/`setLong` calls on the hot path, with no
    * typeclass dispatch at all. */
  inline def runFoldUnboxed[I, S](
      seed:   S,
      f:      (S, I) => S,
      inputs: Iterable[I]
  )(using rs: RegSchema[S]): (ScanDone[S, Any], Vector[S]) = {
    import scala.compiletime.erasedValue
    val stepper: RegStepper[I, S, Nothing] = inline erasedValue[S] match {
      case _: Long   =>
        new RegSteppers.FoldLong[I](
          seed.asInstanceOf[Long],
          f.asInstanceOf[(Long, I) => Long]
        ).asInstanceOf[RegStepper[I, S, Nothing]]
      case _: Int    =>
        new RegSteppers.FoldInt[I](
          seed.asInstanceOf[Int],
          f.asInstanceOf[(Int, I) => Int]
        ).asInstanceOf[RegStepper[I, S, Nothing]]
      case _: Double =>
        new RegSteppers.FoldDouble[I](
          seed.asInstanceOf[Double],
          f.asInstanceOf[(Double, I) => Double]
        ).asInstanceOf[RegStepper[I, S, Nothing]]
      case _         =>
        RegSteppers.Fold[I, S](seed, f)(using rs)
    }
    runStepper[I, S, Any](stepper, inputs)
  }

  /** Drive an already-compiled `RegStepper` against an iterable. Public
    * so callers that hand-roll a stepper (e.g. tests, benchmarks) can
    * exercise the register lane without going through `FreeScan`. */
  def runStepper[I, O, E](
      stepper: RegStepper[I, O, E],
      inputs:  Iterable[I]
  ): (ScanDone[O, E], Vector[O]) =
    runOnce[I, O, E](stepper, inputs)

  /** Run a `FreeScan` through the register-based stepper when possible.
    *
    * Returns the same `(ScanDone, Vector[O])` shape as
    * `SinglePassInterp.runDirect` so it is a drop-in alternative.
    */
  def runDirect[I, O, E](
      scan: FreeScan[I, O],
      inputs: Iterable[I]
  ): (ScanDone[O, E], Vector[O]) =
    Fusion.tryFuse(scan) match {
      case Some(f) =>
        val builder = Vector.newBuilder[O]
        inputs match {
          case ix: IndexedSeq[I] @unchecked => builder.sizeHint(ix.size)
          case _                            => ()
        }
        val it = inputs.iterator
        while it.hasNext do builder += f(it.next())
        (
          ScanDone.success[O].asInstanceOf[ScanDone[O, E]],
          builder.result()
        )

      case None =>
        compileSpine(scan) match {
          case Some(stepper) =>
            runOnce[I, O, E](stepper.asInstanceOf[RegStepper[I, O, E]], inputs)
          case None =>
            SinglePassInterp.runDirect[I, O, E](scan, inputs)
        }
    }

  /** Drive a compiled `RegStepper` against an iterable of inputs. */
  private def runOnce[I, O, E](
      stepper: RegStepper[I, O, E],
      inputs:  Iterable[I]
  ): (ScanDone[O, E], Vector[O]) = {
    val regs = new RegState(
      initialLongs   = math.max(stepper.layout.longs,   1),
      initialObjects = math.max(stepper.layout.objects, 1)
    )
    regs.ensureCapacity(stepper.layout.longs, stepper.layout.objects)
    stepper.init(regs, RegOff.Zero)

    val out      = new RegOutBuffer(64)
    val emitted  = Vector.newBuilder[O]
    inputs match {
      case ix: IndexedSeq[I] @unchecked => emitted.sizeHint(ix.size)
      case _                            => ()
    }

    var sig = RegSignal.Continue
    val it  = inputs.iterator
    while sig == RegSignal.Continue && it.hasNext do {
      out.clear()
      sig = stepper.step(regs, RegOff.Zero, it.next(), out)
      out.drainInto[O](emitted)
    }
    if sig == RegSignal.Continue then {
      out.clear()
      sig = stepper.end(regs, RegOff.Zero, out)
      out.drainInto[O](emitted)
    }

    val leftover = stepper.leftover(regs, RegOff.Zero)
    val signal: ScanDone[O, E] = sig match {
      case RegSignal.Stop    =>
        ScanDone.stopWith[O](leftover).asInstanceOf[ScanDone[O, E]]
      case RegSignal.Failure =>
        stepper.failure(regs, RegOff.Zero) match {
          case Some(e) => ScanDone.failWith[O, E](e, leftover)
          case None    =>
            // Should not happen if the primitive cooperates; preserve
            // shape with a generic error to avoid a crash on the hot path.
            ScanDone
              .failWith[O, E](
                new RuntimeException("RegStepper.Failure with no payload").asInstanceOf[E],
                leftover
              )
        }
      case _                 =>
        ScanDone.successWith[O](leftover).asInstanceOf[ScanDone[O, E]]
    }
    val all = emitted.result() ++ leftover.toVector
    (signal, all)
  }

  /** Try to compile `scan` as a RegStepper spine.
    *
    * Returns `None` when any node is `Fanout`/`Choice` (those route
    * through the legacy interpreter for now).
    */
  private def compileSpine[I, O](
      scan: FreeScan[I, O]
  ): Option[RegStepper[I, O, Any]] = {
    val nodes = SinglePassInterp.flattenSpine(scan)
    val buf   = scala.collection.mutable.ArrayBuffer.empty[RegStepper[Any, Any, Any]]
    var i     = 0
    while i < nodes.length do {
      compileNode(nodes(i)) match {
        case Some(s) => buf += s
        case None    => return None
      }
      i += 1
    }
    if buf.isEmpty then None
    else if buf.length == 1 then Some(buf(0).asInstanceOf[RegStepper[I, O, Any]])
    else {
      var acc: RegStepper[Any, Any, Any] = buf(0)
      var k = 1
      while k < buf.length do {
        acc = new RegSteppers.AndThen[Any, Any, Any, Any, Any](acc, buf(k))
          .asInstanceOf[RegStepper[Any, Any, Any]]
        k += 1
      }
      Some(acc.asInstanceOf[RegStepper[I, O, Any]])
    }
  }

  private def compileNode(
      node: FreeScan[Any, Any]
  ): Option[RegStepper[Any, Any, Any]] = {
    val widened: FreeScan[?, ?] = node
    widened match {
      case FreeScan.Arr(f) =>
        Some(new RegSteppers.Pure(f).asInstanceOf[RegStepper[Any, Any, Any]])

      case FreeScan.Prim(p) =>
        compilePrim(p.asInstanceOf[ScanPrim[Any, StepOut[Any]]])

      case FreeScan.Fanout(_, _) | FreeScan.Choice(_, _) =>
        // Not yet handled by the register lane.
        None

      case FreeScan.AndThen(_, _) =>
        sys.error("unreachable: AndThen survived flattenSpine")
    }
  }

  private def compilePrim[I, O](
      p: ScanPrim[I, StepOut[O]]
  ): Option[RegStepper[Any, Any, Any]] = p match {
    case ScanPrim.Map(f) =>
      Some(new RegSteppers.Pure(f.asInstanceOf[Any => Any])
        .asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.Filter(pred) =>
      Some(new RegSteppers.Filter(pred.asInstanceOf[Any => Boolean])
        .asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.Take(n) =>
      Some(new RegSteppers.Take[Any](n).asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.Drop(n) =>
      Some(new RegSteppers.Drop[Any](n).asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.Fold(seed, step) =>
      // At this lowering boundary the static `S` has been erased, so
      // pick the erased object-slot schema -- the legacy behaviour
      // for a fully erased fold. Call sites that want primitive
      // unboxing build the RegStepper directly via
      // `RegSteppers.Fold(seed, step)` with `RegSchema[S]` in scope.
      Some(
        RegSteppers.Fold
          .erased[Any, Any](
            seed.asInstanceOf[Any],
            step.asInstanceOf[(Any, Any) => Any]
          )
          .asInstanceOf[RegStepper[Any, Any, Any]]
      )

    case ScanPrim.Hash(algo) =>
      Some(new RegSteppers.Hash(algo).asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.CountBytes =>
      Some(new RegSteppers.CountBytes().asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.BombGuard(max) =>
      Some(new RegSteppers.BombGuard(max).asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.FixedChunk(n) =>
      Some(new RegSteppers.FixedChunk(n).asInstanceOf[RegStepper[Any, Any, Any]])

    case ScanPrim.FastCDC(min, avg, max) =>
      Some(new RegSteppers.FastCDC(min, avg, max).asInstanceOf[RegStepper[Any, Any, Any]])
  }
}
