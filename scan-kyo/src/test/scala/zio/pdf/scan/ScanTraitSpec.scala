/*
 * Shape-level tests for the unified `Scan` trait.
 *
 * The point is not per-element correctness (that's covered by
 * `ScanSpec` / `AdvancedCompositionSpec` / `ArrowKitchenSinkSpec` on
 * the `FreeScan` side). The point is to *show on the type* that:
 *
 *   1. capability types are tracked in the input and output types
 *      (`R` on the scan itself, `EE` on the caller via `I < EE`);
 *   2. a pure `Int => Int` lifts into a `Scan[Int, Any, Int, Nothing]`
 *      that can be fed from `Int < Sync`, `Int < Abort[X]`, etc.
 *      without any adaptation at the scan site;
 *   3. stateful scans own their `State` as a path-dependent member,
 *      so no caller ever has to spell out the state type;
 *   4. the core effect set is exactly `Var[S] & Emit[O] & Abort[E]`
 *      (+ `R` + `EE`) -- no stray `Poll` leaking into step bodies, no
 *      bespoke per-stage wrappers.
 *
 * The tests run the scans by handling the effect row end-to-end. If any
 * of the above invariants break, this file stops compiling.
 */

package zio.pdf.scan

import kyo.{Result as KResult, *}
import kyo.AllowUnsafe.embrace.danger
import zio.test.*

object ScanTraitSpec extends ZIOSpecDefault {

  // ---------- Helpers: run a scan against an iterable, collect outputs. ----

  /** Drive a scan step-by-step. All effects (`Var[S]`, `Emit[O]`,
    * `Abort[ScanSignal]`, and the scan's own `R`) are handled here so
    * the returned value has no residual capability. */
  private def runCollect[I, O, E, R](
      scan: Scan[I, R, O, E],
      inputs: Seq[I]
  )(using
      emitTag: Tag[Emit[O]],
      abortTag: Tag[Abort[E]],
      abortCT: ConcreteTag[E],
      frame: Frame
  ): (Chunk[O], KResult[E, Unit]) < R = {
    given Tag[Var[scan.S]] = scan.stateTag

    val body: Unit < (Var[scan.S] & Emit[O] & Abort[E] & R) =
      inputs.foldLeft[Unit < (Var[scan.S] & Emit[O] & Abort[E] & R)]((): Unit) { (acc, i) =>
        acc.andThen(scan.step[Any](i))
      }.andThen(scan.end)

    // Handle (from innermost to outermost): Abort, Emit, Var.
    val withAbort: KResult[E, Unit] < (Var[scan.S] & Emit[O] & R) =
      Abort.run[E](body)
    val withEmit: (Chunk[O], KResult[E, Unit]) < (Var[scan.S] & R) =
      Emit.run[O](withAbort)
    scan.init.map { s0 => Var.run[scan.S, (Chunk[O], KResult[E, Unit]), R](s0)(withEmit) }
  }

  def spec: Spec[Any, Any] = suite("Scan (unified trait)")(

    // --------------------------------------------------------------
    // (1) Pure `arr` lifts into `Scan[I, Any, O, Nothing]`.
    // --------------------------------------------------------------

    test("Scan.liftArr runs a pure function on each input") {
      val s: Scan[Int, Any, Int, Nothing] = Scan.liftArr[Int, Int](_ + 1)
      val (out, result)                   = runCollect(s, Seq(1, 2, 3)).eval
      assertTrue(out == Chunk(2, 3, 4)) &&
      assertTrue(result == KResult.succeed(()))
    },

    // --------------------------------------------------------------
    // (2) Caller-row widening: the scan has `R = Any`, the caller
    // supplies `Int < Sync` at the step site. The widening is the
    // point -- if this test compiles it's already half-working.
    // --------------------------------------------------------------

    test("Scan.step accepts an effectful input `I < Sync`") {
      // Keep the refined `{ type S = Unit }` so the Tag machinery sees
      // the concrete state and no path-dependent tag acrobatics are
      // needed at the call site.
      val scan: Scan[Int, Any, Int, Nothing] { type S = Unit } =
        Scan.liftArr[Int, Int](_ * 10)

      // Here `EE = Sync`. The scan doesn't know about Sync; variance on
      // `I < EE` carries it through automatically.
      val stepWithSync: Unit < (Var[Unit] & Emit[Int] & Abort[Nothing] & Any & Sync) =
        scan.step[Sync](Sync.defer(21))

      val prog: (Chunk[Int], KResult[Nothing, Unit]) < (Var[Unit] & Sync) =
        Emit.run[Int](Abort.run[Nothing](stepWithSync))

      val (_, emitted, _) =
        Sync.Unsafe.evalOrThrow {
          Var.runTuple[Unit, (Chunk[Int], KResult[Nothing, Unit]), Sync](())(prog)
            .map { case (s, (c, r)) => (s, c, r) }
        }

      assertTrue(emitted == Chunk(210))
    },

    // --------------------------------------------------------------
    // (3) `Scan.make` owns its state; the caller never spells it out.
    // --------------------------------------------------------------

    test("Scan.make threads state via `Var[S]` without leaking S") {
      given Tag[Var[Long]] = Tag[Var[Long]]
      val sum: Scan[Int, Any, Long, Nothing] =
        Scan.make[Int, Any, Long, Nothing, Long](
          initial = 0L,
          stepFn = [EE] => (in: Int < EE, fr: Frame) => {
            given Frame = fr
            in.map(i => Var.update[Long](_ + i.toLong).unit)
          },
          endFn = fr => {
            given Frame = fr
            Var.get[Long].map(s => Emit.value(s))
          }
        )
      val (out, _) = runCollect(sum, Seq(1, 2, 3, 4, 5)).eval
      assertTrue(out == Chunk(15L))
    },

    // --------------------------------------------------------------
    // (4) Typed `Abort[E]` works: a failing scan closes the row with
    // `KResult.Failure(err)`.
    // --------------------------------------------------------------

    test("Scan.make can abort with a typed error") {
      given Tag[Var[Int]] = Tag[Var[Int]]

      case class Explode(n: Int)

      val boom: Scan[Int, Any, Int, Explode] =
        Scan.make[Int, Any, Int, Explode, Int](
          initial = 0,
          stepFn = [EE] => (in: Int < EE, fr: Frame) => {
            given Frame = fr
            in.map { i =>
              if (i < 0) Abort.fail[Explode](Explode(i))
              else Emit.value(i)
            }
          },
          endFn = _ => (): Unit
        )

      val (out, result) = runCollect(boom, Seq(1, 2, -7, 99)).eval
      assertTrue(out == Chunk(1, 2)) &&
      assertTrue(result == KResult.fail(Explode(-7)))
    }
  )
}
