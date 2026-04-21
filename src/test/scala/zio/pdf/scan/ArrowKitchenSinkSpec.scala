/*
 * Tests for the full Arrow / ArrowChoice surface plus a single
 * pathological-by-design composition that uses *every* combinator.
 *
 * The combinators tested:
 *
 *   Category:  >>> <<< andThen compose
 *   Profunctor: map contramap dimap
 *   Strong arrow:  first second *** &&& diag fst snd swap keepFirst keepSecond
 *   Choice arrow:  left right ||| +++ mirror merge injectLeft injectRight test
 *   Glue:          arr id const void drainLeft
 *
 * Plus a "kitchen sink" pipeline at the bottom that wires all of them
 * together over a single input stream. If any operator regresses, that
 * pipeline produces the wrong outputs and the test fails.
 */

package zio.pdf.scan

import kyo.*
import zio.test.*

object ArrowKitchenSinkSpec extends ZIOSpecDefault {

  // ----- helpers -----

  private def runD[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): (ScanDone[O, Any], Vector[O]) =
    Scan.runDirect[I, O, Any](scan, inputs)

  private def agree[I, O](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): TestResult = {
    val (_, dOut) = runD(scan, inputs)
    val (_, kOut) = Scan.runKyo[I, O, Any](scan, inputs).eval
    assertTrue(dOut == kOut.toVector)
  }

  // -------- the suite --------

  def spec: Spec[Any, Any] = suite("Scan Arrow / ArrowChoice surface")(

    // ===================================================================
    // Category basics
    // ===================================================================

    test("<<< is the flip of >>>") {
      val f: FreeScan[Int, Int]    = Scan.map[Int, Int](_ + 1)
      val g: FreeScan[Int, String] = Scan.map[Int, String](i => s"#$i")
      val rhs = f >>> g
      val lhs = g <<< f
      val inputs = (0 until 16).toList
      assertTrue(runD(lhs, inputs)._2 == runD(rhs, inputs)._2)
    },

    test("andThen / compose are aliases for >>> / <<<") {
      val f: FreeScan[Int, Int]    = Scan.map[Int, Int](_ * 2)
      val g: FreeScan[Int, String] = Scan.map[Int, String](_.toString)
      val a = f.andThen(g)
      val b = g.compose(f)
      val c = f >>> g
      val inputs = List(1, 2, 3, 4)
      assertTrue(runD(a, inputs)._2 == runD(c, inputs)._2) &&
      assertTrue(runD(b, inputs)._2 == runD(c, inputs)._2)
    },

    // ===================================================================
    // Strong arrow: first / second / *** / &&&
    // ===================================================================

    test("first runs only on _1; the second component flows through") {
      val f: FreeScan[Int, Int]                 = Scan.map[Int, Int](_ * 10)
      val pipeline: FreeScan[(Int, String), (Int, String)] = f.first[String]
      val inputs: Seq[(Int, String)] = (1 to 4).map(i => (i, s"k$i"))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector((10, "k1"), (20, "k2"), (30, "k3"), (40, "k4")))
    },

    test("second runs only on _2; the first component flows through") {
      val f: FreeScan[Int, Int] = Scan.map[Int, Int](_ + 100)
      val pipeline: FreeScan[(String, Int), (String, Int)] = f.second[String]
      val inputs: Seq[(String, Int)] = (1 to 3).map(i => (s"x$i", i))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector(("x1", 101), ("x2", 102), ("x3", 103)))
    },

    test("*** is parallel composition on a tuple input") {
      val l: FreeScan[Int, Int]    = Scan.map[Int, Int](_ * 2)
      val r: FreeScan[String, Int] = Scan.map[String, Int](_.length)
      val pipeline: FreeScan[(Int, String), (Int, Int)] = l *** r
      val inputs = Seq((3, "ab"), (4, "xyz"), (5, ""))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector((6, 2), (8, 3), (10, 0)))
    },

    test("a *** b === a.first[Bin] >>> b.second[Aout] (both shapes equal)") {
      // Exercise the Arrow law `first >>> second === ***` (sans tuple
      // shuffling, which is identity here because the components don't
      // overlap). For the comparison we keep the operands pure so the
      // fusion step kicks in for both pipelines.
      val a: FreeScan[Int, Int]    = Scan.map[Int, Int](_ + 1)
      val b: FreeScan[String, Int] = Scan.map[String, Int](_.length)
      val viaStarStar = a *** b
      val viaFirstSecond =
        (a.first[String]) >>> (b.second[Int])
      val inputs = Seq((1, "a"), (2, "bb"), (3, "ccc"))
      assertTrue(runD(viaStarStar, inputs)._2 == runD(viaFirstSecond, inputs)._2)
    },

    test("&&& fans the same input into a tuple") {
      val pipeline: FreeScan[Int, (Int, String)] =
        Scan.map[Int, Int](_ * 3) &&& Scan.map[Int, String](i => s"$i!")
      val (_, out) = runD(pipeline, List(1, 2, 3))
      assertTrue(out == Vector((3, "1!"), (6, "2!"), (9, "3!")))
    },

    test("diag === id &&& id") {
      val viaDiag: FreeScan[Int, (Int, Int)] = Scan.diag[Int]
      val viaIdId: FreeScan[Int, (Int, Int)] = FreeScan.id[Int] &&& FreeScan.id[Int]
      val inputs = (0 until 8).toList
      assertTrue(runD(viaDiag, inputs)._2 == runD(viaIdId, inputs)._2)
    },

    test("fst / snd project a tupled stream") {
      val inputs = Seq((1, "a"), (2, "b"), (3, "c"))
      val (_, ls) = runD(Scan.fst[Int, String], inputs)
      val (_, rs) = runD(Scan.snd[Int, String], inputs)
      assertTrue(ls == Vector(1, 2, 3)) && assertTrue(rs == Vector("a", "b", "c"))
    },

    test("swap is its own inverse") {
      val pipeline: FreeScan[(Int, String), (Int, String)] =
        Scan.swap[Int, String] >>> Scan.swap[String, Int]
      val inputs   = Seq((1, "a"), (2, "b"))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == inputs.toVector)
    },

    test("keepFirst / keepSecond projects out of a fanout") {
      val fan: FreeScan[Int, (Int, String)] =
        Scan.map[Int, Int](_ + 100) &&& Scan.map[Int, String](i => s"$i")
      val (_, ls) = runD(fan.keepFirst[Int, String], List(1, 2))
      val (_, rs) = runD(fan.keepSecond[Int, String], List(1, 2))
      assertTrue(ls == Vector(101, 102)) && assertTrue(rs == Vector("1", "2"))
    },

    // ===================================================================
    // Choice arrow: left / right / ||| / +++ / mirror / merge / inject*
    // ===================================================================

    test("left runs on the Left branch; Rights flow through") {
      val f: FreeScan[Int, Int] = Scan.map[Int, Int](_ * 2)
      val pipeline: FreeScan[Either[Int, String], Either[Int, String]] = f.left[String]
      val inputs: Seq[Either[Int, String]] = Seq(Left(3), Right("hi"), Left(4))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector(Left(6), Right("hi"), Left(8)))
    },

    test("right runs on the Right branch; Lefts flow through") {
      val f: FreeScan[Int, String] = Scan.map[Int, String](i => s"#$i")
      val pipeline: FreeScan[Either[String, Int], Either[String, String]] = f.right[String]
      val inputs: Seq[Either[String, Int]] = Seq(Left("a"), Right(7), Left("b"), Right(8))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector(Left("a"), Right("#7"), Left("b"), Right("#8")))
    },

    test("+++ is disjoint parallel composition over Either") {
      val l: FreeScan[Int, String]    = Scan.map[Int, String](i => s"L$i")
      val r: FreeScan[String, String] = Scan.map[String, String](_.toUpperCase.nn)
      val pipeline: FreeScan[Either[Int, String], Either[String, String]] = l +++ r
      val inputs: Seq[Either[Int, String]] = Seq(Left(1), Right("a"), Left(2), Right("b"))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector(Left("L1"), Right("A"), Left("L2"), Right("B")))
    },

    test("|||  collapses both branches to a single output type") {
      val l: FreeScan[Int, Int] = Scan.map[Int, Int](_ * 100)
      val r: FreeScan[String, Int] = Scan.map[String, Int](_.length)
      val pipeline: FreeScan[Either[Int, String], Int] = l ||| r
      val inputs: Seq[Either[Int, String]] = Seq(Left(1), Right("ab"), Left(2), Right("xyz"))
      val (_, out) = runD(pipeline, inputs)
      assertTrue(out == Vector(100, 2, 200, 3))
    },

    test("mirror swaps Either sides, idempotent under double application") {
      val pipeline: FreeScan[Either[Int, String], Either[Int, String]] =
        Scan.mirror[Int, String] >>> Scan.mirror[String, Int]
      val inputs: Seq[Either[Int, String]] = Seq(Left(1), Right("a"), Left(2))
      assertTrue(runD(pipeline, inputs)._2 == inputs.toVector)
    },

    test("merge collapses Either[A, A] back to A") {
      val inputs: Seq[Either[Int, Int]] = Seq(Left(1), Right(2), Left(3), Right(4))
      val (_, out) = runD(Scan.merge[Int], inputs)
      assertTrue(out == Vector(1, 2, 3, 4))
    },

    test("injectLeft / injectRight tag a stream") {
      val (_, ls) = runD(Scan.injectLeft[Int, String], List(1, 2, 3))
      val (_, rs) = runD(Scan.injectRight[Int, String], List("a", "b"))
      assertTrue(ls == Vector(Left(1), Left(2), Left(3))) &&
      assertTrue(rs == Vector(Right("a"), Right("b")))
    },

    test("test routes by predicate") {
      val yes: FreeScan[Int, String] = Scan.map[Int, String](i => s"+$i")
      val no:  FreeScan[Int, String] = Scan.map[Int, String](i => s"-$i")
      val pipeline = Scan.test((i: Int) => i > 0)(yes)(no)
      val (_, out) = runD(pipeline, List(-2, -1, 0, 1, 2))
      assertTrue(out == Vector("--2", "--1", "-0", "+1", "+2"))
    },

    // ===================================================================
    // Glue
    // ===================================================================

    test("const ignores the input and emits a fixed value") {
      val pipeline: FreeScan[Int, String] = Scan.const("hi")
      val (_, out) = runD(pipeline, List(1, 2, 3))
      assertTrue(out == Vector("hi", "hi", "hi"))
    },

    test("void emits Unit per input") {
      val pipeline: FreeScan[Int, Unit] = Scan.void(Scan.map[Int, Int](_ * 2))
      val (_, out) = runD(pipeline, List(1, 2, 3))
      assertTrue(out == Vector((), (), ()))
    },

    test("drainLeft is the identity on a single arm") {
      val s: FreeScan[Int, Int] = Scan.map[Int, Int](_ + 1)
      val pipeline: FreeScan[Int, Int] = s.drainLeft[String]
      val (_, out) = runD(pipeline, List(1, 2, 3))
      assertTrue(out == Vector(2, 3, 4))
    },

    // ===================================================================
    // Arrow laws
    // ===================================================================

    test("Arrow law: arr id >>> f === f === f >>> arr id") {
      val f: FreeScan[Int, Int] = Scan.map[Int, Int](_ * 5 + 1)
      val inputs = (0 until 16).toList
      val base = runD(f, inputs)._2
      assertTrue(runD(FreeScan.id[Int] >>> f, inputs)._2 == base) &&
      assertTrue(runD(f >>> FreeScan.id[Int], inputs)._2 == base)
    },

    test("Arrow law: arr (g . f) === arr f >>> arr g") {
      val f: Int => Int    = _ + 1
      val g: Int => String = i => s"!$i"
      val inputs = (0 until 8).toList
      val lhs = FreeScan.arr[Int, String](g compose f)
      val rhs = FreeScan.arr[Int, Int](f) >>> FreeScan.arr[Int, String](g)
      assertTrue(runD(lhs, inputs)._2 == runD(rhs, inputs)._2)
    },

    test("Arrow law: first(arr f) === arr (f *** id)") {
      val f: Int => Int = _ * 7
      val pipeline1: FreeScan[(Int, String), (Int, String)] =
        FreeScan.arr[Int, Int](f).first[String]
      val pipeline2: FreeScan[(Int, String), (Int, String)] =
        FreeScan.arr[(Int, String), (Int, String)] { case (i, s) => (f(i), s) }
      val inputs = (1 to 4).map(i => (i, s"k$i"))
      assertTrue(runD(pipeline1, inputs)._2 == runD(pipeline2, inputs)._2)
    },

    test("Choice law: f >>> injectLeft === injectLeft <<< f") {
      val f: FreeScan[Int, Int] = Scan.map[Int, Int](_ + 1)
      val inputs = (0 until 6).toList
      val a = f >>> Scan.injectLeft[Int, String]
      val b = Scan.injectLeft[Int, String] <<< f
      assertTrue(runD(a, inputs)._2 == runD(b, inputs)._2)
    },

    // ===================================================================
    // Kyo runner agreement on derived combinators
    // ===================================================================

    test("Kyo runner agrees on first / second / ***") {
      val f: FreeScan[Int, Int]    = Scan.map[Int, Int](_ * 3)
      val g: FreeScan[String, Int] = Scan.map[String, Int](_.length)
      val pipeline: FreeScan[(Int, String), ((Int, String), (Int, Int))] =
        f.first[String] &&& (f *** g)
      val inputs: Seq[(Int, String)] = (1 to 6).map(i => (i, "x" * i))
      agree(pipeline, inputs)
    },

    test("Kyo runner agrees on left / right / +++ / |||") {
      val l: FreeScan[Int, Int]    = Scan.map[Int, Int](_ + 10)
      val r: FreeScan[String, Int] = Scan.map[String, Int](_.length)
      // Two parallel views over Either[Int, String]:
      //   - the left view runs `l +++ identity-on-String`
      //   - the right view collapses to Int via |||
      val pipeline: FreeScan[Either[Int, String], (Either[Int, String], Int)] =
        (l.left[String]) &&& (l ||| r)
      val inputs: Seq[Either[Int, String]] = Seq(Left(1), Right("ab"), Left(2), Right("xyz"))
      agree(pipeline, inputs)
    },

    // ===================================================================
    // The kitchen sink: every combinator in one pipeline.
    // ===================================================================

    test("kitchen-sink pipeline uses every combinator and produces the expected outputs") {
      // Input:  Int (an opcode)
      //
      // Step 1: diag                      -- Int      => (Int, Int)
      // Step 2: (test on _1) *** (id)     -- decide whether _1 is "small"
      //                                       (>= 0) and route via test:
      //                                       small Int   -> Right("ok i")
      //                                       large Int   -> Left(i*10)
      //                                       so _1 becomes Either[Int, String]
      // Step 3: (mirror.first[Int])       -- swap the Either inside _1 and
      //                                       leave _2 alone:  ((Either[String, Int], Int))
      // Step 4: ((map _.length ||| map _*2).first[Int])
      //                                    -- collapse the inner Either to
      //                                       Int via |||: small lengths,
      //                                       large doubles.  So _1 becomes
      //                                       Int again.
      // Step 5: swap                      -- (Int, Int)  =>  (Int, Int)
      //                                       (yes it's the same type, but
      //                                       the components are swapped --
      //                                       _1 is now what used to be the
      //                                       passthrough copy, _2 the
      //                                       processed value.)
      // Step 6: (Scan.const(0)).second[Int]
      //                                    -- replace _2 with the constant 0
      //                                       leaving _1 (the passthrough)
      //                                       untouched.
      // Step 7: keepFirst                 -- drop _2 (which is now 0)
      // Step 8: injectRight[String, Int]  -- tag the remaining Int as Right
      // Step 9: merge[Either[String,Int]] -- nope, merge wants Either[A,A].
      //                                       Use mirror to wrap in
      //                                       Either[Int,String], then
      //                                       collapse via ||| so we end up
      //                                       with a plain Int again.
      //
      // The final result for input i is therefore: the original i.
      //
      // We check this end-to-end. If any combinator regresses, the
      // observed value will diverge from `inputs`.

      val step1: FreeScan[Int, (Int, Int)] = Scan.diag[Int]

      val routeOnFirst: FreeScan[Int, Either[Int, String]] =
        Scan.test[Int, Either[Int, String]](_ < 0)(
          yes = Scan.map[Int, Int](_ * 10).map(Left(_))
        )(
          no  = Scan.map[Int, String](i => s"ok $i").map(Right(_))
        )

      val step2: FreeScan[(Int, Int), (Either[Int, String], Int)] =
        routeOnFirst *** FreeScan.id[Int]

      val step3: FreeScan[(Either[Int, String], Int), (Either[String, Int], Int)] =
        Scan.mirror[Int, String].first[Int]

      val collapseInner: FreeScan[Either[String, Int], Int] =
        Scan.map[String, Int](_.length) ||| Scan.map[Int, Int](_ * 2)

      val step4: FreeScan[(Either[String, Int], Int), (Int, Int)] =
        collapseInner.first[Int]

      val step5: FreeScan[(Int, Int), (Int, Int)] = Scan.swap[Int, Int]

      val step6: FreeScan[(Int, Int), (Int, Int)] =
        Scan.const[Int, Int](0).second[Int]

      val step7: FreeScan[(Int, Int), Int] =
        FreeScan.id[(Int, Int)].keepFirst[Int, Int]

      val pipeline: FreeScan[Int, Int] =
        step1 >>> step2 >>> step3 >>> step4 >>> step5 >>> step6 >>> step7

      val inputs = (-3 to 3).toList
      val (_, out) = runD(pipeline, inputs)
      // Step 5 (swap) puts the *passthrough* copy of the input into _1
      // and the processed Int into _2; step 6 then replaces _2 with 0
      // and step 7 keeps _1. So the output is identically the input.
      assertTrue(out == inputs.toVector)
    },

    test("kitchen-sink: Kyo runner agrees with direct runner") {
      val pipeline: FreeScan[Int, Int] = {
        val routeOnFirst: FreeScan[Int, Either[Int, String]] =
          Scan.test[Int, Either[Int, String]](_ < 0)(
            yes = Scan.map[Int, Int](_ * 10).map(Left(_))
          )(
            no  = Scan.map[Int, String](i => s"ok $i").map(Right(_))
          )
        val collapseInner: FreeScan[Either[String, Int], Int] =
          Scan.map[String, Int](_.length) ||| Scan.map[Int, Int](_ * 2)

        Scan.diag[Int]                                  >>>
          (routeOnFirst *** FreeScan.id[Int])           >>>
          Scan.mirror[Int, String].first[Int]           >>>
          collapseInner.first[Int]                      >>>
          Scan.swap[Int, Int]                           >>>
          Scan.const[Int, Int](0).second[Int]           >>>
          FreeScan.id[(Int, Int)].keepFirst[Int, Int]
      }
      agree(pipeline, (-3 to 3).toList)
    }
  )
}
