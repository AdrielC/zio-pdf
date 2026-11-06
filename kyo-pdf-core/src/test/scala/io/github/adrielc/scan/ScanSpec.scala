package io.github.adrielc.scan

import scala.collection.mutable.ArrayBuffer
import scala.compiletime.testing.typeChecks
import zio.ZIO
import zio.test.*

object ScanSpec extends ZIOSpecDefault:
  private def checked[E, A](value: Either[E, A]): A = value match
    case Right(result) => result
    case Left(error) => throw AssertionError(error.toString)
  private def outputs[I, O](scan: Scan[I, O], input: List[I]): Vector[O] =
    val out = Vector.newBuilder[O]
    val run = checked(scan.compile()).open(value => { out += value; () })
    input.foreach(value => checked(run.push(value)))
    checked(run.finish())
    out.result()

  def spec = suite("Composable generic scans")(
    test("path-dependent state survives mapping, compilation and typed snapshots") {
      val sum: Scan.Aux[Long, Int, Long] = Scan.foldLong[Int](0L)((state, value) => state + value)
      val mapped: Scan.Aux[sum.State, String, String] = sum.contramap[String](_.length).map(_.toString)
      val compiled = checked(mapped.compile())
      val out = ArrayBuffer.empty[String]
      val run = compiled.open(value => { out += value; () })
      val seed: sum.State = mapped.initial
      checked(run.push("abc"))
      val current: Either[Scan.RunIssue, sum.State] = run.state
      val finalState: Either[Scan.RunIssue, sum.State] = run.finish()
      assertTrue(seed == 0L, current == Right(3L), finalState == Right(3L), out.toVector == Vector("3"),
        compiled.layout.longSlots == 1, compiled.layout.referenceSlots == 0)
    },
    test("incompatible input and state types fail at compile time") {
      val wrongInput = typeChecks("""
        import io.github.adrielc.scan.Scan
        Scan.map[Int, String](_.toString).andThen(Scan.count[Boolean])
      """)
      val wrongState = typeChecks("""
        import io.github.adrielc.scan.Scan
        val sum = Scan.count[Int]
        val bad: Scan.Aux[String, Int, Long] = sum
      """)
      assertTrue(!wrongInput, !wrongState)
    },
    test("zero-many emissions chain and upstream completion flushes through downstream before its EOF") {
      val expand = Scan.stateful[Int, Int, Int](0)(
        (state, value, emit) => { if value > 0 then { emit(value); emit(value + 1) }; state + 1 },
        (state, emit) => emit(state)
      )
      val sum = Scan.foldLong[Int](0L)((state, value) => state + value)
      val pipe: Scan.Aux[(Int, Long), Int, Long] = expand.andThen(sum)
      val out = ArrayBuffer.empty[Long]
      val run = checked(pipe.compile()).open(value => { out += value; () })
      checked(run.push(2)); checked(run.push(-1))
      val before = out.isEmpty
      val end: Either[Scan.RunIssue, (Int, Long)] = run.finish()
      assertTrue(before, end == Right((2, 7L)), out.toVector == Vector(7L))
    },
    test("fresh mutable state for initial calls, repeated runs and repeated branch definitions") {
      var initialized = 0
      val scan = Scan.stateful[ArrayBuffer[Int], Int, Int]({ initialized += 1; ArrayBuffer.empty[Int] })(
        (state, value, _) => { state += value; state }, (state, emit) => emit(state.sum))
      val seed1 = scan.initial
      val seed2 = scan.initial
      val compiled = checked(scan.broadcast(scan).compile())
      val out = ArrayBuffer.empty[Either[Int, Int]]
      val first = compiled.open(value => { out += value; () })
      val second = compiled.open(value => { out += value; () })
      checked(first.push(3)); checked(second.push(7))
      val a = checked(first.finish())
      val b = checked(second.finish())
      assertTrue(!(seed1 eq seed2), initialized == 6, !(a._1 eq a._2), !(a._1 eq b._1),
        a._1.toList == List(3), b._2.toList == List(7), out.toSet == Set(Left(3), Right(3), Left(7), Right(7)))
    },
    test("broadcast shares the input identity and preserves unequal output counts") {
      val owner = new Object
      var same = true
      val one = Scan.map[Object, Int](value => { same &&= value eq owner; 1 })
      val many = Scan.stateless[Object, Int] { (value, emit) =>
        same &&= value eq owner
        emit(2); emit(3)
      }
      val found = outputs(one.broadcast(many), List(owner))
      assertTrue(same, found.collect { case Left(n) => n } == Vector(1),
        found.collect { case Right(n) => n } == Vector(2, 3))
    },
    test("conditionals invoke only the selected branch and preserve branch state") {
      var yesCalls = 0
      var noCalls = 0
      val yes = Scan.statefulLong[Int, String](0L)((state, value, emit) => {
        yesCalls += 1; emit(s"yes:$value"); state + 1
      })
      val no = Scan.statefulLong[Int, String](0L)((state, value, emit) => {
        noCalls += 1; emit(s"no:$value"); state + 1
      })
      val scan: Scan.Aux[(Long, Long), Int, Either[String, String]] = Scan.branch[Int, String, String](_ > 0)(yes, no)
      val out = ArrayBuffer.empty[Either[String, String]]
      val run = checked(scan.compile()).open(value => { out += value; () })
      List(1, -1, 2).foreach(value => checked(run.push(value)))
      val state = run.finish()
      assertTrue(yesCalls == 2, noCalls == 1, state == Right((2L, 1L)),
        out.toVector == Vector(Left("yes:1"), Right("no:-1"), Left("yes:2")))
    },
    test("left and right route untouched values without resetting scanner state") {
      val scan = Scan.statefulLong[Int, Long](0L)((state, value, emit) => { emit(state + value); state + value })
      val left: Scan.Aux[Long, Either[Int, String], Either[Long, String]] = scan.left[String]
      val right: Scan.Aux[Long, Either[String, Int], Either[String, Long]] = scan.right[String]
      assertTrue(outputs(left, List(Left(2), Right("pass"), Left(3))) == Vector(Left(2L), Right("pass"), Left(5L)),
        outputs(right, List(Right(2), Left("pass"), Right(3))) == Vector(Right(2L), Left("pass"), Right(5L)))
    },
    test("composition preserves outputs under reassociation and identity, including EOF") {
      val first = Scan.stateful[Int, Int, Int](0)((state, value, emit) => { emit(value); state + 1 }, (state, emit) => emit(state))
      val second = Scan.filter[Int](_ % 2 == 0)
      val third = Scan.foldLong[Int](0L)((sum, value) => sum + value)
      val input = List(2, 3, 4)
      val expected = outputs(first.andThen(second).andThen(third), input)
      assertTrue(expected == Vector(6L), outputs(first.andThen(second.andThen(third)), input) == expected,
        outputs(Scan.identity[Int].andThen(first).andThen(second).andThen(third).andThen(Scan.identity[Long]), input) == expected)
    },
    test("deep composition compiles, runs, finishes and snapshots without recursive interpretation") {
      var chain: Scan.Aux[Unit, Int, Int] = Scan.identity[Int]
      var index = 0
      while index < 6000 do
        chain = chain.map(_ + 1)
        index += 1
      val compiled = checked(chain.compile(checked(Scan.Limits.checked(nodes = Scan.MaxNodes))))
      var result = 0
      val run = compiled.open(value => result = value)
      checked(run.push(1))
      assertTrue(run.state == Right(()), run.finish() == Right(()), result == 6001,
        compiled.layout.referenceSlots == 0, compiled.layout.longSlots == 0)
    },
    test("plan and pending-output budgets fail predictably instead of retaining unbounded outputs") {
      val limitedPlan = Scan.map[Int, Int](_ + 1).map(_ + 1).compile(checked(Scan.Limits.checked(nodes = 1)))
      val burst = Scan.stateless[Int, Int]((value, emit) => { emit(value); emit(value); emit(value) })
      var delivered = 0
      val run = checked(burst.compile(checked(Scan.Limits.checked(pending = 2)))).open(_ => delivered += 1)
      val failed = run.push(1)
      assertTrue(limitedPlan == Left(Scan.CompileIssue.TooManyNodes(1)), failed == Left(Scan.RunIssue.PendingLimit(2)),
        run.isClosed, delivered == 0)
    },
    test("escaped emitters cannot publish into a later callback or run") {
      var escaped: Option[Scan.Emit[Int]] = None
      val run = checked(Scan.stateless[Int, Int]((_, emit) => escaped = Some(emit)).compile()).open(_ => ())
      checked(run.push(1))
      escaped.get(2)
      assertTrue(run.isClosed, run.push(3) == Left(Scan.RunIssue.EscapedEmitter))
    },
    test("batch input drains incrementally with a tiny queue and its handle expires") {
      var escaped: Option[Scan.Emit[Int]] = None
      var observed = 0
      val run = checked(Scan.map[Int, Int](_ + 1).compile(checked(Scan.Limits.checked(pending = 1))))
        .open(value => observed += value)
      val result = run.feed { emit =>
        escaped = Some(emit)
        (0 until 100).foreach(emit(_))
      }
      escaped.get(999)
      assertTrue(result == Right(()), observed == 5050, run.isClosed,
        run.finish() == Left(Scan.RunIssue.EscapedEmitter))
    },
    test("callback defects close the runner and cannot replay queued outputs") {
      val bad = Scan.stateless[Int, Int] { (value, emit) => emit(value); throw IllegalStateException("synthetic defect") }
      var delivered = 0
      val run = checked(bad.compile()).open(_ => delivered += 1)
      ZIO.succeed(run.push(1)).exit.map(exit => assertTrue(exit.isFailure, run.isClosed, delivered == 0,
        run.state == Left(Scan.RunIssue.Closed)))
    },
    test("reentrant driving is rejected and close during output stops subsequent delivery") {
      var nested: Option[Either[Scan.RunIssue, Unit]] = None
      var handle: Option[Scan.Runner[Int, Unit]] = None
      var delivered = 0
      val scan = Scan.stateless[Int, Int]((value, emit) => { emit(value); emit(value + 1) })
      val run = checked(scan.compile()).open { _ =>
        delivered += 1
        nested = Some(handle.get.push(9))
        handle.get.close()
      }
      handle = Some(run)
      val result = run.push(1)
      assertTrue(result == Left(Scan.RunIssue.Closed), nested.contains(Left(Scan.RunIssue.ReentrantDriver)), delivered == 1)
    },
    test("completion emits once, supports empty sources and returns the final state") {
      val out = ArrayBuffer.empty[Long]
      val run = checked(Scan.count[String].compile()).open(value => { out += value; () })
      val done = run.finish()
      val again = run.finish()
      assertTrue(done == Right(0L), again == Left(Scan.RunIssue.Closed), out.toVector == Vector(0L))
    },
    test("spans slice owners generically without copying and preserve long coordinates") {
      val owner = new Object
      val span = checked(Span.checked(owner, Long.MaxValue - 8, Long.MaxValue))
      val part = checked(span.slice(Long.MaxValue - 3, Long.MaxValue))
      assertTrue(part.owner eq owner, part.length == 3, span.slice(0, 1).isLeft,
        Span.checked(owner, -1, 1).isLeft, Span.checked(owner, 2, 1).isLeft)
    }
  )
