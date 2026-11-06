package io.github.adrielc.scan

import scala.collection.mutable.ArrayBuffer
import zio.test.*

object ScanCompatibilitySpec extends ZIOSpecDefault:
  private def checked[E, A](value: Either[E, A]): A = value match
    case Right(result) => result
    case Left(error) => throw AssertionError(error.toString)
  private def run[I, O](scan: Scan[I, O], input: List[I]): (Vector[O], scan.State) =
    val out = ArrayBuffer.empty[O]
    val execution = checked(scan.compile()).open(value => { out += value; () })
    checked(execution.feed(emit => input.foreach(emit(_))))
    val state = checked(execution.finish())
    (out.toVector, state)
  private def accumulating = Scan.stateful[Int, Int, Int](0)(
    (state, input, emit) => { emit(state + input); emit(input * 10); state + input },
    (state, emit) => emit(state))

  def spec = suite("FS2 Scan behavior coverage")(
    test("contextual adapters retain the correct input across repeated upstream emissions") {
      val source = Scan.stateless[(Int, String), (Int, String)]((input, emit) => { emit(input); emit((input._1 + 1, input._2 + "!")) })
      val pipeline = source.andThen(accumulating.first[String])
      val (values, state) = run(pipeline, List((2, "a"), (4, "b")))
      assertTrue(values == Vector((2, "a"), (20, "a"), (5, "a!"), (30, "a!"), (9, "b"), (40, "b"), (14, "b!"), (50, "b!")),
        state == ((), 14))
    },
    test("lens and semilens discard completion outputs and bypass inputs leave state untouched") {
      val left = accumulating.left[String]
      val (values, state) = run(left, List(Left(2), Right("pass"), Left(3)))
      val right = accumulating.right[String]
      val (mirrored, rightState) = run(right, List(Right(2), Left("pass"), Right(3)))
      val mapped = accumulating.lens[String, String](_.length, (input, output) => s"$input:$output")
      assertTrue(values == Vector(Left(2), Left(20), Right("pass"), Left(5), Left(30)), state == 5,
        mirrored == Vector(Right(2), Right(20), Left("pass"), Right(5), Right(30)), rightState == 5,
        run(mapped, List("aa", "b"))._1 == Vector("aa:2", "aa:20", "b:3", "b:10"))
    },
    test("second and semipass retain input context and drop the unmatched EOF tail") {
      val second = accumulating.second[String]
      val pass = accumulating.semipass[Either[String, Int], Int | String] {
        case Left(value) => Left(value)
        case Right(value) => Right(value)
      }
      assertTrue(run(second, List(("x", 2), ("y", 3)))._1 == Vector(("x", 2), ("x", 20), ("y", 5), ("y", 30)),
        run(pass, List(Right(2), Left("bypass"), Right(3)))._1 == Vector(2, 20, "bypass", 5, 30))
    },
    test("choice and choose complete left then right before a shared downstream completion") {
      val left = Scan.foldLong[Int](0L)((sum, value) => sum + value)
      val right = Scan.foldLong[String](0L)((sum, value) => sum + value.length)
      val mixed: List[Either[Int, String]] = List(Left(2), Right("abc"), Left(3))
      assertTrue(run(left.choose(right), mixed) == (Vector(Left(5L), Right(3L)), (5L, 3L)),
        run(left.choice(right), mixed) == (Vector(5L, 3L), (5L, 3L)))
    },
    test("state mapping applies conversions on every input, including a non-isomorphic mapping") {
      val source = Scan.stateful1[Int, Int, Int](0)((state, input) => (state + input, state + input))
      val mapped: Scan.Aux[Int, Int, Int] = source.imapState(_ / 10)(_ * 10)
      assertTrue(run(mapped, List(1, 1, 1)) == (Vector(1, 1, 1), 0),
        run(source.dimap[String, String](_.length)(_.toString), List("ab", "c")) == (Vector("2", "3"), 3))
    },
    test("explicit state stepping and completion do not evaluate replaced initializers") {
      var seeds = 0
      val source = Scan.stateful[Int, Int, Int]({ seeds += 1; 100 })(
        (state, value, emit) => { emit(state + value); state + value }, (state, emit) => emit(state))
      val out = ArrayBuffer.empty[Int]
      val single = checked(source.transform(5, 2)(value => { out += value; () }))
      val batch = checked(source.transformAccumulate(single, List(3, 4))(value => { out += value; () }))
      checked(source.onComplete(batch)(value => { out += value; () }))
      val before = seeds
      val resumed = checked(source.step(1)(_ => ()))
      val state = run(resumed, List(2))._2
      assertTrue(single == 7, batch == 14, out.toVector == Vector(7, 10, 14, 14), before == 0, seeds == 1, state == 103)
    },
    test("mapped state can be replaced and continued without allocating new interpreter storage per input") {
      val mapped = accumulating.imapState(_.toString)(_.toInt)
      val resumed = mapped.withState("7")
      val (values, state) = run(resumed, List(2, 3))
      assertTrue(values == Vector(9, 20, 12, 30, 12), state == "12")
    },
    test("non-inverse state conversions do not introduce a transition at completion") {
      val mapped = Scan.fold[Int, Int](0)(_ + _).imapState(_ + 1)(_ * 2)
      assertTrue(run(mapped, Nil) == (Vector(2), 1), run(mapped, List(3)) == (Vector(12), 6))
    },
    test("explicit accumulation stops reading inputs after the first typed failure") {
      var read = 0
      val input = Iterator.continually { read += 1; read }
      val scan = Scan.stateless[Int, Int]((value, emit) => (0 until 2000).foreach(_ => emit(value)))
      val result = scan.transformAccumulate((), input)(_ => ())
      assertTrue(result == Left(Scan.RunIssue.PendingLimit(1024)), read == 1)
    },
    test("deep initial state and stepping do not recursively evaluate definition seeds") {
      val deep = (1 to 1800).foldLeft(Scan.identity[Int])((scan, _) => scan.map(_ + 1))
      val out = ArrayBuffer.empty[Int]
      val continued = checked(deep.step(1)(value => { out += value; () }))
      assertTrue(deep.initial == (), out.toVector == Vector(1801), run(continued, List(2))._1 == Vector(1802))
    },
    test("nested context adapters preserve outer context and fresh state across independent runs") {
      val nested = accumulating.first[String].second[Boolean]
      val expected = (Vector((true, (2, "a")), (true, (20, "a")), (false, (5, "b")), (false, (30, "b"))), 5)
      val input = List((true, (2, "a")), (false, (3, "b")))
      assertTrue(run(nested, input) == expected, run(nested, input) == expected)
    },
    test("observation syntax preserves input identity, event order and EOF observations") {
      import ObservationSyntax.*
      final case class Notice(value: Int)
      val original = new Object
      val observer = Scan.stateful1[Int, Object, Notice](0)((state, _) => (state + 1, Notice(state + 1)))
      val union = run(observer.withInput, List(original, original))._1
      val tagged = run(observer.withInputEither, List(original, original))._1
      assertTrue(union == Vector(original, Notice(1), original, Notice(2)),
        tagged == Vector(Left(original), Right(Notice(1)), Left(original), Right(Notice(2))))
    },
    test("composition budgets count adapter children and reject excessive nesting before initialization") {
      var initialized = 0
      var nested: Scan.Aux[Int, Int, Int] = Scan.stateful1({ initialized += 1; 0 })((state: Int, input: Int) => (state + input, input))
      (0 until Scan.MaxAdaptationDepth + 1).foreach(_ => nested = nested.imapState(value => value)(value => value))
      val rejected = nested.compile()
      val nodes = accumulating.first[String].compile(checked(Scan.Limits.checked(nodes = 1)))
      assertTrue(rejected == Left(Scan.CompileIssue.AdaptationDepth(Scan.MaxAdaptationDepth)),
        nodes == Left(Scan.CompileIssue.TooManyNodes(1)), initialized == 0)
    }
  )
