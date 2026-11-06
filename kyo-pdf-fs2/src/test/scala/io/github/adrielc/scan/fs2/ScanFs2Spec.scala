package io.github.adrielc.scan.fs2

import cats.{Contravariant, Functor}
import cats.arrow.Strong
import cats.effect.{Deferred, IO}
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Stream}
import io.github.adrielc.scan.{ObservationSyntax, Scan}
import scala.collection.mutable.ArrayBuffer
import zio.test.*

object ScanFs2Spec extends ZIOSpecDefault:
  private final case class Bytes(value: Vector[Byte])
  private final case class Seen(total: Int)
  import ScanFs2.*
  import ScanFs2.given

  private def values[I, O](scan: Scan[I, O], input: List[I]): Vector[O] =
    Stream.emits(input).covary[IO].through(scan.toPipe[IO]).compile.toVector.unsafeRunSync()
  private def reference[S, I, O](scan: fs2.Scan[S, I, O], input: List[I]): Vector[O] =
    Stream.emits(input).covary[IO].through(scan.toPipe[IO]).compile.toVector.unsafeRunSync()
  private def ours = Scan[Int, Int, Int](0)(
    (state, input, emit) => { emit(state + input); emit(input * 10); state + input },
    (state, emit) => emit(state))
  private def theirs = fs2.Scan[Int, Int, Int](0)(
    (state, input) => (state + input, Chunk(state + input, input * 10)), state => Chunk.singleton(state))

  def spec = suite("FS2 3.14 Scan differential and transport tests")(
    test("constructors, mapping, chaining and upstream EOF agree with FS2") {
      val next = Scan.stateful1[Int, Int, String](0)((state, value) => (state + value, s"${state + value}"))
      val fsNext = fs2.Scan.stateful1[Int, Int, String](0)((state, value) => (state + value, s"${state + value}"))
      val input = List("ab", "c", "def")
      assertTrue(values(ours.dimap[String, Int](_.length)(_ + 1).andThen(next), input) ==
        reference(theirs.dimap[String, Int](_.length)(_ + 1).andThen(fsNext), input),
        values(Scan.lift[Int, Int](_ + 2), List(1, 3)) == reference(fs2.Scan.lift[Int, Int](_ + 2), List(1, 3)),
        values(ours, Nil) == reference(theirs, Nil))
    },
    test("all context adapters agree with FS2 including discarded EOF tails") {
      val pairs = List((2, "a"), (3, "b"))
      val reverse = pairs.map(_.swap)
      val mixed: List[Either[Int, String]] = List(Left(2), Right("x"), Left(3))
      assertTrue(values(ours.first[String], pairs) == reference(theirs.first[String], pairs),
        values(ours.second[String], reverse) == reference(theirs.second[String], reverse),
        values(ours.left[String], mixed) == reference(theirs.left[String], mixed),
        values(ours.right[String], mixed.map(_.swap)) == reference(theirs.right[String], mixed.map(_.swap)),
        values(ours.lens[String, String](_.length, (s, n) => s + n), List("ab", "c")) ==
          reference(theirs.lens[String, String](_.length, (s, n) => s + n), List("ab", "c")))
    },
    test("semilens and semipass bypass without changing state and agree with FS2") {
      val input = List(2, -4, 3)
      val extract: Int => Either[Int, Int] = n => if n < 0 then Left(n) else Right(n)
      val inject: (Int, Int) => Int = (input, output) => input + output
      assertTrue(values(ours.semilens(extract, inject), input) == reference(theirs.semilens(extract, inject), input),
        values(ours.semipass(extract), input) == reference(theirs.semipass(extract), input))
    },
    test("choice, choose, state mapping and EOF order agree with FS2") {
      val input: List[Either[Int, Int]] = List(Left(2), Right(5), Left(3))
      assertTrue(values(ours.choose(ours), input) == reference(theirs.choose(theirs), input),
        values(ours.choice(ours), input) == reference(theirs.choice(theirs), input),
        values(ours.imapState(_ / 10)(_ * 10), List(1, 2, 3)) ==
          reference(theirs.imapState(_ / 10)(_ * 10), List(1, 2, 3)))
    },
    test("explicit state stepping, accumulation and completion agree with FS2") {
      val out = ArrayBuffer.empty[Int]
      val state = ours.transformAccumulate(7, List(2, 3))(value => { out += value; () }).toOption.get
      val expected = theirs.transformAccumulate(7, Chunk(2, 3))
      val sameBatch = out.toVector == expected._2.toVector
      out.clear()
      val completion = ours.onComplete(state)(value => { out += value; () })
      val step = ours.step(2)(_ => ()).toOption.get
      val fsStep = theirs.step(2)._1
      assertTrue(state == expected._1, sameBatch, completion.isRight,
        out.toVector == theirs.onComplete(expected._1).toVector,
        values(step, List(3)) == reference(fsStep, List(3)))
    },
    test("Cats Functor, Contravariant and Strong match direct composition") {
      val mapped = Functor[[O] =>> Scan.Aux[Int, Int, O]].map(ours)(_.toString)
      val contra = Contravariant[[I] =>> Scan.Aux[Int, I, Int]].contramap(ours)((s: String) => s.length)
      val strong = Strong[[I, O] =>> Scan.Aux[Int, I, O]]
      assertTrue(values(mapped, List(2, 3)) == reference(theirs.map(_.toString), List(2, 3)),
        values(contra, List("aa", "b")) == reference(theirs.contramap[String](_.length), List("aa", "b")),
        values(strong.first[Int, Int, String](ours), List((2, "x"))) == reference(theirs.first[String], List((2, "x"))),
        values(strong.second[Int, Int, String](ours), List(("x", 2))) == reference(theirs.second[String], List(("x", 2))),
        values(strong.dimap(ours)((s: String) => s.length)(_.toString), List("aa")) ==
          reference(theirs.dimap[String, String](_.length)(_.toString), List("aa")))
    },
    test("one shared pipe definition creates fresh state for every evaluation") {
      var seeds = 0
      val scan = Scan.stateful1[Array[Int], Int, Int]({ seeds += 1; Array(0) }) { (state, value) =>
        state(0) += value
        (state, state(0))
      }
      val stream = Stream.emits(List(2, 3)).covary[IO].through(scan.toPipe[IO])
      val before = seeds
      val first = stream.compile.toVector.unsafeRunSync()
      val second = stream.compile.toVector.unsafeRunSync()
      assertTrue(before == 0, seeds == 2, first == Vector(2, 5), first == second)
    },
    test("union and Either observation streams preserve input ownership, bytes and EOF events") {
      import ObservationSyntax.*
      val input = List(Bytes(Vector(1, 2)), Bytes(Vector(3, 4, 5)))
      val observer = Scan.stateful[Int, Bytes, Seen](0)(
        (state, bytes, emit) => { val total = state + bytes.value.size; emit(Seen(total)); total },
        (state, emit) => emit(Seen(state)))
      val union = values(observer.withInput, input)
      val tagged = values(observer.withInputEither, input)
      assertTrue(union == Vector(input(0), Seen(2), input(1), Seen(5), Seen(5)),
        union.collect { case bytes: Bytes => bytes }.zip(input).forall((a, b) => a eq b),
        tagged.collect { case Left(bytes) => bytes.value }.flatten == Vector[Byte](1, 2, 3, 4, 5),
        tagged.collect { case Right(seen) => seen } == Vector(Seen(2), Seen(5), Seen(5)))
    },
    test("early downstream termination stops source pulls and closes its scope without EOF") {
      var pulls = 0
      var released = 0
      var completions = 0
      val scan = Scan.stateful[Unit, Int, Int](())((_, n, emit) => emit(n), (_, _) => completions += 1)
      val source = Stream.bracket(IO.unit)(_ => IO { released += 1 }) >>
        Stream.repeatEval(IO { pulls += 1; pulls })
      val first = source.through(scan.toPipe[IO]).take(1).compile.toVector.unsafeRunSync()
      assertTrue(first == Vector(1), pulls == 1, released == 1, completions == 0)
    },
    test("source errors and scan defects keep their identity and release source scopes") {
      val error = new RuntimeException("synthetic source failure")
      var released = 0
      val source = Stream.bracket(IO.unit)(_ => IO { released += 1 }) >> Stream.raiseError[IO](error)
      val fromSource = source.through(Scan.identity[Int].toPipe[IO]).compile.drain.attempt.unsafeRunSync()
      val defect = valuesAttempt(Scan.lift[Int, Int](_ => throw error), Stream.emit(1).covary[IO])
      assertTrue(fromSource.left.toOption.contains(error), defect.left.toOption.contains(error), released == 1)
    },
    test("cancellation releases a blocked source and does not synthesize completion") {
      val result = (for
        entered <- Deferred[IO, Unit]
        released <- Deferred[IO, Unit]
        fiber <- (Stream.bracket(IO.unit)(_ => released.complete(()).void) >>
          Stream.eval(entered.complete(()) >> IO.never[Int]))
          .through(Scan.identity[Int].toPipe[IO]).compile.drain.start
        _ <- entered.get
        _ <- fiber.cancel
        closed <- released.tryGet
      yield closed.isDefined).unsafeRunSync()
      assertTrue(result)
    },
    test("bounded queue/output overflow fails without pulling the next input") {
      val limits = Scan.Limits.checked(pending = 2).toOption.get
      var pulled = 0
      val expand = Scan.stateless[Int, Int]((n, emit) => (0 until 10).foreach(_ => emit(n)))
      val source = Stream.repeatEval(IO { pulled += 1; pulled })
      val result = source.through(expand.toPipeWith[IO](limits)).compile.drain.attempt.unsafeRunSync()
      assertTrue(result.left.toOption.exists {
        case issue: ScanException => issue.issue == Scan.RunIssue.PendingLimit(2)
        case _ => false
      }, pulled == 1)
    },
    test("invalid plans fail before evaluating the source or creating state") {
      var seeds = 0
      var pulled = 0
      val scan = Scan.stateful1[Int, Int, Int]({ seeds += 1; 0 })((state, n) => (state + n, n)).map(_ + 1)
      val result = Stream.eval(IO { pulled += 1; 1 }).through(scan.toPipeWith[IO](Scan.Limits.checked(nodes = 1).toOption.get))
        .compile.drain.attempt.unsafeRunSync()
      assertTrue(result.left.toOption.exists(_.isInstanceOf[ScanException]), seeds == 0, pulled == 0)
    }
  )

  private def valuesAttempt[I, O](scan: Scan[I, O], source: Stream[IO, I]): Either[Throwable, Unit] =
    source.through(scan.toPipe[IO]).compile.drain.attempt.unsafeRunSync()
