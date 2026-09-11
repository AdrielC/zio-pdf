package zio.pdf.pipe

import volga.*
import PipeArrow.*
import PipeObjects.{Ob, U}
import zio.test.*

object CatSpec extends ZIOSpecDefault {

  private def ob[A]: Ob[A] = PipeObjects.ob[A]

  def spec: Spec[Any, Any] = suite("Cat")(
    test("pipeCartesian <> matches Pipe.fanOut") {
      given Ob[Int]               = ob[Int]
      given Ob[String]            = ob[String]
      given CartesianCat[Pipe, U] = PipeCat.pipeCartesian
      val f                       = Pipe[Int, Int](_ + 1)
      val g                       = Pipe[Int, String](i => s"$i")
      val inputs                  = (0 until 8).toList
      assertTrue(inputs.map(i => (f <> g).run(i)) == inputs.map(Pipe.fanOut(f, g).run))
    },
    test("pipeMonoidal >< matches par") {
      given Ob[Int]              = ob[Int]
      given Ob[String]           = ob[String]
      given MonoidalCat[Pipe, U] = PipeCat.pipeMonoidal
      val f                      = Pipe[Int, Int](_ * 2)
      val g                      = Pipe[String, String](_.reverse)
      val inputs                 = List(1 -> "ab", 2 -> "cd")
      assertTrue(inputs.map((f >< g).run) == inputs.map(i => Pipe.par(f, g).run(i)))
    },
    test("Pipe <> / >< sugar matches Cat operators") {
      val f = Pipe[Int, Int](_ + 1)
      val g = Pipe[Int, String](i => s"$i")
      val h = Pipe[String, String](_.reverse)
      assertTrue(
        (f <> g).run(7) == (f &&& g).run(7),
        (f >< h).run(3 -> "xy") == (f *** h).run(3 -> "xy")
      )
    },
    test("pipeCocartesian sum matches Pipe.sum") {
      given Ob[Int]    = ob[Int]
      given Ob[String] = ob[String]
      val f            = Pipe[Int, Int](_ + 1)
      val g                         = Pipe[String, Int](_.length)
      val leftInputs                = List[Either[Int, String]](Left(3), Left(10))
      val rightInputs               = List[Either[Int, String]](Right("ab"), Right("hello"))
      val merge = PipeCat.pipeCocartesian.sum(f, g)
      assertTrue(
        leftInputs.map(merge.run) == leftInputs.map(Pipe.sum(f, g).run),
        rightInputs.map(merge.run) == rightInputs.map(Pipe.sum(f, g).run)
      )
    },
    test("PipeArrow choice / ||| / +++ follow volga ArrChoice") {
      val double = Pipe[Int, Int](_ * 2)
      val plus10 = Pipe[Int, Int](_ + 10)
      val tag    = Pipe[Int, String](i => if i > 5 then "big" else "small")

      val routed = choose(double, tag)
      assertTrue(
        routed.run(Left(4)) == Left(8),
        routed.run(Right(7)) == Right("big")
      )

      val merged = double ||| plus10
      assertTrue(
        merged.run(Left(3)) == 6,
        merged.run(Right(3)) == 13
      )

      val disjoint = double +++ tag
      assertTrue(
        disjoint.run(Left(4)) == Left(8),
        disjoint.run(Right(4)) == Right("small")
      )
    },
    test("PipeArrow first / second thread one component") {
      val inc = Pipe[Int, Int](_ + 1)
      assertTrue(
        inc.first[String].run(3 -> "x") == (4 -> "x"),
        inc.second[String].run("x" -> 3) == ("x" -> 4)
      )
    },
    test("pipeDistributive distributeFrom matches manual Either split") {
      given Ob[Int]     = ob[Int]
      given Ob[String]  = ob[String]
      given Ob[Boolean] = ob[Boolean]
      val d             = PipeCat.pipeDistributive.distributeFrom[Int, String, Boolean]
      assertTrue(
        d.run((1, Left("a"))) == Left((1, "a")),
        d.run((1, Right(true))) == Right((1, true))
      )
    },
    test("FreePipe fold agrees with direct Pipe for fanout graph") {
      val fp =
        FreePipe.arr[Int, Int](_ + 1) &&& FreePipe.arr[Int, String](i => s"$i")
      val pipe = Pipe[Int, Int](_ + 1) &&& Pipe[Int, String](i => s"$i")
      val inputs = (0 until 8).toList
      assertTrue(inputs.map(fp.run) == inputs.map(pipe.run))
    },
    test("StateCont reassociates like FreeScan AndThen") {
      import zio.pdf.pipe.functors.State
      val prog =
        State.Success[Int, Int](1).flatMap(a => State.Success(a + 1)).flatMap(a => State.Success(a * 10))
      val (s, res) = prog.run(0)
      assertTrue(s == 0, res.toEither == Right(20))
    }
  )
}
