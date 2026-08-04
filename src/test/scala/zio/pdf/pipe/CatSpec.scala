package zio.pdf.pipe

import PipeObjects.{Ob, U}
import zio.test.*

object CatSpec extends ZIOSpecDefault {

  private def ob[A]: Ob[A] = PipeObjects.ob[A]

  def spec: Spec[Any, Any] = suite("Cat")(
    test("pipeCartesian <> matches Pipe.fanOut") {
      given Ob[Int]                 = ob[Int]
      given Ob[String]              = ob[String]
      given CartesianCat[Pipe, U]   = PipeCat.pipeCartesian
      val f                         = Pipe[Int, Int](_ + 1)
      val g                         = Pipe[Int, String](i => s"$i")
      val inputs                    = (0 until 8).toList
      assertTrue(inputs.map(i => (f <> g).run(i)) == inputs.map(Pipe.fanOut(f, g).run))
    },
    test("pipeMonoidal >< matches par") {
      given Ob[Int]               = ob[Int]
      given Ob[String]            = ob[String]
      given MonoidalCat[Pipe, U]  = PipeCat.pipeMonoidal
      val f                       = Pipe[Int, Int](_ * 2)
      val g                       = Pipe[String, String](_.reverse)
      val inputs                  = List(1 -> "ab", 2 -> "cd")
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
    test("StateCont reassociates like FreeScan AndThen") {
      import zio.pdf.pipe.functors.State
      val prog =
        State.Success[Int, Int](1).flatMap(a => State.Success(a + 1)).flatMap(a => State.Success(a * 10))
      val (s, res) = prog.run(0)
      assertTrue(s == 0, res.toEither == Right(20))
    }
  )
}
