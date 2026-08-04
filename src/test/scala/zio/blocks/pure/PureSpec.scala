package zio.blocks.pure

import zio.blocks.chunk.Chunk
import zio.*
import zio.test.*

object PureSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("zio.blocks.pure.Pure")(
    test("log accumulates in runAll") {
      val p: Pure[String, Unit, Unit, Any, Nothing, Unit] =
        Pure.log("a") *> Pure.log("b")
      val (log, r) = p.runAll(())
      assertTrue(log == Chunk("a", "b"), r == Right(((), ())))
    },
    test("log stores Chunk entries") {
      val entry         = Chunk.single(42)
      val (log, result) = Pure.log(entry).runAll(())
      assertTrue(log.size == 1, log(0) == entry, result == Right(((), ())))
    },
    test("update threads Long state") {
      val step   = Pure.update[Long, Long](_ + 1L)
      val (_, r) = step.runAll(10L)
      assertTrue(r == Right((11L, ())))
    },
    test("Env provides services") {
      trait Greeter { def greet: String }
      val greeter: Greeter = new Greeter { def greet = "hi" }
      val p: Pure[Nothing, Unit, Unit, Greeter, Nothing, String] =
        Pure.serviceWith[Greeter](_.greet)
      val (_, r) = p.provideService(greeter).runAll(())
      assertTrue(r == Right(((), "hi")))
    },
    test("runValidation returns Validation") {
      val p: Pure[String, Unit, Unit, Any, Nothing, Int] =
        Pure.log("x") *> Pure.succeed(1)
      assertTrue(p.runValidation == Validation.Success(Chunk("x"), 1))
    },
    test("runAll is safe under parallel fibers") {
      val n = 128
      for {
        results <- ZIO.foreachPar(0 until n) { i =>
                     ZIO.succeed {
                       val p: Pure[Int, Long, Long, Any, Nothing, Long] =
                         Pure.log(i) *> Pure.update[Long, Long](_ + i) *> Pure.get[Long]
                       val (log, r) = p.runAll(0L)
                       (log, r)
                     }
                   }
      } yield assertTrue(
        results.length == n,
        results.forall { case (log, r) =>
          log.size == 1 && r.exists { case (s, a) => s == a && s == log(0).toLong }
        }
      )
    },
    test("nested runAll does not corrupt outer log") {
      val inner: Pure[String, Unit, Unit, Any, Nothing, Int] =
        Pure.log("inner") *> Pure.succeed(1)
      val outer: Pure[String, Unit, Unit, Any, Nothing, Int] =
        Pure.log("outer-before") *>
          Pure.succeed {
            val (ilog, ir) = inner.runAll(())
            require(ilog == Chunk("inner") && ir == Right(((), 1)))
            0
          } *>
          Pure.log("outer-after") *>
          Pure.succeed(2)
      val (log, r) = outer.runAll(())
      assertTrue(log == Chunk("outer-before", "outer-after"), r == Right(((), 2)))
    }
  )
}
