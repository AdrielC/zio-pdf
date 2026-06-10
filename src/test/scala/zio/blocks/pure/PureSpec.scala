package zio.blocks.pure

import zio.blocks.chunk.Chunk
import zio.test.*

object PureSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Nothing] = suite("zio.blocks.pure.Pure")(
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
    }
  )
}
