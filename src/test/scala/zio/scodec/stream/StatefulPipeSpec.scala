/*
 * Tests for StatefulPipe: the bridge between per-element ZPure
 * state-and-log work and a streaming ZPipeline.
 */

package zio.scodec.stream

import zio.*
import zio.prelude.fx.ZPure
import zio.stream.*
import zio.test.*

object StatefulPipeSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("StatefulPipe (ZPure -> ZPipeline)")(

    test("Step that just emits each input is identity") {
      val pipe = StatefulPipe[Int, Unit, Int]((), _ => ZPure.unit[Unit])(
        a => ZPure.log[Unit, Int](a)
      )
      for {
        out <- ZStream(1, 2, 3, 4, 5).via(pipe).runCollect
      } yield assertTrue(out == Chunk(1, 2, 3, 4, 5))
    },

    test("State threads correctly across chunks") {
      // Running sum: emit a running total after each input.
      val pipe = StatefulPipe[Int, Int, Int](0, _ => ZPure.unit[Int])(a =>
        ZPure.modify[Int, Int, Int](s => (s + a, s + a)).flatMap(t => ZPure.log[Int, Int](t))
      )
      for {
        out <- ZStream(1, 2, 3, 4).rechunk(1).via(pipe).runCollect
      } yield assertTrue(out == Chunk(1, 3, 6, 10))
    },

    test("Step that emits multiple outputs per input is fanned out") {
      val pipe = StatefulPipe[Int, Unit, Int]((), _ => ZPure.unit[Unit])(a =>
        ZPure.log[Unit, Int](a) *> ZPure.log[Unit, Int](a * 10)
      )
      for {
        out <- ZStream(1, 2).via(pipe).runCollect
      } yield assertTrue(out == Chunk(1, 10, 2, 20))
    },

    test("finalize emits trailing outputs after upstream is done") {
      val pipe = StatefulPipe[Int, Int, Int](
        initial = 0,
        finalize = (s: Int) => ZPure.log[Int, Int](s)
      )(a => ZPure.modify[Int, Int, Int](old => (old + a, old + a)).map(_ => ()))
      for {
        out <- ZStream(1, 2, 3).via(pipe).runCollect
      } yield assertTrue(out == Chunk(6))
    },

    test("ZPure.fail surfaces through the channel error channel") {
      val boom = new RuntimeException("boom")
      val pipe = StatefulPipe[Int, Unit, Int]((), _ => ZPure.unit[Unit])(a =>
        if (a == 3) ZPure.fail(boom) else ZPure.log[Unit, Int](a)
      )
      for {
        exit <- ZStream(1, 2, 3, 4).via(pipe).runCollect.exit
      } yield assert(exit)(Assertion.fails(Assertion.equalTo(boom)))
    },

    test("applyEffect runs the onDone hook with the final state") {
      for {
        ref  <- Ref.make(-1)
        pipe = StatefulPipe.applyEffect[Int, Int, Int](
                 initial = 0,
                 onDone = (s: Int) => ref.set(s)
               )(a => ZPure.modify[Int, Int, Int](old => (old + a, old + a)).map(_ => ()))
        _    <- ZStream(1, 2, 3, 4).via(pipe).runDrain
        seen <- ref.get
      } yield assertTrue(seen == 10)
    }
  )
}
