package zio.graviton.scan

import kyo.*
import zio.test.*

/** Exercises [[ScanRunner]] / [[ScanPrograms]] against real Kyo handlers (unsafe eval). */
object ScanKyoSpec extends ZIOSpecDefault:

  given AllowUnsafe = AllowUnsafe.embrace.danger

  def spec = suite("Graviton Scan (Kyo runner)")(
    test("BombGuard via ScanRunner matches emitted byte count") {
      val prog = ScanPrograms.bombGuardProg(3L)
      val pair = Sync.Unsafe.evalOrThrow:
        ScanRunner.run("hello".getBytes.toList)(prog)
      val (done, emitted) = pair
      assertTrue(done.isInstanceOf[ScanDone.Failure[?, ?]], emitted.size == 3)
    },
    test("Take via ScanRunner emits first n values then Stop") {
      val prog = ScanPrograms.takeProg[Int](3)
      val pair = Sync.Unsafe.evalOrThrow:
        ScanRunner.run(List.range(0, 10))(prog)
      val (done, emitted) = pair
      assertTrue(done.isInstanceOf[ScanDone.Stop[?, ?]], emitted == List(0, 1, 2))
    }
  )
