package zio.pdf

import zio.test.*

object ZPureLogSpec extends ZIOSpecDefault {

  def spec = suite("ZPureLog")(
    test("debug accumulates lines via ZPure.log") {
      val lines = ZPureLog.lines("hello")
      assertTrue(lines == zio.Chunk("hello"))
    },
    test("empty when diagnostics disabled at call sites") {
      assertTrue(ZPureLog.empty.isEmpty)
    }
  )
}
