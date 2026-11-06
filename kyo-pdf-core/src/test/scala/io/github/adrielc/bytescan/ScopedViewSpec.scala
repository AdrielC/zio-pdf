package io.github.adrielc.bytescan

import scala.annotation.experimental
import io.github.adrielc.bytescan.capture.ScopedView
import SharedScan.*
import zio.test.*

/** Runtime guard coverage for unchecked interoperability. The separate compiler
  * fixtures prove these escapes are rejected for capture-checked callers.
  */
@experimental
object ScopedViewSpec extends ZIOSpecDefault:
  def spec = suite("Scoped byte views")(
    test("borrowed reads and owned detach work, while unchecked escapes expire immediately") {
      var escaped: Option[ScopedView.Borrowed] = None
      var read: Either[AccessIssue, Byte] = Left(AccessIssue.Expired)
      var detached: Option[Block] = None
      val probe = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, eof) =>
        if window.position < window.end then
          ScopedView.use(window) { borrow =>
            escaped = Some(borrow)
            read = borrow.read(borrow.from)
            detached = borrow.detach(borrow.from, borrow.until).toOption
          }
        if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      }
      val run = Session.open(probe).toOption.get
      val input = Block.copyOf(Array[Byte](7)).toOption.get
      val offered = run.offer(input)
      val ended = run.finish()
      val verify = Session.open(Probe.foldBytes(0)((sum, byte) => sum + byte)).toOption.get
      val copied = verify.offer(detached.get)
      assertTrue(offered == Right(None), ended == Right(()), copied == Right(None), read == Right(7.toByte),
        escaped.get.read(0) == Left(AccessIssue.Expired), !input.isRetained, verify.finish() == Right(7))
    }
  )
