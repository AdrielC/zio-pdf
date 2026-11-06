package io.github.adrielc.bytescan

import SharedScan.*
import zio.test.*

object SharedScanSpec extends ZIOSpecDefault:
  private def block(bytes: Int*): Block = Block.copyOf(bytes.map(_.toByte).toArray).toOption.get
  private def session[E, A](scan: Probe[E, A], limits: Limits = Limits.default): Session[E, A] =
    Session.open(scan, limits).toOption.get
  private def checked[E, A](value: Either[E, A]): A = value match
    case Right(result) => result
    case Left(error) => throw AssertionError(error.toString)

  /** A non-PDF, length-prefixed protocol. Headers and payloads may cross any
    * input boundary. The payload is folded incrementally, never retained whole.
    */
  private enum FrameError:
    case TruncatedPayload
  private final case class Frames(remaining: Int = 0, total: Long = 0, count: Int = 0)
  private val frames = Probe.stateful[FrameError, Frames, (Int, Long)](Frames()) { (state, window, eof) =>
    var next = state
    checked(window.fold(window.position, window.end)(()) { (_, byte) =>
      if next.remaining == 0 then next = next.copy(remaining = byte & 255)
      else next = next.copy(remaining = next.remaining - 1, total = next.total + (byte & 255),
        count = next.count + (if next.remaining == 1 then 1 else 0))
    })
    if eof && next.remaining == 0 then Step.Done((next.count, next.total))
    else if eof then Step.Failed(FrameError.TruncatedPayload)
    else Step.Await(next, window.end, window.end)
  }

  def spec = suite("Generic shared byte scans")(
    test("eight MiB runs through composed register scans using one 64 KiB input owner") {
      import io.github.adrielc.scan.Scan
      val compiled = checked(Scan.count[Byte].broadcast(Scan.foldLong[Byte](0L)((sum, byte) => sum + byte)).compile())
      val probe = Probe.bytes(compiled, (0L, 0L)) { case ((count, sum), event) => event match
        case Left(value) => (value, sum)
        case Right(value) => (count, value)
      }
      val run = session(probe, checked(Limits.checked(retainedBytes = MaxBlockBytes)))
      val bytes = checked(Block.unsafeBorrowArray(Array.fill[Byte](MaxBlockBytes)(1)))
      (0 until 128).foreach(_ => checked(run.offer(bytes)))
      assertTrue(run.finish() == Right((8L * 1024 * 1024, 8L * 1024 * 1024)),
        run.stats.peakRetainedBytes == MaxBlockBytes, !bytes.isRetained,
        compiled.layout.longSlots == 2, compiled.layout.referenceSlots == 0)
    },
    test("a decoder feeds composed generic scans once across every input fragmentation") {
      import io.github.adrielc.scan.Scan
      var emitted = 0
      val facts = checked(Scan.map[Int, Int](value => { emitted += 1; value })
        .andThen(Scan.count[Int].broadcast(Scan.foldLong[Int](0L)((sum, value) => sum + value))).compile())
      val decoded = Probe.decode[String, (Int, Int), Int, Either[Long, Long], (Long, Long)]((0, 0), facts, (0L, 0L)) {
        (state, window, eof, emit) =>
          var remaining = state._1
          var total = state._2
          checked(window.fold(window.position, window.end)(()) { (_, byte) =>
            if remaining == 0 then
              remaining = byte & 255
              total = 0
              if remaining == 0 then emit(0)
            else
              total += byte & 255
              remaining -= 1
              if remaining == 0 then emit(total)
          })
          if eof && remaining != 0 then Step.Failed("truncated frame")
          else if eof then Step.Done(())
          else Step.Await((remaining, total), window.end, window.end)
      } { case ((count, sum), output) => output match
        case Left(value) => (value, sum)
        case Right(value) => (count, value)
      }
      val input = Vector[Byte](3, 10, 20, 30, 2, 40, 50)
      val results = (0 until 64).map { mask =>
        val run = session(decoded.zip(Probe.foldBytes(0)((sum, byte) => sum + byte)))
        var from = 0
        (1 to input.length).foreach { until =>
          if until == input.length || (mask & (1 << (until - 1))) != 0 then
            checked(run.offer(checked(Block.copyOf(input.slice(from, until).toArray))))
            from = until
        }
        val result = run.finish()
        (result, run.stats.retainedBytes)
      }
      assertTrue(results.forall(_ == (Right(((2L, 150L), 155)), 0L)), emitted == 128)
    },
    test("generic pipeline failures close the byte owner and release retained blocks") {
      import io.github.adrielc.scan.Scan
      val burst = checked(Scan.stateless[Byte, Byte]((byte, emit) => { emit(byte); emit(byte); emit(byte) })
        .compile(checked(Scan.Limits.checked(pending = 2))))
      val probe = Probe.bytes(burst, 0)((sum, byte) => sum + byte)
      val run = session(probe)
      val bytes = block(3)
      val result = run.offer(bytes)
      assertTrue(result == Left(Failure.Stage(0, PipelineIssue.Run(Scan.RunIssue.PendingLimit(2)))),
        run.isClosed, !bytes.isRetained)
    },
    test("a span separates the guarded owner range from reader progress") {
      var escaped: Option[io.github.adrielc.scan.Span[View]] = None
      var cursor: Option[Cursor] = None
      val probe = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, eof) =>
        if window.position < window.end then
          escaped = Some(checked(window.span.slice(window.position, window.end)))
          cursor = Some(window.cursor)
        if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      }
      val run = session(probe)
      checked(run.offer(block(1, 2)))
      checked(run.finish())
      assertTrue(escaped.get.length == 2, escaped.get.owner.read(0) == Left(AccessIssue.Expired),
        cursor.contains(Cursor(0, 0, 1)))
    },
    test("independent checks read the same borrowed block without a per-scan copy") {
      val bytes = Vector[Byte](1, 2, 3, 4)
      var reads = 0
      val input = checked(Block.borrowImmutable(bytes.length, bytes.length.toLong) { index =>
        reads += 1
        bytes(index)
      })
      val sum = Probe.foldBytes(0)((state, byte) => state + byte)
      val xor = Probe.foldBytes(0)((state, byte) => state ^ byte)
      val run = session(sum.zip(xor))
      checked(run.offer(input))
      val result = run.finish()
      assertTrue(result == Right((10, 4)), reads == bytes.length * 2,
        run.stats.peakRetainedBytes == bytes.length, !input.isRetained)
    },
    test("independent non-PDF scans reuse the source and parser maps share one machine") {
      var reads = 0
      var parserCalls = 0
      val input = Vector[Byte](3, 10, 20, 30, 2, 40, 50)
      val bytes = checked(Block.borrowImmutable(input.length, input.length.toLong) { index => reads += 1; input(index) })
      val parsed = frames.map { value => parserCalls += 1; value }
      val plan = parsed.map(_._1).zip(parsed.map(_._2)).zip(Probe.count)
      val run = session(plan)
      val offered = run.offer(bytes)
      val result = run.finish()
      assertTrue(offered == Right(None), result == Right(((2, 150L), 7L)), reads == input.length,
        parserCalls == 1, run.stats.peakRetainedBytes == input.length.toLong, !bytes.isRetained)
    },
    test("every chunk split preserves length-prefixed framing and the empty stream") {
      val input = Vector[Byte](3, 10, 20, 30, 2, 40, 50)
      val results = (0 until (1 << (input.length - 1))).map { mask =>
        val run = session(frames.zip(Probe.count))
        var from = 0
        (1 to input.length).foreach { until =>
          if until == input.length || (mask & (1 << (until - 1))) != 0 then
            checked(run.offer(checked(Block.copyOf(input.slice(from, until).toArray))))
            from = until
        }
        run.finish()
      }
      assertTrue(results.forall(_ == Right(((2, 150L), 7L))), session(Probe.count).finish() == Right(0L))
    },
    test("different lookahead demands retain only the bytes still needed") {
      val slow = Probe.stateful[Nothing, Long, Long](0L) { (sum, window, eof) =>
        val available = window.end - window.position
        if available < 4 && !eof then Step.Await(sum, window.position, window.start, 4)
        else
          val next = checked(window.fold(window.position, window.end)(sum)((a, b) => a + b))
          if eof then Step.Done(next) else Step.Await(next, window.end, window.end, 4)
      }
      val run = session(Probe.count.zip(slow))
      val first = block(1, 2)
      val second = block(3, 4)
      checked(run.offer(first))
      val held = first.isRetained && run.stats.retainedBytes == 2
      checked(run.offer(second))
      assertTrue(held, !first.isRetained, !second.isRetained, run.stats.retainedBytes == 0,
        run.stats.peakRetainedBytes == 4, run.finish() == Right((4L, 10L)))
    },
    test("prefix retention handles a delimiter across boundaries") {
      val delimiter = Probe.stateful[Nothing, Option[Long], Long](None) { (found, window, eof) =>
        var hit = found
        var position = window.start
        while position + 1 < window.end do
          if checked(window.read(position)) == 13 && checked(window.read(position + 1)) == 10 then hit = Some(position)
          position += 1
        hit match
          case Some(offset) => Step.Done(offset)
          case None if eof => Step.Done(-1L)
          case _ => Step.Await(None, window.end, math.max(window.start, window.end - 1))
      }
      val run = session(delimiter.zip(Probe.count))
      checked(run.offer(block(65, 13)))
      checked(run.offer(block(10, 66)))
      assertTrue(run.finish() == Right((1L, 4L)), run.stats.retainedBytes == 0)
    },
    test("escaped windows expire; explicit leases keep only their own segments") {
      var escaped: Option[Window] = None
      var lease: Option[Lease] = None
      val scanner = Probe.stateful[AccessIssue, Unit, Unit](()) { (_, window, eof) =>
        escaped = Some(window)
        if window.end > window.position && lease.isEmpty then lease = Some(checked(window.pin(window.position, window.position + 1)))
        if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      }
      val run = session(scanner)
      val first = block(7, 8)
      checked(run.offer(first))
      val expired = escaped.get.read(0) == Left(AccessIssue.Expired)
      checked(run.offer(block(9, 10)))
      checked(run.finish())
      val retained = run.stats.retainedBytes == 2 && first.isRetained && lease.get.read(0) == Right(7.toByte)
      lease.get.close()
      lease.get.close()
      assertTrue(expired, retained, run.stats.retainedBytes == 0, !first.isRetained,
        lease.get.read(0) == Left(AccessIssue.Expired))
    },
    test("backpressure rejects input without acquiring it and a pin release permits retry") {
      var held: Option[Lease] = None
      val scanner = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, eof) =>
        if window.position == 0 && window.end > 0 then held = Some(checked(window.pin(0, 1)))
        if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      }
      val limits = checked(Limits.checked(retainedBytes = 2))
      val run = session(scanner, limits)
      val first = block(1, 2)
      val second = block(3, 4)
      checked(run.offer(first))
      val rejected = run.offer(second)
      val untouched = !second.isRetained && run.stats.acceptedBytes == 2
      held.get.close()
      val retry = run.offer(second)
      assertTrue(rejected.left.exists { case Failure.Backpressure(_, _, _, _, _) => true; case _ => false }, untouched,
        retry == Right(None), run.finish() == Right(()), run.stats.peakRetainedBytes == 2)
    },
    test("metadata, pin, and backing-storage budgets are independently enforced") {
      val errors = Limits.checked(retainedBytes = 0, segments = 0, scans = 0, pins = -1).left.toOption.get
      val neverReady = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, _) =>
        Step.Await((), window.position, window.start, 16)
      }
      val run = session(neverReady, checked(Limits.checked(segments = 1)))
      checked(run.offer(block(1)))
      val segments = run.offer(block(2))
      run.close()
      val backing = checked(Block.borrowImmutable(1, 8)(_ => 1.toByte))
      val small = session(Probe.count, checked(Limits.checked(retainedBytes = 4)))
      val rejected = small.offer(backing)
      small.close()
      val noPins = Probe.stateful[Nothing, Unit, AccessIssue](()) { (_, window, _) =>
        if window.end == window.position then Step.Await((), window.end, window.end)
        else Step.Done(window.pin(window.position, window.end).left.toOption.get)
      }
      val pins = session(noPins, checked(Limits.checked(pins = 0)))
      val pinResult = pins.offer(block(1))
      assertTrue(errors.map(_.field) == Vector(LimitField.RetainedBytes, LimitField.Segments, LimitField.Scans, LimitField.Pins),
        segments.isLeft, rejected.isLeft, !backing.isRetained,
        pinResult == Right(Some(AccessIssue.PinLimit(0))))
    },
    test("safe ingress copies once; borrowed producer storage is released before reuse") {
      val array = Array[Byte](1, 2)
      val owned = checked(Block.copyOf(array))
      array(0) = 99
      val first = session(Probe.foldBytes(0)((sum, byte) => sum + byte))
      checked(first.offer(owned))
      val original = first.finish()
      val borrowed = checked(Block.unsafeBorrowArray(array))
      val second = session(Probe.foldBytes(0)((sum, byte) => sum + byte))
      checked(second.offer(borrowed))
      val released = !borrowed.isRetained
      array(0) = 3
      checked(second.offer(borrowed))
      assertTrue(original == Right(3), released, second.finish() == Right(106))
    },
    test("EOF and typed scan failures release retained storage") {
      val short = session(frames)
      val incomplete = block(3, 1)
      checked(short.offer(incomplete))
      val failed = short.finish()
      val waiting = session(Probe.stateful[String, Unit, Unit](()) { (_, window, _) =>
        Step.Await((), window.position, window.start, 4)
      })
      val held = block(1)
      checked(waiting.offer(held))
      val eof = waiting.finish()
      assertTrue(failed == Left(Failure.Stage(0, FrameError.TruncatedPayload)), !incomplete.isRetained,
        eof.left.exists { case Failure.UnexpectedEnd(_, _, _, _) => true; case _ => false }, !held.isRetained, waiting.stats.retainedBytes == 0)
    },
    test("invalid cursor contracts fail once instead of spinning") {
      val stuck = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, _) =>
        Step.Await((), window.position, window.start)
      }
      val run = session(stuck)
      val bytes = block(1)
      val failed = run.offer(bytes)
      assertTrue(failed == Left(Failure.Protocol(0, ProtocolIssue.NoProgress)), !bytes.isRetained)
    },
    test("sessions get fresh state; common stages and results are shared only within a run") {
      var states = 0
      val scanner = Probe.stateful[Nothing, Array[Int], Int]({ states += 1; Array(0) }) { (state, window, eof) =>
        state(0) += (window.end - window.position).toInt
        if eof then Step.Done(state(0)) else Step.Await(state, window.end, window.end)
      }
      val pair = scanner.zip(scanner)
      val one = session(pair)
      val two = session(pair)
      checked(one.offer(block(1, 2)))
      checked(two.offer(block(3)))
      assertTrue(states == 2, one.finish() == Right((2, 2)), two.finish() == Right((1, 1)))
    },
    test("logical Long offsets reject overflow without a large allocation") {
      val run = checked(Session.open(Probe.count, initialOffset = Long.MaxValue - 1))
      checked(run.offer(block(1)))
      val extra = block(2)
      val overflow = run.offer(extra)
      assertTrue(overflow == Left(Failure.OffsetOverflow(Long.MaxValue, 1)), !extra.isRetained,
        run.finish() == Right(1L), Block.copyOf(MaxBlockBytes + 1)(_ => 0.toByte).isLeft)
    },
    test("close invalidates output leases and releases storage once") {
      val capture = Probe.stateful[Nothing, Unit, Lease](()) { (_, window, _) =>
        if window.end == window.position then Step.Await((), window.end, window.end)
        else Step.Done(checked(window.pin(window.position, window.end)))
      }
      val run = session(capture)
      val bytes = block(1, 2)
      val lease = checked(run.offer(bytes)).get
      run.close()
      run.close()
      assertTrue(!bytes.isRetained, lease.read(0) == Left(AccessIssue.Expired), run.stats.releasedBlocks == 1,
        run.stats.retainedBytes == 0, run.offer(block(3)) == Left(Failure.Closed))
    },
    test("independent leases and repeated block views account backing storage once") {
      var lease: Option[Lease] = None
      val scanner = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, eof) =>
        if lease.isEmpty && window.end > 0 then lease = Some(checked(window.pin(0, 1)))
        if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      }
      val run = session(scanner, checked(Limits.checked(retainedBytes = 2)))
      val bytes = block(1, 2)
      checked(run.offer(bytes))
      val second = checked(lease.get.duplicate())
      checked(run.offer(bytes))
      checked(run.finish())
      lease.get.close()
      val stillValid = second.read(0) == Right(1.toByte) && run.stats.retainedBytes == 2
      second.close()
      assertTrue(stillValid, !bytes.isRetained, run.stats.retainedBytes == 0, run.stats.peakRetainedBytes == 2)
    },
    test("a bounded detached output releases a larger borrowed backing") {
      var reads = 0
      val bytes = checked(Block.borrowImmutable(2, 1024) { index => reads += 1; (index + 7).toByte })
      val scanner = Probe.stateful[AccessIssue, Unit, Block](()) { (_, window, _) =>
        if window.end == window.position then Step.Await((), window.end, window.end)
        else window.detach(window.position, window.position + 1) match
          case Right(copy) => Step.Done(copy)
          case Left(error) => Step.Failed(error)
      }
      val run = session(scanner)
      val detached = checked(run.offer(bytes)).get
      val verify = session(Probe.foldBytes(0)((sum, byte) => sum + byte))
      checked(verify.offer(detached))
      assertTrue(!bytes.isRetained, run.stats.retainedBytes == 0, run.stats.peakRetainedBytes == 1024,
        detached.retainedBytes == 1, reads == 1, verify.finish() == Right(7))
    },
    test("eight MiB passes through one reusable 64 KiB input block") {
      val backing = Array.fill[Byte](MaxBlockBytes)(1)
      val bytes = checked(Block.unsafeBorrowArray(backing))
      val run = session(Probe.count.zip(Probe.foldBytes(0L)((sum, byte) => sum + byte)),
        checked(Limits.checked(retainedBytes = MaxBlockBytes.toLong)))
      var released = true
      (0 until 128).foreach { _ =>
        checked(run.offer(bytes))
        released = released && !bytes.isRetained
      }
      assertTrue(released, run.finish() == Right((8L * 1024 * 1024, 8L * 1024 * 1024)),
        run.stats.peakRetainedBytes == MaxBlockBytes, run.stats.acceptedBlocks == 128, run.stats.releasedBlocks == 128)
    },
    test("plan size is checked before creating per-session scanner state") {
      var created = 0
      val stages = Vector.fill(MaxScans + 1)(Probe.stateful[Nothing, Unit, Unit]({ created += 1; () }) {
        (_, window, eof) => if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      })
      val graph = stages.foldLeft(Probe.succeed(()))((left, right) => left.zip(right).map(_ => ()))
      val result = Session.open(graph)
      assertTrue(result == Left(Failure.TooManyScans(MaxScans + 1, Limits.default.scans)), created == 0)
    },
    test("closing a session during a read defers backing release until the borrow ends") {
      var current: Option[Session[Nothing, Unit]] = None
      var input: Option[Block] = None
      var protectedDuringRead = false
      val scan = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, _) =>
        if window.end == window.position then Step.Await((), window.end, window.end)
        else
          val _ = window.fold(window.position, window.end)(()) { (_, _) =>
            current.get.close()
            protectedDuringRead = input.get.isRetained
          }
          Step.Done(())
      }
      val run = session(scan)
      current = Some(run)
      val bytes = block(1)
      input = Some(bytes)
      val result = run.offer(bytes)
      assertTrue(result == Left(Failure.Closed), protectedDuringRead, !bytes.isRetained)
    },
    test("a known blob length larger than Int needs only a Long counter") {
      val length = 1L << 40
      val scanner = Probe.stateful[Long, Long, Unit](length) { (remaining, window, eof) =>
        val next = remaining - (window.end - window.position)
        if eof then
          if next == 0 then Step.Done(()) else Step.Failed(next)
        else Step.Await(next, window.end, window.end)
      }
      val run = session(scanner)
      checked(run.offer(block(1)))
      assertTrue(run.finish() == Left(Failure.Stage(0, length - 1)), run.stats.peakRetainedBytes == 1)
    },
    test("closing during EOF returns a typed closed result") {
      var current: Option[Session[Nothing, Unit]] = None
      val scan = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, eof) =>
        if eof then
          current.get.close()
          Step.Done(())
        else Step.Await((), window.end, window.end)
      }
      val run = session(scan)
      current = Some(run)
      assertTrue(run.finish() == Left(Failure.Closed))
    }
  )
