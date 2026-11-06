package io.github.adrielc.bytescan.zio

import io.github.adrielc.bytescan.SharedScan.*
import _root_.zio.{Chunk, Promise, Ref, ZIO}
import _root_.zio.stream.ZStream
import _root_.zio.test.*

object ZioSharedScanSpec extends ZIOSpecDefault:
  private def get[E, A](value: Either[E, A]): A = value match
    case Right(result) => result
    case Left(error) => throw AssertionError(error.toString)
  private val first = Probe.stateful[AccessIssue, Unit, Byte](()) { (_, window, _) =>
    if window.position == window.end then Step.Await((), window.end, window.end)
    else window.read(window.position) match
      case Right(byte) => Step.Done(byte)
      case Left(error) => Step.Failed(error)
  }
  private enum SourceError:
    case Disconnected

  def spec = suite("Scoped shared scan transport")(
    test("a generic conditional chain runs through the shared-byte transport repeatedly") {
      import io.github.adrielc.scan.Scan
      val pipe = get(Scan.filter[Byte](_ % 2 == 0).andThen(Scan.map[Byte, Long](_.toLong * 10))
        .andThen(Scan.foldLong[Long](0L)((sum, value) => sum + value)).compile())
      val probe = Probe.bytes(pipe, 0L)((_, value) => value).zip(Probe.count)
      val source = ZStream.fromChunks(Chunk[Byte](1, 2), Chunk[Byte](3, 4))
      val program = ZIO.scoped(ZioSharedScan.runBorrowed(source, probe, _.length.toLong))
      for
        first <- program
        second <- program
      yield assertTrue(first == (60L, 4L), second == first)
    },
    test("owned and borrowed ZIO chunks drive the same generic combined plan") {
      val source = ZStream.fromChunks(Chunk[Byte](1, 2), Chunk.empty, Chunk[Byte](3, 4))
      val plan = Probe.count.zip(Probe.foldBytes(0)((sum, byte) => sum + byte))
      for
        copied <- ZIO.scoped(ZioSharedScan.run(source, plan))
        borrowed <- ZIO.scoped(ZioSharedScan.runBorrowed(source, plan, _.length.toLong))
        empty <- ZIO.scoped(ZioSharedScan.run(ZStream.empty, Probe.count))
      yield assertTrue(copied == (4L, 10), borrowed == copied, empty == 0L)
    },
    test("construction is lazy and an early result stops pulling and closes the input") {
      for
        pulls <- Ref.make(0)
        closed <- Ref.make(false)
        source = (ZStream.fromZIO(pulls.updateAndGet(_ + 1)).as(7.toByte) ++
          ZStream.fromZIO(pulls.updateAndGet(_ + 1)).as(8.toByte)).ensuring(closed.set(true))
        program = ZIO.scoped(ZioSharedScan.run(source, first))
        before <- pulls.get
        result <- program
        after <- pulls.get
        released <- closed.get
      yield assertTrue(before == 0, after == 1, result == 7.toByte, released)
    },
    test("a plan that already has a result never acquires the input") {
      for
        acquired <- Ref.make(0)
        source = ZStream.scoped(acquired.updateAndGet(_ + 1)).as(1.toByte)
        result <- ZIO.scoped(ZioSharedScan.run(source, Probe.succeed("ready")))
        count <- acquired.get
      yield assertTrue(result == "ready", count == 0)
    },
    test("reusing a ZIO program acquires fresh sessions and scanner state") {
      val source = ZStream.fromChunk(Chunk[Byte](1, 2, 3))
      val program = ZIO.scoped(ZioSharedScan.run(source, Probe.count))
      for
        first <- program
        second <- program
      yield assertTrue(first == 3L, second == 3L)
    },
    test("source failures invalidate retained leases before returning the error") {
      var lease: Option[Lease] = None
      val waiting = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, _) =>
        if lease.isEmpty && window.end > window.position then lease = Some(get(window.pin(window.position, window.end)))
        Step.Await((), window.end, window.start)
      }
      val source = ZStream.fromChunk(Chunk[Byte](1, 2)) ++ ZStream.fail(SourceError.Disconnected)
      for
        observed <- ZIO.scoped(ZioSharedScan.run(source, waiting).either.map(result => (result, lease.exists(_.isValid))))
      yield assertTrue(observed._1 == Left(ZioSharedScan.RunError.Input(SourceError.Disconnected)), !observed._2)
    },
    test("interruption releases pinned blocks and input resources") {
      var lease: Option[Lease] = None
      val waiting = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, _) =>
        if lease.isEmpty && window.end > window.position then lease = Some(get(window.pin(window.position, window.end)))
        Step.Await((), window.end, window.start)
      }
      for
        reached <- Promise.make[Nothing, Unit]
        closed <- Ref.make(false)
        source = (ZStream.fromChunk(Chunk[Byte](1, 2)) ++
          ZStream.fromZIO(reached.succeed(())).drain ++ ZStream.never).ensuring(closed.set(true))
        fiber <- ZIO.scoped(ZioSharedScan.run(source, waiting)).fork
        _ <- reached.await
        before <- ZIO.succeed(lease.exists(_.isValid))
        _ <- fiber.interrupt
        after <- ZIO.succeed(lease.exists(_.isValid))
        released <- closed.get
      yield assertTrue(before, !after, released)
    },
    test("callback defects still release borrowed windows and leases") {
      var lease: Option[Lease] = None
      val broken = Probe.stateful[Nothing, Unit, Unit](()) { (_, window, _) =>
        if window.end == window.position then Step.Await((), window.end, window.end)
        else
          lease = Some(get(window.pin(window.position, window.end)))
          throw IllegalStateException("synthetic scanner defect")
      }
      for
        exit <- ZIO.scoped(ZioSharedScan.run(ZStream.fromChunk(Chunk[Byte](1)), broken)).exit
      yield assertTrue(exit.isFailure, !lease.exists(_.isValid))
    },
    test("oversized transport chunks fail before pulling more input") {
      for
        extra <- Ref.make(0)
        source = ZStream.fromChunk(Chunk.fromArray(Array.fill[Byte](MaxBlockBytes + 1)(1))) ++
          ZStream.fromZIO(extra.updateAndGet(_ + 1)).as(2.toByte)
        result <- ZIO.scoped(ZioSharedScan.run(source, Probe.count)).either
        count <- extra.get
      yield assertTrue(result.left.exists {
        case ZioSharedScan.RunError.Block(issues) => issues.contains(BlockIssue.Length(MaxBlockBytes + 1, MaxBlockBytes))
        case _ => false
      }, count == 0)
    },
    test("borrowed slices charge their backing cost; owned ingress can detach them") {
      val source = ZStream.fromChunk(Chunk[Byte](1))
      val limits = get(Limits.checked(retainedBytes = 4))
      for
        borrowed <- ZIO.scoped(ZioSharedScan.runBorrowed(source, Probe.count, _ => 8L, limits)).either
        copied <- ZIO.scoped(ZioSharedScan.run(source, Probe.count, limits))
      yield assertTrue(borrowed.left.exists {
        case ZioSharedScan.RunError.Scan(Failure.Backpressure(_, incoming, _, _, _)) => incoming == 8
        case _ => false
      }, copied == 1L)
    },
    test("leased results stay readable inside their scope and expire on scope exit") {
      val scanner = Probe.stateful[AccessIssue, Unit, Lease](()) { (_, window, _) =>
        if window.end == window.position then Step.Await((), window.end, window.end)
        else window.pin(window.position, window.end) match
          case Right(value) => Step.Done(value)
          case Left(error) => Step.Failed(error)
      }
      for
        observed <- ZIO.scoped(ZioSharedScan.run(ZStream.fromChunk(Chunk[Byte](9)), scanner)
          .map(lease => (lease, lease.read(0))))
      yield assertTrue(observed._2 == Right(9.toByte), observed._1.read(0) == Left(AccessIssue.Expired))
    },
    test("eight MiB streams through both transport ownership modes") {
      val chunk = Chunk.fromArray(Array.fill[Byte](MaxBlockBytes)(1))
      val source = ZStream.fromIterable(0 until 128).flatMap(_ => ZStream.fromChunk(chunk))
      val scan = Probe.count.zip(Probe.foldBytes(0L)((sum, byte) => sum + byte))
      for
        owned <- ZIO.scoped(ZioSharedScan.run(source, scan))
        borrowed <- ZIO.scoped(ZioSharedScan.runBorrowed(source, scan, _ => MaxBlockBytes.toLong))
      yield assertTrue(owned == (8L * 1024 * 1024, 8L * 1024 * 1024), borrowed == owned)
    }
  )
