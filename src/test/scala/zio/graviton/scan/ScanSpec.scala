package zio.graviton.scan

import kyo.*
import zio.test.*

object ScanSpec extends ZIOSpecDefault {

  def spec = suite("Graviton Scan")(
    suite("DirectScanRun")(
      test("Map emits One per input") {
        val scan = FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        assertTrue(DirectScanRun.runDirect(scan, List(1, 2, 3)) == List(2, 4, 6))
      },
      test("Filter drops odds") {
        val scan = FreeScan.Prim(ScanPrim.Filter[Int](_ % 2 == 0))
        assertTrue(DirectScanRun.runDirect(scan, List(1, 2, 3, 4, 5)) == List(2, 4))
      },
      test("map >>> map is correct") {
        val composed =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        assertTrue(DirectScanRun.runDirect(composed, List(1, 2, 3)) == List(4, 6, 8))
      },
      test("filter in pipeline") {
        val pipeline =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 3)) >>>
            FreeScan.Prim(ScanPrim.Filter[Int](_ % 2 == 0)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, String](_.toString))
        assertTrue(DirectScanRun.runDirect(pipeline, List(1, 2, 3, 4)) == List("6", "12"))
      },
      test("(a >>> b) >>> c == a >>> (b >>> c)") {
        val f = FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1))
        val g = FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        val h = FreeScan.Prim(ScanPrim.Map[Int, Int](_ - 3))
        val inputs = List(1, 2, 3, 4, 5)
        val la = DirectScanRun.runDirect(FreeScan.AndThen(FreeScan.AndThen(f, g), h), inputs)
        val ra = DirectScanRun.runDirect(FreeScan.AndThen(f, FreeScan.AndThen(g, h)), inputs)
        assertTrue(la == ra)
      },
      test("1000-deep left-nested AndThen does not blow stack") {
        val base = FreeScan.Prim(ScanPrim.Map[Int, Int](identity))
        val deep = (1 to 1000).foldLeft(base: FreeScan[Int, Int]) { (acc, _) =>
          FreeScan.AndThen(acc, FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)))
        }
        assertTrue(DirectScanRun.runDirect(deep, List(0)) == List(1000))
      },
      test("Fusion.tryFuse finds pure map chain") {
        val chain =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, String](_.toString))
        assertTrue(Fusion.tryFuse(chain).exists(_(3) == "8"))
      },
      test("Filter breaks fusion") {
        val chain =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)) >>>
            FreeScan.Prim(ScanPrim.Filter[Int](_ > 0))
        assertTrue(Fusion.tryFuse(chain).isEmpty)
      },
      test("fanout runs both branches") {
        val count = FreeScan.Prim(ScanPrim.Map[Int, Int](_ => 1))
        val dbl   = FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        val fan   = count &&& dbl
        assertTrue(DirectScanRun.runFanout(fan, List(5, 10)) == List((1, 10), (1, 20)))
      },
      test("BombGuard fails after limit") {
        val scan = FreeScan.Prim(ScanPrim.BombGuard(3L))
        val outcome =
          DirectScanRun.runDirectOutcome(scan, "hello".getBytes.map(_.toInt.toByte).toIndexedSeq)
        outcome match {
          case DirectScanRun.Failed(err, partial) =>
            assertTrue(err.seen == 4L && err.limit == 3L && partial.length == 3)
          case other =>
            assertNever(s"unexpected $other")
        }
      },
      test("Hash Sha256 flush matches one-shot digest") {
        import java.security.MessageDigest
        val bytes = "abc".getBytes
        val scan  = FreeScan.Prim(ScanPrim.Hash(HashAlgo.Sha256))
        val out   = DirectScanRun.runDirect(scan, bytes.toIndexedSeq)
        assertTrue(out.length == 1)
        val md = MessageDigest.getInstance("SHA-256")
        val expected = md.digest(bytes)
        assertTrue(out == expected.toList)
      }
    ),
    suite("ScanRunner (Kyo)")(
      test("mapProg doubles ints") {
        val prog = ScanPrograms.mapProg[Int, Int](_ * 2)
        given AllowUnsafe = AllowUnsafe.embrace.danger
        val (done, seq) = Sync.Unsafe.evalOrThrow {
          ScanRunner.run(Chunk.from(Array(1, 2, 3)))(prog)
        }
        assertTrue(done == ScanDone.success[Int] && seq == Seq(2, 4, 6))
      },
      test("bombGuardProg fails with BombError") {
        val prog =
          Var.run(0L)(ScanPrograms.bombGuardProg(3L))
        given AllowUnsafe = AllowUnsafe.embrace.danger
        val (done, seq) = Sync.Unsafe.evalOrThrow {
          ScanRunner.run(Chunk.from("hello".getBytes))(prog)
        }
        done match {
          case ScanDone.Failure(_: BombError, _) =>
            assertTrue(seq.length == 3)
          case _ =>
            assertNever(s"unexpected $done")
        }
      }
    )
  )
}
