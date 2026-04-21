package zio.graviton.scan

import zio.blocks.schema.Schema
import zio.graviton.scan.ScanDone as SD
import zio.test.*

object ScanSpec extends ZIOSpecDefault:

  def spec = suite("Graviton Scan")(
    suite("output cardinality")(
      test("Map always emits One") {
        val scan = FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        assertTrue(PureScanEval.runDirect(scan, List(1, 2, 3)) == List(2, 4, 6))
      },
      test("Filter emits Null or One") {
        val scan = FreeScan.Prim(ScanPrim.Filter[Int](_ % 2 == 0))
        assertTrue(PureScanEval.runDirect(scan, List(1, 2, 3, 4, 5)) == List(2, 4))
      }
    ),
    suite("composition correctness")(
      test("map >>> map is correct") {
        val composed =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        assertTrue(PureScanEval.runDirect(composed, List(1, 2, 3)) == List(4, 6, 8))
      },
      test("filter in pipeline drops elements") {
        val pipeline =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 3)) >>>
            FreeScan.Prim(ScanPrim.Filter[Int](_ % 2 == 0)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, String](_.toString))
        assertTrue(PureScanEval.runDirect(pipeline, List(1, 2, 3, 4)) == List("6", "12"))
      }
    ),
    suite("associativity")(
      test("(a >>> b) >>> c == a >>> (b >>> c)") {
        val f = FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1))
        val g = FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        val h = FreeScan.Prim(ScanPrim.Map[Int, Int](_ - 3))
        val inputs = List(1, 2, 3, 4, 5)
        val la = PureScanEval.runDirect(FreeScan.AndThen(FreeScan.AndThen(f, g), h), inputs)
        val ra = PureScanEval.runDirect(FreeScan.AndThen(f, FreeScan.AndThen(g, h)), inputs)
        assertTrue(la == ra)
      }
    ),
    suite("stack safety")(
      test("1000-deep left-nested chain does not blow stack") {
        val base = FreeScan.Prim(ScanPrim.Map[Int, Int](identity))
        val deep = (1 to 1000).foldLeft(base: FreeScan[Int, Int]) { (acc, _) =>
          FreeScan.AndThen(acc, FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)))
        }
        assertTrue(PureScanEval.runDirect(deep, List(0)) == List(1000))
      }
    ),
    suite("fusion")(
      test("chain of Maps fuses to single function") {
        val chain =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2)) >>>
            FreeScan.Prim(ScanPrim.Map[Int, String](_.toString))
        assertTrue(Fusion.tryFuse(chain).isDefined)
        assertTrue(Fusion.tryFuse(chain).get(3) == "8")
      },
      test("Filter breaks fusion") {
        val chain =
          FreeScan.Prim(ScanPrim.Map[Int, Int](_ + 1)) >>>
            FreeScan.Prim(ScanPrim.Filter[Int](_ > 0))
        assertTrue(Fusion.tryFuse(chain).isEmpty)
      }
    ),
    suite("fanout")(
      test("fanout runs both branches on same input") {
        val count = FreeScan.Prim(ScanPrim.Map[Int, Int](_ => 1))
        val dbl   = FreeScan.Prim(ScanPrim.Map[Int, Int](_ * 2))
        val fan   = count &&& dbl
        assertTrue(PureScanEval.runDirect(fan, List(5, 10)) == List((1, 10), (1, 20)))
      }
    ),
    suite("ScanDone")(
      test("BombGuard fails with BombError on overflow") {
        val scan = FreeScan.Prim(ScanPrim.BombGuard(3L))
        val r    = PureScanEval.runWithDone(scan, "hello".getBytes.toList)
        assertTrue(r.done.isInstanceOf[SD.Failure[?, ?]])
        assertTrue(r.emitted.size == 3)
      },
      test("Take(3) stops early with Stop") {
        val scan = FreeScan.Prim(ScanPrim.Take[Int](3))
        val r    = PureScanEval.runWithDone(scan, List.range(0, 10))
        assertTrue(r.done.isInstanceOf[SD.Stop[?, ?]])
        assertTrue(r.emitted == List(0, 1, 2))
      }
    ),
    suite("zio-blocks-schema")(
      test("KeyedBlock derives a non-null Schema") {
        val s = summon[Schema[KeyedBlock]]
        assertTrue(s ne null, s.toString.contains("KeyedBlock"))
      }
    )
  )
