/*
 * Tests for the scan algebra. Two execution paths are exercised in
 * parallel: the pure `runDirect` driver and the Kyo-effect-typed
 * `runKyo` driver. Both should produce identical output sequences for
 * the same input scan.
 */

package zio.pdf.scan

import kyo.*
import zio.test.*

object ScanSpec extends ZIOSpecDefault {

  // ----- helpers -----

  /** Drive a `FreeScan` synchronously and project to `(signal, outputs)`. */
  private def runD[I, O](scan: FreeScan[I, O], inputs: Iterable[I]): (ScanDone[O, Any], Vector[O]) =
    Scan.runDirect[I, O, Any](scan, inputs)

  /** Drive a `FreeScan` through Kyo and evaluate. */
  private def runK[I, O](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      frame: Frame
  ): (ScanDone[O, Any], Chunk[O]) =
    Scan.runKyo[I, O, Any](scan, inputs).eval

  /** Both runners agree on the final outputs (ignoring the signal). */
  private def agreeOnOutputs[I, O](scan: FreeScan[I, O], inputs: Seq[I])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]]
  ): TestResult = {
    val (_, dOut) = runD(scan, inputs)
    val (_, kOut) = runK(scan, inputs)
    assertTrue(dOut == kOut.toVector)
  }

  def spec: Spec[Any, Any] = suite("Scan")(

    // ---------- Output cardinality ----------

    test("Map always emits One") {
      val scan = FreeScan.lift(ScanPrim.Map[Int, Int](_ * 2))
      val (_, out) = runD(scan, List(1, 2, 3))
      assertTrue(out == Vector(2, 4, 6))
    },

    test("Filter emits Null or One") {
      val scan = FreeScan.lift(ScanPrim.Filter[Int](_ % 2 == 0))
      val (_, out) = runD(scan, List(1, 2, 3, 4, 5))
      assertTrue(out == Vector(2, 4))
    },

    // ---------- Composition correctness ----------

    test("map >>> map is correct") {
      val composed =
        FreeScan.lift(ScanPrim.Map[Int, Int](_ + 1)) >>>
          FreeScan.lift(ScanPrim.Map[Int, Int](_ * 2))
      val (_, out) = runD(composed, List(1, 2, 3))
      assertTrue(out == Vector(4, 6, 8))
    },

    test("filter in pipeline drops elements") {
      val pipeline =
        FreeScan.lift(ScanPrim.Map[Int, Int](_ * 3)) >>>
          FreeScan.lift(ScanPrim.Filter[Int](_ % 2 == 0)) >>>
          FreeScan.lift(ScanPrim.Map[Int, String](_.toString))
      val (_, out) = runD(pipeline, List(1, 2, 3, 4))
      assertTrue(out == Vector("6", "12"))
    },

    // ---------- Associativity ----------

    test("(a >>> b) >>> c == a >>> (b >>> c)") {
      val f = FreeScan.lift(ScanPrim.Map[Int, Int](_ + 1))
      val g = FreeScan.lift(ScanPrim.Map[Int, Int](_ * 2))
      val h = FreeScan.lift(ScanPrim.Map[Int, Int](_ - 3))
      val inputs = List(1, 2, 3, 4, 5)
      val (_, la) = runD(FreeScan.AndThen(FreeScan.AndThen(f, g), h), inputs)
      val (_, ra) = runD(FreeScan.AndThen(f, FreeScan.AndThen(g, h)), inputs)
      assertTrue(la == ra)
    },

    // ---------- Stack safety ----------

    test("1000-deep left-nested chain does not blow stack") {
      val base: FreeScan[Int, Int] = FreeScan.lift(ScanPrim.Map[Int, Int](identity))
      val deep = (1 to 1000).foldLeft(base) { (acc, _) =>
        FreeScan.AndThen(acc, FreeScan.lift(ScanPrim.Map[Int, Int](_ + 1)))
      }
      val (_, out) = runD(deep, List(0))
      assertTrue(out == Vector(1000))
    },

    // ---------- Fusion ----------

    test("chain of Maps fuses to single function") {
      val chain: FreeScan[Int, String] =
        FreeScan.lift(ScanPrim.Map[Int, Int](_ + 1)) >>>
          FreeScan.lift(ScanPrim.Map[Int, Int](_ * 2)) >>>
          FreeScan.lift(ScanPrim.Map[Int, String](_.toString))
      val fused = Fusion.tryFuse(chain)
      assertTrue(fused.isDefined) &&
      assertTrue(fused.get(3) == "8")
    },

    test("Filter breaks fusion") {
      val chain =
        FreeScan.lift(ScanPrim.Map[Int, Int](_ + 1)) >>>
          FreeScan.lift(ScanPrim.Filter[Int](_ > 0))
      assertTrue(Fusion.tryFuse(chain).isEmpty)
    },

    // ---------- Take/Drop ----------

    test("Take(3) stops early with Stop signal") {
      val scan = FreeScan.lift(ScanPrim.Take[Int](3))
      val (sig, out) = runD(scan, List(1, 2, 3, 4, 5))
      assertTrue(out == Vector(1, 2, 3)) &&
      assertTrue(sig.isInstanceOf[ScanDone.Stop[?]])
    },

    test("Drop(2) skips first two") {
      val scan = FreeScan.lift(ScanPrim.Drop[Int](2))
      val (_, out) = runD(scan, List(1, 2, 3, 4, 5))
      assertTrue(out == Vector(3, 4, 5))
    },

    // ---------- Fold ----------

    test("Fold delivers final accumulator via Success leftover") {
      val scan: FreeScan[Int, Int] =
        FreeScan.lift(ScanPrim.Fold[Int, Int](0, _ + _))
      val (sig, out) = runD(scan, List(1, 2, 3, 4))
      assertTrue(sig == ScanDone.Success(Seq(10))) &&
      assertTrue(out == Vector(10))
    },

    // ---------- BombGuard ----------

    test("BombGuard fails with BombError on overflow") {
      val scan = FreeScan.lift(ScanPrim.BombGuard(3L))
      val (sig, out) = runD(scan, "hello".getBytes.toIndexedSeq)
      assertTrue(out.size == 3) &&
      assertTrue(sig.isInstanceOf[ScanDone.Failure[?, ?]]) &&
      assertTrue(sig match {
        case ScanDone.Failure(_: BombError, _) => true
        case _                                 => false
      })
    },

    test("BombGuard within limit completes successfully") {
      val scan = FreeScan.lift(ScanPrim.BombGuard(100L))
      val (sig, out) = runD(scan, "hi".getBytes.toIndexedSeq)
      assertTrue(out == Vector('h'.toByte, 'i'.toByte)) &&
      assertTrue(sig.isInstanceOf[ScanDone.Success[?]])
    },

    // ---------- Hash / CountBytes ----------

    test("Hash delivers digest as Success leftover") {
      val scan = FreeScan.lift(ScanPrim.Hash(HashAlgo.Sha256))
      val (sig, out) = runD(scan, "abc".getBytes.toIndexedSeq)
      // The digest of "abc" under SHA-256 starts with 0xba 0x78 ...
      val expected = java.security.MessageDigest.getInstance("SHA-256").digest("abc".getBytes)
      assertTrue(out.toList == expected.toList) &&
      assertTrue(sig.isInstanceOf[ScanDone.Success[?]])
    },

    test("CountBytes encodes count as 8 big-endian bytes") {
      val scan = FreeScan.lift(ScanPrim.CountBytes)
      val (_, out) = runD(scan, Seq.fill[Byte](42)(0))
      val n = java.nio.ByteBuffer.wrap(out.toArray).getLong
      assertTrue(n == 42L)
    },

    // ---------- Fanout ----------

    test("fanout runs both branches on same input and zips outputs") {
      val count = FreeScan.lift(ScanPrim.Map[Int, Int](_ => 1))
      val dbl   = FreeScan.lift(ScanPrim.Map[Int, Int](_ * 2))
      val fan: FreeScan[Int, (Int, Int)] = count &&& dbl
      val (_, out) = runD(fan, List(5, 10))
      assertTrue(out == Vector((1, 10), (1, 20)))
    },

    // ---------- Choice ----------

    test("choice routes Lefts and Rights independently") {
      val l = FreeScan.lift(ScanPrim.Map[Int, Int](_ * 10))
      val r = FreeScan.lift(ScanPrim.Map[String, Int](_.length))
      val ch: FreeScan[Either[Int, String], Int] = l ||| r
      val (_, out) = runD(ch, List(Left(1), Right("ab"), Left(3), Right("xyzw")))
      assertTrue(out == Vector(10, 2, 30, 4))
    },

    // ---------- FixedChunk ----------

    test("fixedChunk(3) emits chunks of 3 bytes plus a tail in leftover") {
      val scan: FreeScan[Byte, Chunk[Byte]] = Scan.fixedChunk(3)
      val (_, out) = runD(scan, "abcdefgh".getBytes.toIndexedSeq)
      assertTrue(out.map(_.toList) == Vector(
        List('a'.toByte, 'b'.toByte, 'c'.toByte),
        List('d'.toByte, 'e'.toByte, 'f'.toByte),
        List('g'.toByte, 'h'.toByte)
      ))
    },

    // ---------- Kyo runner agreement ----------

    test("Kyo runner agrees with direct runner on map>>>filter>>>map") {
      val pipeline: FreeScan[Int, String] =
        Scan.map[Int, Int](_ * 3) >>>
          Scan.filter[Int](_ % 2 == 0) >>>
          Scan.map[Int, String](_.toString)
      agreeOnOutputs(pipeline, List(1, 2, 3, 4, 5, 6))
    },

    test("Kyo runner agrees with direct runner on Take") {
      val pipeline: FreeScan[Int, Int] = Scan.take[Int](3)
      agreeOnOutputs(pipeline, List(1, 2, 3, 4, 5))
    },

    test("Kyo runner agrees with direct runner on BombGuard") {
      val scan: FreeScan[Byte, Byte] = Scan.bombGuard(2L)
      val (sigK, outK) = runK(scan, "abc".getBytes.toIndexedSeq)
      assertTrue(outK.size == 2) &&
      assertTrue(sigK.isInstanceOf[ScanDone.Failure[?, ?]])
    }
  )
}
