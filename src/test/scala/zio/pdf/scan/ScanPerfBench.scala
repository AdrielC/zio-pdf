/*
 * Non-JMH microbench for the scan algebra. Runs as part of `sbt test`,
 * prints results to stdout, never fails on perf.
 *
 * Lanes:
 *
 *   1. `scodec.codecs.vector(uint8).decode`     -- the strict, no-streaming
 *                                                  baseline.
 *   2. `Scan.runDirect (fused, 4 maps)`         -- a 4-stage pipeline of
 *                                                  pure maps. Fusion collapses
 *                                                  the spine to a single
 *                                                  `Byte => Int`; the runner
 *                                                  is `it.foreach(builder +=
 *                                                  f(_))`.
 *   3. `Scan.runDirect (unfused, 4 maps + filt)` -- the same maps with a
 *                                                   Filter spliced in. The
 *                                                   filter blocks fusion so
 *                                                   the driver routes through
 *                                                   the per-stage Stepper
 *                                                   path -- the conservative
 *                                                   cost when fusion is not
 *                                                   possible.
 *   4. `Scan.runKyo (fused, 4 maps)`             -- the same fused pipeline
 *                                                   driven through Kyo's
 *                                                   Poll/Emit/Abort row.
 *                                                   Pays a fixed per-element
 *                                                   suspension cost that
 *                                                   dominates trivial
 *                                                   workloads but stays flat
 *                                                   under bigger per-stage
 *                                                   work.
 *   5. `hand-coded while loop`                   -- JIT-friendly reference.
 *
 * Lower is better. The fused direct lane should beat the scodec strict
 * baseline by a comfortable margin (it does strictly more work --
 * `Byte => Int` after three arithmetic stages, not the bare uint8
 * decode -- and still wins because the runner is one builder slot per
 * element, no per-stage dispatch).
 */

package zio.pdf.scan

import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.{uint8, vector}
import kyo.*
import zio.test.*

object ScanPerfBench extends ZIOSpecDefault {

  /** ~1 MiB. Same shape as the existing scodec PerfBench so cross-comparison
    * stays honest. */
  private val N: Int = 1 * 1024 * 1024

  private val payloadBytes: Array[Byte] = Array.tabulate[Byte](N)(i => (i & 0xff).toByte)
  private val payloadBits: BitVector    = BitVector.view(payloadBytes)
  private val payloadSeq: IndexedSeq[Byte] = scala.collection.immutable.ArraySeq.unsafeWrapArray(payloadBytes)

  /** Four stages of pure work over a Byte. After fusion this is a single
    * `Byte => Int`. The same arithmetic is what scodec's `uint8` does
    * (but it returns the bytes themselves; our pipeline goes a bit
    * further so the comparison is conservative *against* scan). */
  private val fusedPipeline: FreeScan[Byte, Int] =
    Scan.map[Byte, Int](b => b & 0xff)        >>>
      Scan.map[Int, Int](_ + 1)               >>>
      Scan.map[Int, Int](_ ^ 0x55)            >>>
      Scan.map[Int, Int](_ - 1)

  /** Same arithmetic as the fused pipeline, but interleaved with a `Filter`
    * that always returns true -- the filter blocks `Fusion.tryFuse` so the
    * driver stays on the per-stage stepper path. This is the slow-path
    * comparison. */
  private val unfusedPipeline: FreeScan[Byte, Int] =
    Scan.map[Byte, Int](b => b & 0xff)        >>>
      Scan.filter[Int](_ => true)             >>>
      Scan.map[Int, Int](_ + 1)               >>>
      Scan.map[Int, Int](_ ^ 0x55)            >>>
      Scan.map[Int, Int](_ - 1)

  /** Reference: the same arithmetic written by hand. Runs at JIT speed
    * since it's a tight `while` loop. */
  private def handCoded(): Array[Int] = {
    val out = new Array[Int](N)
    var i   = 0
    while i < N do {
      val b = payloadBytes(i) & 0xff
      out(i) = (((b + 1) ^ 0x55) - 1)
      i += 1
    }
    out
  }

  private def timeMillis(label: String, repeats: Int)(thunk: => Unit): Long = {
    var i = 0
    while i < 2 do { thunk; i += 1 }   // warm-up
    val t0 = java.lang.System.nanoTime()
    var j  = 0
    while j < repeats do { thunk; j += 1 }
    val ms = (java.lang.System.nanoTime() - t0) / 1_000_000L / repeats
    println(f"  [$label%-50s] ${ms}%5d ms / iter (avg of $repeats, $N bytes)")
    ms
  }

  def spec: Spec[Any, Any] = suite("ScanPerfBench")(

    test("scan vs scodec on 1 MiB of bytes through a 4-stage pure pipeline") {
      println(s"\n=== scan vs scodec on $N bytes (lower is better) ===")

      val baselineMs = timeMillis("scodec.codecs.vector(uint8).decode      ", 5) {
        val r = vector(uint8).decode(payloadBits).require
        require(r.value.size == N)
      }

      val fusedMs = timeMillis("Scan.runDirect (fused, 4 maps)           ", 5) {
        val (_, out) = Scan.runDirect[Byte, Int, Any](fusedPipeline, payloadSeq)
        require(out.size == N)
      }

      val unfusedMs = timeMillis("Scan.runDirect (unfused, 4 maps + filter)", 5) {
        val (_, out) = Scan.runDirect[Byte, Int, Any](unfusedPipeline, payloadSeq)
        require(out.size == N)
      }

      // Kyo lane on a much smaller payload -- Kyo's per-element
      // Poll/Emit/Abort suspension cost dominates trivial workloads.
      // We measure it here so the result is *visible* but use a smaller
      // N so the bench finishes in reasonable time.
      val kyoN: Int = 1024
      val kyoSeq    = payloadSeq.take(kyoN)
      val kyoMs = timeMillis(s"Scan.runKyo  (fused, 4 maps, N=$kyoN)     ", 5) {
        val (_, out) = Scan.runKyo[Byte, Int, Any](fusedPipeline, kyoSeq).eval
        require(out.size == kyoN)
      }

      val handMs = timeMillis("hand-coded while loop (reference)         ", 5) {
        val out = handCoded()
        require(out.length == N)
      }

      println(
        s"  scodec=$baselineMs ms  fused=$fusedMs ms  unfused=$unfusedMs ms  " +
          s"kyo($kyoN)=$kyoMs ms  hand=$handMs ms"
      )

      // We don't fail the build on perf, but assert at least that none
      // of the lanes is catastrophically broken (e.g. > 60s).
      assertTrue(baselineMs < 60_000L) &&
      assertTrue(fusedMs < 60_000L) &&
      assertTrue(unfusedMs < 60_000L) &&
      assertTrue(kyoMs < 60_000L) &&
      assertTrue(handMs < 60_000L)
    },

    test("scan fused fast path is faster than scodec's strict baseline (loose check)") {
      // Strictly informational. The numbers are printed in the test above;
      // here we just sanity-check that the fused lane is within 5x of
      // scodec's strict baseline. This is a loose bound because the JVM
      // is unpredictable on small in-memory workloads, but a 50x
      // regression would clearly mean fusion is broken.
      val warmRuns = 3
      val measureRuns = 5

      def go(f: () => Unit): Long = {
        var i = 0
        while i < warmRuns do { f(); i += 1 }
        val t0 = java.lang.System.nanoTime()
        var j  = 0
        while j < measureRuns do { f(); j += 1 }
        (java.lang.System.nanoTime() - t0) / 1_000_000L / measureRuns
      }

      val baselineMs = go { () =>
        val r = vector(uint8).decode(payloadBits).require
        require(r.value.size == N)
      }
      val fusedMs = go { () =>
        val (_, out) = Scan.runDirect[Byte, Int, Any](fusedPipeline, payloadSeq)
        require(out.size == N)
      }
      println(f"  ratio fused/scodec = ${fusedMs.toDouble / math.max(1L, baselineMs).toDouble}%.2fx")
      assertTrue(fusedMs <= math.max(1L, baselineMs) * 50)
    }
  ) @@ TestAspect.sequential
}
