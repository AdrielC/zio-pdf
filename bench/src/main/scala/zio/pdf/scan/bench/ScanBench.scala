/*
 * JMH benchmarks for the Graviton Scan algebra.
 *
 * Run from sbt:
 *
 *   sbt "bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 .*ScanBench.*"
 *
 * Or for a quick smoke run:
 *
 *   sbt "bench/Jmh/run -i 2 -wi 1 -f 1 -t 1 .*ScanBench.*"
 *
 * The benchmarks measure the same workload across four lanes:
 *
 *   1. `scodecBaseline`            -- `vector(uint8).decode(bits)`. The
 *                                     reference for "decode N bytes
 *                                     into something boxed".
 *   2. `scanFusedDirect`           -- `Scan.runDirect` over a 4-stage
 *                                     pure pipeline. Fusion collapses
 *                                     the pipeline to a single
 *                                     `Byte => Int`; the runner does
 *                                     `it.foreach(builder += f(_))`.
 *   3. `scanUnfusedDirect`         -- the same 4 maps with a Filter
 *                                     spliced in, which blocks fusion.
 *                                     Measures the per-stage Stepper
 *                                     dispatch cost.
 *   4. `scanFusedKyo`              -- `Scan.runKyo` over the same fused
 *                                     pipeline. Adds Kyo's
 *                                     Poll/Emit/Abort plumbing on top
 *                                     of the fast path.
 *   5. `handCoded`                 -- a tight `while`-loop reference
 *                                     baseline, JIT-friendly.
 *
 * The arithmetic in each stage is `b => b & 0xff`, then `+1`, `^0x55`,
 * `-1`. Same per-element work in all lanes (modulo the per-stage
 * dispatch cost in the unfused lane).
 */

package zio.pdf.scan.bench

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.{uint8, vector}

import scala.compiletime.uninitialized

import zio.pdf.scan.*

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class ScanBench {

  /** ~1 MiB. Big enough to amortise per-call overhead, small enough to
    * keep JMH iterations fast. */
  @Param(Array("1048576"))
  var n: Int = uninitialized

  private var bytes: Array[Byte]              = uninitialized
  private var bits: BitVector                 = uninitialized
  private var bytesSeq: IndexedSeq[Byte]      = uninitialized

  private val fused: FreeScan[Byte, Int] =
    Scan.map[Byte, Int](b => b & 0xff)        >>>
      Scan.map[Int, Int](_ + 1)               >>>
      Scan.map[Int, Int](_ ^ 0x55)            >>>
      Scan.map[Int, Int](_ - 1)

  private val unfused: FreeScan[Byte, Int] =
    Scan.map[Byte, Int](b => b & 0xff)        >>>
      Scan.filter[Int](_ => true)             >>>
      Scan.map[Int, Int](_ + 1)               >>>
      Scan.map[Int, Int](_ ^ 0x55)            >>>
      Scan.map[Int, Int](_ - 1)

  @Setup(Level.Trial)
  def setup(): Unit = {
    bytes    = Array.tabulate[Byte](n)(i => (i & 0xff).toByte)
    bits     = BitVector.view(bytes)
    bytesSeq = scala.collection.immutable.ArraySeq.unsafeWrapArray(bytes)
  }

  // -------------------------------------------------------------------
  // strict baselines
  // -------------------------------------------------------------------

  @Benchmark
  def scodecBaseline: Vector[Int] =
    vector(uint8).decode(bits).require.value

  @Benchmark
  def scanFusedDirect: Vector[Int] =
    Scan.runDirect[Byte, Int, Any](fused, bytesSeq)._2

  @Benchmark
  def scanUnfusedDirect: Vector[Int] =
    Scan.runDirect[Byte, Int, Any](unfused, bytesSeq)._2

  // Note: a Kyo-runner lane is intentionally not part of the JMH suite.
  // The Kyo Poll/Emit/Abort plumbing imposes a fixed per-element
  // suspension cost that dominates this trivial workload by ~30x. See
  // `ScanPerfBench` in the test sources for an in-test measurement that
  // exercises the Kyo lane on a smaller payload.

  @Benchmark
  def handCoded: Array[Int] = {
    val out = new Array[Int](n)
    var i   = 0
    while i < n do {
      val b = bytes(i) & 0xff
      out(i) = ((b + 1) ^ 0x55) - 1
      i += 1
    }
    out
  }
}
