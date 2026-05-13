/*
 * Complex-composition benchmarks for the scan algebra.
 *
 * The original `ScanBench` measures 4-stage toy pipelines (4 maps;
 * 4 maps with one filter spliced in). That is enough to *detect*
 * fusion working and per-stage allocations going away, but it is
 * not the workload the architecture is *for*. The register lane
 * was built so that a deep, real-world composition with multiple
 * stateful stages does not pay one allocation per stage per input
 * byte; fusion was built so that an N-stage pure spine costs O(1)
 * per input, not O(N). The point of this file is to bench the
 * shapes where N is realistic.
 *
 * Lanes:
 *
 *   - `deepPureSpine{Direct,Reg}`        -- 32 pure maps in a row.
 *                                            Tests how the runners
 *                                            scale on a deep pure
 *                                            spine: fusion should
 *                                            collapse both lanes to
 *                                            ~one call per byte.
 *
 *   - `deepMixedSpine{Direct,Reg}`       -- 16 stages of alternating
 *                                            map/filter/map. Non-
 *                                            fusable (Filter blocks
 *                                            fusion); each runner
 *                                            walks every stage.
 *                                            This is where per-stage
 *                                            allocation differences
 *                                            multiply.
 *
 *   - `ingest3Stage{Direct,Reg}`         -- BombGuard >>> CountBytes
 *                                            >>> Hash(SHA-256). The
 *                                            canonical Graviton
 *                                            sequential ingest shape.
 *                                            Three stateful primitives
 *                                            in series.
 *
 *   - `ingestFanout{Direct,Reg}`         -- (CountBytes) &&& (Hash).
 *                                            Tests how the runners
 *                                            handle stateful fanout.
 *                                            NOTE: the register lane
 *                                            falls back to the legacy
 *                                            stepper on Fanout shapes
 *                                            today, so this lane
 *                                            measures whether that
 *                                            fallback regresses.
 *
 *   - `deepFoldSpine{Direct,Reg}`        -- 8 chained `Fold[Byte, Long]`
 *                                            stages (each fold counts;
 *                                            the chain accumulates and
 *                                            re-aggregates). All folds
 *                                            are erased through the
 *                                            FreeScan layer, so the
 *                                            reg lane stores each
 *                                            accumulator in an object
 *                                            slot. Tests that the
 *                                            allocation-elimination
 *                                            wins multiply with stage
 *                                            count.
 *
 * Workload is 256 KiB instead of 1 MiB so each iteration completes
 * within JMH's 1s budget even on the deepest spines.
 */

package zio.pdf.scan.bench

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import scala.compiletime.uninitialized

import zio.pdf.scan.*

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class ComplexScanBench {

  /** 256 KiB. Smaller than ScanBench to keep deep spines inside the
    * JMH iteration budget. */
  @Param(Array("262144"))
  var n: Int = uninitialized

  private var bytes:    Array[Byte]         = uninitialized
  private var bytesSeq: IndexedSeq[Byte]    = uninitialized

  // -------------------------------------------------------------------
  // Pipeline definitions.
  // -------------------------------------------------------------------

  /** 32 pure maps composed left-to-right. Every node is `Arr` / `Map`,
    * so `Fusion.tryFuse` collapses the chain to a single `Byte => Int`. */
  private val deepPure: FreeScan[Byte, Int] = {
    val head: FreeScan[Byte, Int] = Scan.map[Byte, Int](b => b & 0xff)
    var acc: FreeScan[Byte, Int]  = head
    var i                          = 0
    while i < 31 do {
      acc = acc >>> Scan.map[Int, Int](_ + 1)
      i  += 1
    }
    acc
  }

  /** 16 stages, alternating maps and `Filter(_ => true)` (true so we
    * keep every element -- this measures stepper overhead, not
    * filtering work). The `Filter` blocks fusion, so the runners
    * walk every stage per byte. */
  private val deepMixed: FreeScan[Byte, Int] = {
    var acc: FreeScan[Byte, Int] = Scan.map[Byte, Int](b => b & 0xff)
    var i                         = 0
    while i < 8 do {
      acc = acc >>> Scan.filter[Int](_ => true) >>> Scan.map[Int, Int](_ + 1)
      i  += 1
    }
    acc
  }

  /** `Fold[Byte, Long](+1)` -- a single primitive-accumulator fold.
    * Routes through the register lane's *erased* path for the reg
    * lane (object slot), and through the unboxed long-slot path for
    * `ingest3StageUnboxedFold`. */
  private val foldLong: FreeScan[Byte, Long] =
    Scan.fold[Byte, Long](0L)((acc, _) => acc + 1L)

  /** Graviton-style 3-stage sequential ingest. */
  private val ingestSeq: FreeScan[Byte, Byte] =
    Scan.bombGuard(maxBytes = 1L << 30) >>>
      Scan.countBytes                    >>>
      Scan.hash(HashAlgo.Sha256)

  /** Fanout: counter and hasher fed from the same byte stream.
    * Register lane falls back to legacy for this shape. */
  private val ingestFanout: FreeScan[Byte, (Byte, Byte)] =
    Scan.countBytes &&& Scan.hash(HashAlgo.Sha256)

  /** 8 chained `Fold[Byte, Long]` stages. Each fold counts; the
    * output of one feeds the next as `I = Long`. Stays in the
    * register lane (no Fanout/Choice). */
  private val deepFold: FreeScan[Byte, Long] = {
    val head: FreeScan[Byte, Long] =
      Scan.fold[Byte, Long](0L)((acc, _) => acc + 1L)
    var acc: FreeScan[Byte, Long] = head
    var i                          = 0
    while i < 7 do {
      acc = acc >>> Scan.fold[Long, Long](0L)((a, b) => a + b)
      i  += 1
    }
    acc
  }

  @Setup(Level.Trial)
  def setup(): Unit = {
    bytes    = Array.tabulate[Byte](n)(i => (i & 0xff).toByte)
    bytesSeq = scala.collection.immutable.ArraySeq.unsafeWrapArray(bytes)
  }

  // -------------------------------------------------------------------
  // Lanes.
  // -------------------------------------------------------------------

  // 32-stage pure spine (fuses).
  @Benchmark
  def deepPureSpineDirect: Vector[Int] =
    Scan.runDirect[Byte, Int, Any](deepPure, bytesSeq)._2

  @Benchmark
  def deepPureSpineReg: Vector[Int] =
    Scan.runDirectReg[Byte, Int, Any](deepPure, bytesSeq)._2

  // 16-stage non-fusable spine.
  @Benchmark
  def deepMixedSpineDirect: Vector[Int] =
    Scan.runDirect[Byte, Int, Any](deepMixed, bytesSeq)._2

  @Benchmark
  def deepMixedSpineReg: Vector[Int] =
    Scan.runDirectReg[Byte, Int, Any](deepMixed, bytesSeq)._2

  // Graviton-style 3-stage sequential ingest.
  @Benchmark
  def ingest3StageDirect: Long = {
    val (sig, _) = Scan.runDirect[Byte, Byte, Any](ingestSeq, bytesSeq)
    sig.leftoverSeq.size.toLong
  }

  @Benchmark
  def ingest3StageReg: Long = {
    val (sig, _) = Scan.runDirectReg[Byte, Byte, Any](ingestSeq, bytesSeq)
    sig.leftoverSeq.size.toLong
  }

  // Stateful fanout (register lane falls back to legacy).
  @Benchmark
  def ingestFanoutDirect: Long = {
    val (sig, _) = Scan.runDirect[Byte, (Byte, Byte), Any](ingestFanout, bytesSeq)
    sig.leftoverSeq.size.toLong
  }

  @Benchmark
  def ingestFanoutReg: Long = {
    val (sig, _) = Scan.runDirectReg[Byte, (Byte, Byte), Any](ingestFanout, bytesSeq)
    sig.leftoverSeq.size.toLong
  }

  // 8-deep fold spine.
  @Benchmark
  def deepFoldSpineDirect: Long = {
    val (sig, _) = Scan.runDirect[Byte, Long, Any](deepFold, bytesSeq)
    sig.leftoverSeq.headOption.getOrElse(0L)
  }

  @Benchmark
  def deepFoldSpineReg: Long = {
    val (sig, _) = Scan.runDirectReg[Byte, Long, Any](deepFold, bytesSeq)
    sig.leftoverSeq.headOption.getOrElse(0L)
  }
}
