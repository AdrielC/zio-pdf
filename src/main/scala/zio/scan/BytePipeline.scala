/*
 * Fused byte pipelines on [[zio.blocks.chunk.Chunk]].
 *
 * This is the ZPure-aligned scan substrate: pure per-element transforms
 * composed and collapsed to a single hot-loop function. Output accumulates
 * in zio.blocks.chunk (not zio.Chunk).
 *
 * See `zio.pdf.scan.bench.ScanBench` for the reference workload and
 * `handCoded` baseline this package targets.
 */

package zio.scan

import zio.blocks.chunk.Chunk

/** A pure byte-to-int transform stage. Map stages fuse into one function. */
opaque type ByteStage = Int => Int

object ByteStage {
  def identity: ByteStage = i => i
  def apply(f: Int => Int): ByteStage = f
}

/** A fused pipeline: `Byte => Int` after map-composition. */
final case class BytePipeline private (f: Byte => Int) {

  def map(g: Int => Int): BytePipeline =
    BytePipeline(b => g(f(b)))

  /** Run over a byte array, materialising `Chunk[Int]`. */
  def run(bytes: Array[Byte]): Chunk[Int] =
    runSlice(bytes, 0, bytes.length)

  /** Run over `bytes[offset .. offset+length)` without copying the slice. */
  def runSlice(bytes: Array[Byte], offset: Int, length: Int): Chunk[Int] = {
    val out = new Array[Int](length)
    var i   = 0
    val end = offset + length
    var j   = offset
    while j < end do
      out(i) = f(bytes(j))
      i += 1
      j += 1
    Chunk.fromArray(out)
  }

  /** Run without materialising output (checksum-style). For JMH blackholes. */
  def runCount(bytes: Array[Byte]): Long =
    runCountSlice(bytes, 0, bytes.length)

  def runCountSlice(bytes: Array[Byte], offset: Int, length: Int): Long = {
    var acc: Long = 0L
    val end       = offset + length
    var j         = offset
    while j < end do
      acc += f(bytes(j))
      j += 1
    acc
  }
}

object BytePipeline {

  val empty: BytePipeline = BytePipeline(b => b & 0xff)

  def map(g: Int => Int): BytePipeline =
    empty.map(g)

  /** The fused 4-map workload from ScanBench (for cross-bench comparison). */
  val scanBenchFused: BytePipeline =
    BytePipeline(b => (((b & 0xff) + 1) ^ 0x55) - 1)

  /** Hand-coded equivalent of [[scanBenchFused]] — reference for tests. */
  def scanBenchFusedHandCoded(bytes: Array[Byte]): Chunk[Int] = {
    val n   = bytes.length
    val out = new Array[Int](n)
    var i   = 0
    while i < n do
      val b = bytes(i) & 0xff
      out(i) = ((b + 1) ^ 0x55) - 1
      i += 1
    Chunk.fromArray(out)
  }
}
