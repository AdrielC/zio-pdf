/*
 * Runtime byte pipelines on [[zio.blocks.chunk.Chunk]].
 *
 * Prefer [[InlineByteScan]] when map stages are literal / inline at the
 * call site — it fuses to one function at compile time. This type is for
 * dynamic composition (stages from config, plugin lists, etc.).
 */

package zio.pdf.bench.scan

import zio.blocks.chunk.Chunk

/** A fused or nested `Byte => Int` pipeline. */
final case class BytePipeline(f: Byte => Int) {

  /** Runtime map — nests lambdas; prefer [[InlineByteScan.map]] when inline. */
  def map(g: Int => Int): BytePipeline =
    BytePipeline(b => g(f(b)))

  def run(bytes: Array[Byte]): Chunk[Int] =
    runSlice(bytes, 0, bytes.length)

  def runSlice(bytes: Array[Byte], offset: Int, length: Int): Chunk[Int] =
    Chunk.fromArray(runArraySlice(bytes, offset, length))

  /** Write into a caller-owned buffer (no extra array alloc). */
  def runInto(bytes: Array[Byte], offset: Int, length: Int, out: Array[Int], outOffset: Int = 0): Unit = {
    var i   = outOffset
    val end = offset + length
    var j   = offset
    while j < end do
      out(i) = f(bytes(j))
      i += 1
      j += 1
  }

  def runArray(bytes: Array[Byte]): Array[Int] =
    runArraySlice(bytes, 0, bytes.length)

  def runArraySlice(bytes: Array[Byte], offset: Int, length: Int): Array[Int] = {
    val out = new Array[Int](length)
    runInto(bytes, offset, length, out, 0)
    out
  }

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

  /** Fused bench preset — one function, no nested [[map]] lambdas. */
  val scanBenchFused: BytePipeline =
    BytePipeline(b => {
      val x = (b & 0xff) + 1
      (x ^ 0x55) - 1
    })

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
