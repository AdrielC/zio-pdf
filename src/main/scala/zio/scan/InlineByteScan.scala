/*
 * Compile-time byte scan builder (zio-blocks only).
 *
 * {{{
 *   InlineByteScan.map(_ + 1).map(_ ^ 0x55).map(_ - 1).run(bytes)
 * }}}
 *
 * Each [[map]] is `inline` — the inliner beta-reduces stages into one
 * monomorphic `while` loop (~hand-coded speed). Use [[pipeline]] only when
 * you need a reusable [[BytePipeline]] value.
 */

package zio.scan

import zio.blocks.chunk.Chunk

final case class InlineByteScan(stage: Int => Int) {

  inline def map(inline f: Int => Int): InlineByteScan =
    InlineByteScan((i: Int) => f(stage(i)))

  /** Fused hot loop — stages inlined here, not delegated to [[BytePipeline]]. */
  inline def run(bytes: Array[Byte]): Chunk[Int] =
    runSlice(bytes, 0, bytes.length)

  inline def runSlice(bytes: Array[Byte], offset: Int, length: Int): Chunk[Int] = {
    val out = new Array[Int](length)
    var i   = 0
    val end = offset + length
    var j   = offset
    while j < end do
      out(i) = stage(bytes(j) & 0xff)
      i += 1
      j += 1
    Chunk.fromArray(out)
  }

  inline def runCount(bytes: Array[Byte]): Long =
    runCountSlice(bytes, 0, bytes.length)

  inline def runCountSlice(bytes: Array[Byte], offset: Int, length: Int): Long = {
    var acc: Long = 0L
    val end       = offset + length
    var j         = offset
    while j < end do
      acc += stage(bytes(j) & 0xff)
      j += 1
    acc
  }

  /** Reusable [[BytePipeline]] when the fused function must escape. */
  inline def pipeline: BytePipeline =
    BytePipeline((b: Byte) => stage(b & 0xff))
}

object InlineByteScan {

  inline def apply: InlineByteScan =
    InlineByteScan(identity)

  inline def map(inline f: Int => Int): InlineByteScan =
    apply.map(f)

  inline def scanBenchFused: BytePipeline =
    map(_ + 1).map(_ ^ 0x55).map(_ - 1).pipeline
}
