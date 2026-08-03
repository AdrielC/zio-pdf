/*
 * Batched byte scan (zio-blocks only).
 *
 * [[runBatched]] — fused slices only (no Pure interpreter); same speed class
 * as [[InlineByteScan.run]].
 *
 * [[runBatchedPure]] — one [[zio.blocks.pure.Pure]] log entry per slice; use
 * when you need the log channel semantics, not raw throughput.
 */

package zio.scan

import zio.blocks.chunk.Chunk
import zio.blocks.pure.Pure

object BlocksPureByteScan {

  /** Fast batched run: one output array, slice via [[BytePipeline.runInto]]. */
  def runBatched(
    pipeline: BytePipeline,
    bytes: Array[Byte],
    batchSize: Int = 65536
  ): Chunk[Int] = {
    val n      = bytes.length
    val out    = new Array[Int](n)
    var offset = 0
    while offset < n do
      val end = math.min(offset + batchSize, n)
      pipeline.runInto(bytes, offset, end - offset, out, offset)
      offset = end
    Chunk.fromArray(out)
  }

  /** Batched run with one `Pure.log` batch per slice (measures interpreter tax). */
  def runBatchedPure(
    pipeline: BytePipeline,
    bytes: Array[Byte],
    batchSize: Int = 65536
  ): Chunk[Int] = {
    val builder = Chunk.newBuilder[Int]
    var offset  = 0
    val n       = bytes.length
    while offset < n do
      val end      = math.min(offset + batchSize, n)
      val batchLen = end - offset
      val (log, _) = stepSlice(pipeline, bytes, offset, batchLen).runAll(())
      var li       = 0
      while li < log.size do
        builder.addAll(log(li))
        li += 1
      offset = end
    builder.result()
  }

  def stepSlice(
    pipeline: BytePipeline,
    bytes: Array[Byte],
    offset: Int,
    length: Int
  ): Pure[Chunk[Int], Unit, Unit, Any, Nothing, Unit] =
    Pure.log(pipeline.runSlice(bytes, offset, length))

  /** State-threading reference (not a hot-path API). */
  def countBytes(bytes: Array[Byte]): Long = {
    val step  = Pure.update[Long, Long](_ + 1L)
    var state = 0L
    var i     = 0
    val n     = bytes.length
    while i < n do
      val (_, r) = step.runAll(state)
      state = r.fold(_ => state, { case (s, _) => s })
      i += 1
    state
  }
}
