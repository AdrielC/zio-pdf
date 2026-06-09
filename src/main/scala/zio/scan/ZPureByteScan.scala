/*
 * ZPure-based byte scan with batched log emission.
 *
 * Per-byte `ZPure.log` is ~150× slower than strict decode (see
 * StreamDecoderBench). This runner batches outputs per upstream chunk
 * via a single log entry per batch — the same amortisation strategy as
 * `PureDecoder.manyChunked` / `StreamDecoder.fromPureChunked`.
 */

package zio.scan

import zio.blocks.chunk.Chunk
import zio.prelude.fx.ZPure

object ZPureByteScan {

  /** One ZPure step: map a byte slice, emit one log chunk of ints. */
  def stepSlice(
    pipeline: BytePipeline,
    bytes: Array[Byte],
    offset: Int,
    length: Int
  ): ZPure[Chunk[Int], Unit, Unit, Any, Nothing, Unit] =
    ZPure.log(pipeline.runSlice(bytes, offset, length))

  /** Fused pipeline only (no ZPure). Same cost as [[BytePipeline.run]]. */
  def runFast(pipeline: BytePipeline, bytes: Array[Byte]): Chunk[Int] =
    pipeline.run(bytes)

  /**
   * Batched ZPure log emission — measures the ZPure tax, not the fused
   * map work. One `runAll` per batch.
   */
  def runBatched(
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
      log.foreach(batch => builder ++= batch)
      offset = end
    builder.result()
  }

  /** Long fold without per-element log — accumulator in ZPure state. */
  def countBytes(bytes: Array[Byte]): Long = {
    val inc: ZPure[Nothing, Long, Long, Any, Nothing, Long] =
      for {
        s <- ZPure.get[Long]
        n  = s + 1L
        _ <- ZPure.set(n)
      } yield n

    var state = 0L
    var i     = 0
    val n     = bytes.length
    while i < n do
      val (_, r) = inc.runAll(state)
      state = r.fold(_ => state, _._2)
      i += 1
    state
  }
}
