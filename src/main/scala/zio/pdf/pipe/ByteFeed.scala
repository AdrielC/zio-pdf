/*
 * Generic byte-slice feeding loop for state-carrying decode stages.
 *
 * Volga threads wires through a comprehension; [[ByteFeed]] is the
 * imperative fuse: one tight `while` over slices, no intermediate chunks.
 */

package zio.pdf.pipe

import java.security.MessageDigest

import zio.Chunk
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object ByteFeed {

  /** One streaming step: feed bytes, return emitted events and next machine state. */
  type Step[S, E] = (S, Array[Byte], Int, Int) => (Chunk[E], S)

  /** Optional per-window hook (digest, metrics) on the same bytes the step sees. */
  type OnBytes = (Array[Byte], Int, Int) => Unit

  /** Per-batch event sink — avoids materialising the full timeline. */
  type OnEvents[E] = Chunk[E] => Unit

  /**
   * Drive [[Step]] across a [[Slice]] in `batchSize` windows.
   * `finalize` runs once after the last byte (e.g. trailing Meta).
   */
  def run[S, E](
    slice: Slice,
    batchSize: Int,
    initial: S,
    step: Step[S, E],
    finalize: S => Chunk[E]
  ): (Chunk[E], S) = {
    val builder = Chunk.newBuilder[E]
    val state   = runBatched(slice, batchSize, initial, step, finalize, onEvents = builder ++= _)
    (builder.result(), state)
  }

  /**
   * Fused driver: one `while` loop, per-batch [[onEvents]], optional [[onBytes]]
   * for incremental digest on the exact windows [[StreamingDecode]] consumes.
   */
  def runBatched[S, E](
    slice: Slice,
    batchSize: Int,
    initial: S,
    step: Step[S, E],
    finalize: S => Chunk[E],
    onBytes: OnBytes = (_, _, _) => (),
    onEvents: OnEvents[E]
  ): S = {
    var state = initial
    var pos   = slice.offset
    val end   = slice.offset + slice.length
    val buf   = slice.bytes
    while pos < end do
      val len = math.min(batchSize, end - pos)
      onBytes(buf, pos, len)
      val (out, next) = step(state, buf, pos, len)
      onEvents(out)
      state = next
      pos += len
    onEvents(finalize(state))
    state
  }

  /** SHA-256 over the same byte windows fed to [[runBatched]]. */
  def digestBatched(slice: Slice, batchSize: Int): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    runBatched(
      slice,
      batchSize,
      (),
      (_, _, _, _) => (Chunk.empty, ()),
      _ => Chunk.empty,
      onBytes = (buf, off, len) => md.update(buf, off, len),
      onEvents = _ => ()
    )
    md.digest()
  }

  /** [[FusedDecode.decodeStreamingSlice]] expressed as [[runBatched]]. */
  def streamingEvents(slice: Slice, cfg: Cfg): Chunk[zio.pdf.StreamingDecoded] = {
    import zio.pdf.StreamingDecode
    val Cfg(diag, config, batchSize) = cfg
    val builder                      = Chunk.newBuilder[zio.pdf.StreamingDecoded]
    val _ = runBatched(
      slice,
      batchSize,
      StreamingDecode.initialFinalState,
      (fs, buf, off, len) => StreamingDecode.stepChunkBytes(config, fs, buf, off, len),
      fs => StreamingDecode.finalizeToMetaSync(diag, fs),
      onEvents = builder ++= _
    )
    builder.result()
  }
}
