/*
 * Generic byte-slice feeding loop for state-carrying decode stages.
 *
 * Same contract as [[zio.scodec.stream.StatefulPipe]]: the step is a
 * [[ZPure]] whose log channel carries event batches and whose state
 * channel threads the machine. The driver is a tight `while` over
 * windows — one `runAll` per slice, no intermediate timelines.
 */

package zio.pdf.pipe

import java.security.MessageDigest

import zio.Chunk
import zio.prelude.fx.ZPure
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object ByteFeed {

  /**
   * One streaming window: feed bytes, emit a batch on the log channel,
   * thread machine state. Mirrors [[zio.scodec.stream.StatefulPipe.Step]].
   */
  type Step[S, E] =
    (Array[Byte], Int, Int) => ZPure[Chunk[E], S, S, Any, Nothing, Unit]

  type Finalize[S, E] =
    S => ZPure[Chunk[E], S, S, Any, Nothing, Unit]

  /** Emit one batch (no-op when empty — avoids a useless log node). */
  def emitBatch[S, E](out: Chunk[E]): ZPure[Chunk[E], S, S, Any, Nothing, Unit] =
    if out.isEmpty then ZPure.unit[S]
    else ZPure.log[S, Chunk[E]](out)

  /** Lift a sync `(state, window) => (events, state)` into a [[Step]]. */
  def fromSync[S, E](f: (S, Array[Byte], Int, Int) => (Chunk[E], S)): Step[S, E] =
    (buf, off, len) =>
      ZPure
        .modify[S, S, Chunk[E]] { s =>
          val (out, next) = f(s, buf, off, len)
          (out, next)
        }
        .flatMap(emitBatch)

  /** Run `hook` on each window before `step` (digest, metrics). */
  def tapBytes[S, E](hook: (Array[Byte], Int, Int) => Unit)(step: Step[S, E]): Step[S, E] =
    (buf, off, len) =>
      ZPure.succeed[S, Unit] { hook(buf, off, len) } *> step(buf, off, len)

  /**
   * Drive [[Step]] across a [[Slice]] in `batchSize` windows.
   * `finalize` runs once after the last byte (e.g. trailing Meta).
   */
  def run[S, E](
    slice: Slice,
    batchSize: Int,
    initial: S,
    step: Step[S, E],
    finalize: Finalize[S, E]
  ): (Chunk[E], S) = {
    val builder = Chunk.newBuilder[E]
    val state   = runDrain(slice, batchSize, initial, step, finalize)(builder ++= _)
    (builder.result(), state)
  }

  /**
   * Fused driver: one `while` loop, drain each window's log batch through
   * `drain` so the full timeline never materialises.
   */
  def runDrain[S, E](
    slice: Slice,
    batchSize: Int,
    initial: S,
    step: Step[S, E],
    finalize: Finalize[S, E]
  )(drain: Chunk[E] => Unit): S = {
    var state = initial
    var pos   = slice.offset
    val end   = slice.offset + slice.length
    val buf   = slice.bytes
    while pos < end do
      val len           = math.min(batchSize, end - pos)
      val (log, result) = step(buf, pos, len).runAll(state)
      drainLog(log, drain)
      state = result.fold(_ => state, _._1)
      pos += len
    val (flog, fresult) = finalize(state).runAll(state)
    drainLog(flog, drain)
    fresult.fold(_ => state, _._1)
  }

  private def drainLog[E](log: Chunk[Chunk[E]], drain: Chunk[E] => Unit): Unit = {
    var i = 0
    while i < log.length do
      val batch = log(i)
      if batch.nonEmpty then drain(batch)
      i += 1
  }

  /** SHA-256 over the same byte windows fed to [[runDrain]]. */
  def digestBatched(slice: Slice, batchSize: Int): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    val _ = runDrain(
      slice,
      batchSize,
      (),
      tapBytes[Unit, Nothing]((buf, off, len) => md.update(buf, off, len))((_, _, _) => ZPure.unit),
      (_: Unit) => ZPure.unit
    )(_ => ())
    md.digest()
  }

  /** [[FusedDecode.decodeStreamingSlice]] expressed as [[runDrain]]. */
  def streamingEvents(slice: Slice, cfg: Cfg): Chunk[zio.pdf.StreamingDecoded] = {
    import zio.pdf.StreamingDecode
    val Cfg(diag, config, batchSize) = cfg
    run(
      slice,
      batchSize,
      StreamingDecode.initialFinalState,
      fromSync((fs, buf, off, len) => StreamingDecode.stepChunkBytes(config, fs, buf, off, len)),
      fs => emitBatch(StreamingDecode.finalizeToMetaSync(diag, fs))
    )._1
  }
}
