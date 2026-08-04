/*
 * Byte-window feed as [[ZPure]] — same slots as [[PureDecoder]] / [[StatefulPipe]]:
 *
 *   W = E     each emitted event is a log entry (`runAll` → `Chunk[E]`)
 *   S         machine state
 *   E (err)   Nothing on the hot path
 *   A         Unit
 *
 * [[apply]] composes the whole slice into one `ZPure`. [[runWindows]] is the
 * StreamDecoder-style interpreter: `runAll` per window so a fuse sink can
 * consume the log without retaining the full timeline.
 */

package zio.pdf.pipe

import java.security.MessageDigest

import zio.Chunk
import zio.prelude.fx.ZPure
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object ByteFeed {

  /**
   * One window step — structurally
   * `(Array[Byte], Int, Int) => S => (Chunk[E], S)`.
   */
  type Step[S, +E] =
    (Array[Byte], Int, Int) => ZPure[E, S, S, Any, Nothing, Unit]

  type Finalize[S, +E] =
    S => ZPure[E, S, S, Any, Nothing, Unit]

  /** Emit each event on the log channel. */
  def logAll[S, E](events: Chunk[E]): ZPure[E, S, S, Any, Nothing, Unit] =
    events.foldLeft[ZPure[E, S, S, Any, Nothing, Unit]](ZPure.unit) {
      (acc, e) => acc *> ZPure.log[S, E](e)
    }

  /** Lift a sync `(state, window) => (events, state)` into a [[Step]]. */
  def fromSync[S, E](f: (S, Array[Byte], Int, Int) => (Chunk[E], S)): Step[S, E] =
    (buf, off, len) =>
      ZPure
        .modify[S, S, Chunk[E]] { s =>
          val (out, next) = f(s, buf, off, len)
          (out, next)
        }
        .flatMap(logAll)

  /**
   * The whole slice as one `ZPure`. Interpret with `.runAll(initial)`:
   * `(Chunk[E], Either[Nothing, (S, Unit)])`.
   */
  def apply[S, E](
    slice: Slice,
    batchSize: Int,
    step: Step[S, E],
    finalize: Finalize[S, E] = (_: S) => ZPure.unit[S]
  ): ZPure[E, S, S, Any, Nothing, Unit] = {
    val buf = slice.bytes
    val end = slice.offset + slice.length

    def windows(pos: Int): ZPure[E, S, S, Any, Nothing, Unit] =
      if pos >= end then ZPure.unit[S]
      else
        val len = math.min(batchSize, end - pos)
        step(buf, pos, len) *> windows(pos + len)

    windows(slice.offset) *> ZPure.get[S].flatMap(finalize)
  }

  /** Materialise: `apply(...).runAll(initial)`. */
  def run[S, E](
    slice: Slice,
    batchSize: Int,
    initial: S,
    step: Step[S, E],
    finalize: Finalize[S, E] = (_: S) => ZPure.unit[S]
  ): (Chunk[E], S) = {
    val (log, result) = apply(slice, batchSize, step, finalize).runAll(initial)
    (log, result.fold(_ => initial, _._1))
  }

  /**
   * Interpret window-by-window (like `StreamDecoder.fromPure`): each
   * `runAll` yields that window's `Chunk[E]` for `consume`, then state
   * continues. Fuse/sink path — does not retain the full log.
   */
  def runWindows[S, E](
    slice: Slice,
    batchSize: Int,
    initial: S,
    step: Step[S, E],
    finalize: Finalize[S, E] = (_: S) => ZPure.unit[S]
  )(consume: Chunk[E] => Unit): S = {
    var state = initial
    var pos   = slice.offset
    val end   = slice.offset + slice.length
    val buf   = slice.bytes
    while pos < end do
      val len           = math.min(batchSize, end - pos)
      val (log, result) = step(buf, pos, len).runAll(state)
      if log.nonEmpty then consume(log)
      state = result.fold(_ => state, _._1)
      pos += len
    val (flog, fresult) = finalize(state).runAll(state)
    if flog.nonEmpty then consume(flog)
    fresult.fold(_ => state, _._1)
  }

  /** SHA-256 over the same byte windows — digest lives in [[ZPure]] state. */
  def digestBatched(slice: Slice, batchSize: Int): Array[Byte] = {
    val initial = MessageDigest.getInstance("SHA-256")
    val step: Step[MessageDigest, Nothing] = (buf, off, len) =>
      ZPure.update[MessageDigest, MessageDigest] { md =>
        md.update(buf, off, len)
        md
      }
    val (_, md) = run(slice, batchSize, initial, step)
    md.digest()
  }

  /** [[FusedDecode.decodeStreamingSlice]] via [[run]]. */
  def streamingEvents(slice: Slice, cfg: Cfg): Chunk[zio.pdf.StreamingDecoded] = {
    import zio.pdf.StreamingDecode
    val Cfg(diag, config, batchSize) = cfg
    run(
      slice,
      batchSize,
      StreamingDecode.initialFinalState,
      fromSync((fs, buf, off, len) => StreamingDecode.stepChunkBytes(config, fs, buf, off, len)),
      fs => logAll(StreamingDecode.finalizeToMetaSync(diag, fs))
    )._1
  }
}
