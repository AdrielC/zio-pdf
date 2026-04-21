/*
 * FastCDC driven by Kyo [[kyo.Emit]]: one [[Emit.valueWith]] per completed
 * CDC chunk (each emission is a single `zio.Chunk[Byte]`, not a batch).
 * Downstream [[kyo.Stream]] composes via `Emit[Chunk[V]]` with Kyo's chunk type.
 */

package zio.pdf.cdc

import kyo.*
import zio.Chunk as ZChunk

object FastCdcKyo {

  /** Feed `bytes` in `rechunk`-sized slices, same framing as `ZStream.rechunk`. */
  def emitChunked(
    bytes: ZChunk[Byte],
    rechunk: Int,
    cfg: FastCdc.Config = FastCdc.defaultConfig
  )(using Frame, Tag[Emit[ZChunk[Byte]]]): Unit < Emit[ZChunk[Byte]] = {
    val arr       = bytes.toArray
    val chunkSize = rechunk max 1

    def flushEmitted(
      toEmit: ZChunk[ZChunk[Byte]],
      i: Int,
      cont: => Unit < Emit[ZChunk[Byte]]
    ): Unit < Emit[ZChunk[Byte]] =
      if (i >= toEmit.length) cont
      else Emit.valueWith(toEmit(i))(flushEmitted(toEmit, i + 1, cont))

    def go(carry: Array[Byte], off: Int): Unit < Emit[ZChunk[Byte]] =
      if (off < arr.length) {
        val end             = math.min(off + chunkSize, arr.length)
        val slice           = java.util.Arrays.copyOfRange(arr, off, end)
        val merged          = FastCdc.mergeArrays(carry, slice)
        val (emitted, rest) = FastCdc.drainBuffer(merged, flushTail = false, cfg)
        flushEmitted(emitted, 0, go(rest, end))
      } else {
        val (emitted, _) = FastCdc.drainBuffer(carry, flushTail = true, cfg)
        flushEmitted(emitted, 0, ())
      }

    go(new Array[Byte](0), 0)
  }

  /** Collect all CDC chunks (for tests / small inputs). */
  def runCollect(
    bytes: ZChunk[Byte],
    rechunk: Int,
    cfg: FastCdc.Config = FastCdc.defaultConfig
  )(using Frame, Tag[Emit[ZChunk[Byte]]]): ZChunk[ZChunk[Byte]] < Any =
    Emit.run(emitChunked(bytes, rechunk, cfg)).map { case (kyoChunks, _) =>
      val b = ZChunk.newBuilder[ZChunk[Byte]]
      kyoChunks.foreach(c => b += c)
      b.result()
    }
}
