/*
 * FastCDC via Kyo [[kyo.Emit]]: one emission per finished CDC segment as a
 * raw `Array[Byte]` (no `zio.Chunk` in the hot path). Convert to ZIO chunks
 * only at integration boundaries ([[runCollect]], ZIO interop).
 */

package zio.pdf.cdc

import kyo.*
import zio.Chunk as ZChunk

object FastCdcKyo {

  /** Same as [[emitChunked]] but reads from a byte array (no `zio.Chunk` copy). */
  def emitChunkedFromArray(
    arr: Array[Byte],
    rechunk: Int,
    cfg: FastCdc.Config = FastCdc.defaultConfig
  )(using Frame, Tag[Emit[Array[Byte]]]): Unit < Emit[Array[Byte]] = {
    val chunkSize = rechunk max 1

    def flushEmitted(
      segs: Array[Array[Byte]],
      i: Int,
      cont: => Unit < Emit[Array[Byte]]
    ): Unit < Emit[Array[Byte]] =
      if (i >= segs.length) cont
      else Emit.valueWith(segs(i))(flushEmitted(segs, i + 1, cont))

    def go(carry: Array[Byte], off: Int): Unit < Emit[Array[Byte]] =
      if (off < arr.length) {
        val end             = math.min(off + chunkSize, arr.length)
        val slice           = java.util.Arrays.copyOfRange(arr, off, end)
        val merged          = FastCdc.mergeArrays(carry, slice)
        val (emitted, rest) = FastCdc.drainToArrays(merged, flushTail = false, cfg)
        flushEmitted(emitted, 0, go(rest, end))
      } else {
        val (emitted, _) = FastCdc.drainToArrays(carry, flushTail = true, cfg)
        flushEmitted(emitted, 0, ())
      }

    go(new Array[Byte](0), 0)
  }

  /** Feed `bytes` in `rechunk`-sized slices (same framing as `ZStream.rechunk`). */
  def emitChunked(
    bytes: ZChunk[Byte],
    rechunk: Int,
    cfg: FastCdc.Config = FastCdc.defaultConfig
  )(using Frame, Tag[Emit[Array[Byte]]]): Unit < Emit[Array[Byte]] =
    emitChunkedFromArray(bytes.toArray, rechunk, cfg)

  /** Collect as `zio.Chunk` (converts arrays once at the end). */
  def runCollect(
    bytes: ZChunk[Byte],
    rechunk: Int,
    cfg: FastCdc.Config = FastCdc.defaultConfig
  )(using Frame, Tag[Emit[Array[Byte]]]): ZChunk[ZChunk[Byte]] < Any =
    Emit.run(emitChunked(bytes, rechunk, cfg)).map { case (kyoChunks, _) =>
      val b = ZChunk.newBuilder[ZChunk[Byte]]
      kyoChunks.foreach(arr => b += ZChunk.fromArray(arr))
      b.result()
    }
}
