/*
 * Stream filter encoding — Flate (zlib) for recompress after edit.
 *
 * Each call owns a fresh [[Deflater]]. No ThreadLocal / shared mutable
 * session: safe under parallel ZIO fibers (and if a call later suspends).
 */

package zio.pdf

import java.util.zip.Deflater

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

private[pdf] object FlateEncode {

  def apply(stream: BitVector): Attempt[BitVector] = {
    val input    = stream.toByteArray
    val deflater = new Deflater(Deflater.DEFAULT_COMPRESSION)
    var out      = new Array[Byte](math.max(64 * 1024, input.length / 2 + 64))
    var written  = 0
    try {
      deflater.setInput(input)
      deflater.finish()
      while !deflater.finished() do
        if written == out.length then {
          val grown = new Array[Byte](out.length * 2)
          System.arraycopy(out, 0, grown, 0, written)
          out = grown
        }
        written += deflater.deflate(out, written, out.length - written)
      val result = new Array[Byte](written)
      System.arraycopy(out, 0, result, 0, written)
      Attempt.successful(BitVector(result))
    } catch {
      case t: Throwable =>
        Attempt.failure(Err(s"FlateEncode: ${t.getMessage}"))
    } finally
      deflater.end()
  }
}
