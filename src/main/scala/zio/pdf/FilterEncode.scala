/*
 * Stream filter encoding — Flate (zlib) for recompress after edit.
 */

package zio.pdf

import java.util.zip.Deflater

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

private[pdf] object FlateEncode {

  private final class Session {
    val deflater = new Deflater(Deflater.DEFAULT_COMPRESSION)
    var out      = new Array[Byte](64 * 1024)

    def deflate(input: Array[Byte]): Attempt[Array[Byte]] = {
      deflater.reset()
      deflater.setInput(input)
      deflater.finish()
      var written = 0
      try {
        while !deflater.finished() do
          if written == out.length then {
            val grown = new Array[Byte](out.length * 2)
            System.arraycopy(out, 0, grown, 0, written)
            out = grown
          }
          written += deflater.deflate(out, written, out.length - written)
        val result = new Array[Byte](written)
        System.arraycopy(out, 0, result, 0, written)
        Attempt.successful(result)
      } catch {
        case t: Throwable =>
          Attempt.failure(Err(s"FlateEncode: ${t.getMessage}"))
      } finally
        deflater.reset()
    }
  }

  private val sessions: ThreadLocal[Session] =
    ThreadLocal.withInitial(() => new Session)

  def apply(stream: BitVector): Attempt[BitVector] = {
    val arr = stream.toByteArray
    sessions.get().deflate(arr).map(BitVector(_))
  }
}
