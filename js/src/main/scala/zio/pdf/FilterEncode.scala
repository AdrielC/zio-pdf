package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

/** Browser / Node.js Flate encoder backed by pako's zlib-compatible codec. */
private[pdf] object FlateEncode:

  def apply(stream: BitVector): Attempt[BitVector] =
    try
      val compressed = PakoDeflate(JsBinary.uint8(stream.toByteArray))
      Attempt.successful(BitVector(JsBinary.bytes(compressed)))
    catch
      case throwable: Throwable => Attempt.failure(Err(s"FlateEncode: ${throwable.getMessage}"))
