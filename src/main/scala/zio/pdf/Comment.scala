/*
 * Port of fs2.pdf.Comment to Scala 3 + scodec 2.3.
 */

package zio.pdf

import zio.pdf.codec.{Codecs, Many, Text}
import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder}
import _root_.scodec.bits.ByteVector
import _root_.scodec.codecs.{optional, provide, recover}

/**
 * PDF comments — anything that starts with `%` and continues to the
 * end of the line. The `%%` prefix (used by version markers and
 * `%%EOF`) is *not* a comment, so the start decoder explicitly
 * rejects a second `%`.
 */
private[pdf] object Comment {

  val startDecoder: Decoder[Unit] =
    Decoder { bits =>
      val bytes = bits.bytes
      if (bytes.lift(0).contains('%'.toByte) && !bytes.lift(1).contains('%'.toByte))
        Attempt.successful(DecodeResult((), bytes.drop(1).bits))
      else
        Codecs.fail("not a comment")
    }

  val start: Codec[Unit] = Codec(provide(()), startDecoder)

  val line: Codec[ByteVector] = Text.line("comment")

  val inline: Codec[Option[ByteVector]] = optional(recover(start), line)

  val many: Codec[List[ByteVector]] = Many(start, line)
}
