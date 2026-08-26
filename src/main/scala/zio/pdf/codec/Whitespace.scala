/*
 * Port of fs2.pdf.codec.Whitespace to Scala 3 + scodec 2.3.
 */

package zio.pdf.codec

import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder, Encoder}
import _root_.scodec.bits.ByteVector
import _root_.scodec.codecs.*

private[pdf] object Whitespace {

  import Newline.*

  val whitespaceBytes: Decoder[ByteVector] =
    Decoder.choiceDecoder(
      constant(lfBytes).map(_ => lfBytes),
      constant(crlfBytes).map(_ => crlfBytes),
      constant(crBytes).map(_ => crBytes),
      constant(spaceBytes).map(_ => spaceBytes),
      constant(tabBytes).map(_ => tabBytes)
    )

  val multiWhitespaceDecoder: Decoder[ByteVector] =
    Decoder { bits =>
      val bytes = bits.bytes
      val ws    = bytes.takeWhile(_.toChar.isWhitespace)
      Attempt.successful(DecodeResult(ws, bytes.drop(ws.size).bits))
    }

  def multiWhitespace(encode: Encoder[Unit]): Codec[Unit] =
    Codec(encode, multiWhitespaceDecoder.map(_ => ()))

  def multiWhitespaceByte(b: Byte): Codec[Unit] =
    multiWhitespace(Codecs.byte(b))

  val whitespace: Codec[Unit] =
    choice(
      constant(lfBytes),
      constant(crlfBytes),
      constant(crBytes),
      constant(spaceBytes),
      constant(tabBytes)
    )

  val ws: Codec[Unit] =
    multiWhitespaceByte(spaceByte)

  val skipWs: Codec[Unit] =
    multiWhitespace(provide(()))

  /** Horizontal PDF whitespace, used where a line ending is structural. */
  val horizontalWs: Codec[Unit] =
    Codec(
      Codecs.byte(spaceByte),
      Decoder { bits =>
        val bytes = bits.bytes
        val prefix = bytes.takeWhile(byte => byte == spaceByte || byte == '\t'.toByte)
        Attempt.successful(DecodeResult((), bytes.drop(prefix.size).bits))
      }
    )

  /** PDF object trivia: whitespace plus any `%` line comments. */
  val skipTrivia: Codec[Unit] =
    Codec(
      provide(()),
      Decoder { bits =>
        val bytes = bits.bytes
        var index = 0L
        var continue = true
        def startsEofMarker(at: Long): Boolean =
          bytes.size - at >= 5L &&
            bytes(at) == '%'.toByte &&
            bytes(at + 1L) == '%'.toByte &&
            bytes(at + 2L) == 'E'.toByte &&
            bytes(at + 3L) == 'O'.toByte &&
            bytes(at + 4L) == 'F'.toByte
        while continue && index < bytes.size do
          while index < bytes.size && bytes(index).toChar.isWhitespace do index += 1L
          if index < bytes.size && bytes(index) == '%'.toByte && !startsEofMarker(index) then
            index += 1L
            while index < bytes.size && bytes(index) != '\n'.toByte && bytes(index) != '\r'.toByte do index += 1L
          else continue = false
        Attempt.successful(DecodeResult((), bytes.drop(index).bits))
      }
    )

  val whitespaceAsNewline: Codec[Unit] =
    multiWhitespaceByte(lfByte)

  /** Encode a newline and decode any PDF object trivia. */
  val triviaAsNewline: Codec[Unit] =
    Codec(whitespaceAsNewline, skipTrivia)

  val space: Codec[Unit] =
    Codecs.byte(' ')

  /**
   * Whitespace + optional inline comments + final newline.
   * Equivalent to the legacy `whitespaceAndCommentAsNewline`.
   * Lives here (not in `Comment`) so the dependency graph stays
   * one-way.
   */
  val nlWs: Codec[Unit] =
    skipWs ~> zio.pdf.Comment.many.unit(Nil) ~> whitespaceAsNewline
}
