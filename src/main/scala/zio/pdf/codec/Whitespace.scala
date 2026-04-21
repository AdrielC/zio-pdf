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

  val whitespaceAsNewline: Codec[Unit] =
    multiWhitespaceByte(lfByte)

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
