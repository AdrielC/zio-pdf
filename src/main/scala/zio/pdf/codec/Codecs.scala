/*
 * Port of fs2.pdf.codec.Codecs to Scala 3 + scodec 2.3.
 */

package zio.pdf.codec

import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder, Encoder, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import _root_.scodec.codecs.*

private[pdf] object Codecs {

  def byte(data: Byte): Codec[Unit] =
    constant(ByteVector.fromByte(data))

  def bracket[A](start: Codec[Unit], end: Codec[Unit])(main: Codec[A]): Codec[A] =
    start ~> main <~ end

  def bracketChar[A](start: Char, end: Char): Codec[A] => Codec[A] =
    bracket(Text.char(start), Text.char(end))

  def encode[A](a: A)(using encoder: Encoder[A]): Attempt[BitVector] =
    encoder.encode(a)

  def encodeBytes[A: Encoder](a: A): Attempt[ByteVector] =
    encode(a).map(_.bytes)

  def encodeOpt[A: Encoder]: Encoder[Option[A]] =
    Encoder {
      case Some(a) => Encoder[A].encode(a)
      case None    => Attempt.successful(BitVector.empty)
    }

  def decodeOpt[A: Decoder]: Decoder[Option[A]] =
    Decoder { bits =>
      Decoder[A].decode(bits) match {
        case Attempt.Successful(DecodeResult(a, rm)) =>
          Attempt.Successful(DecodeResult(Some(a), rm))
        case _ =>
          Attempt.Successful(DecodeResult(None, bits))
      }
    }

  def opt[A: Codec]: Codec[Option[A]] =
    Codec(encodeOpt[A], decodeOpt[A])

  def fail[A](message: String): Attempt[A] =
    Attempt.failure(Err(message))
}
