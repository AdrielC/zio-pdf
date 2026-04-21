/*
 * Top-level PDF chunks. Now extended with IndirectObj and (textual)
 * Xref cases since those ADTs are ported.
 */

package zio.pdf

import zio.pdf.{Comment as CommentCodec}
import zio.scodec.stream.syntax.*
import _root_.scodec.{Codec, Decoder, Err}
import _root_.scodec.bits.ByteVector
import zio.stream.ZPipeline

sealed trait TopLevel
object TopLevel {
  final case class VersionT(version: Version)             extends TopLevel
  final case class CommentT(data: ByteVector)             extends TopLevel
  final case class StartXrefT(startxref: StartXref)       extends TopLevel
  final case class WhitespaceT(byte: Byte)                extends TopLevel
  final case class IndirectObjT(obj: IndirectObj)         extends TopLevel
  final case class XrefT(xref: Xref)                      extends TopLevel

  /**
   * Choice over all top-level shapes. Order matters: Version must
   * come first because `%PDF-` would otherwise be eaten by a
   * Comment, and Xref must come before IndirectObj because `xref`
   * could otherwise be partially absorbed.
   */
  val Decoder_TopLevel: Decoder[TopLevel] =
    Decoder.choiceDecoder(
      Version.codec.map(VersionT(_)),
      summon[Codec[Xref]].map(XrefT(_)),
      StartXref.codec.map(StartXrefT(_)),
      summon[Codec[IndirectObj]].map(IndirectObjT(_)),
      (CommentCodec.start ~> CommentCodec.line).map(CommentT(_)),
      Decoder { bits =>
        if (bits.size < 8L)
          _root_.scodec.Attempt.failure(Err.InsufficientBits(8L, bits.size, Nil))
        else {
          val (head, rest) = bits.splitAt(8L)
          val byte         = head.bytes.head
          if (byte == ' '.toByte || byte == '\n'.toByte || byte == '\r'.toByte || byte == '\t'.toByte)
            _root_.scodec.Attempt.successful(_root_.scodec.DecodeResult(WhitespaceT(byte): TopLevel, rest))
          else
            _root_.scodec.Attempt.failure(Err(s"top-level: unrecognised byte ${byte.toInt & 0xff}"))
        }
      }
    )

  /**
   * Force a failing choice to look like `InsufficientBits` so the
   * streaming `StreamDecoder.many` will pull more input instead of
   * giving up.
   */
  val streamDecoder: Decoder[TopLevel] =
    Decoder { bits =>
      Decoder_TopLevel.decode(bits) match {
        case s @ _root_.scodec.Attempt.Successful(_) => s
        case _root_.scodec.Attempt.Failure(e)        =>
          _root_.scodec.Attempt.failure(Err.InsufficientBits(0, 0, e.context))
      }
    }

  /** ZIO `ZPipeline` from raw bytes to `TopLevel` chunks. */
  val pipe: ZPipeline[Any, Throwable, Byte, TopLevel] =
    streamDecoder.streamMany.toBytePipeline
}
