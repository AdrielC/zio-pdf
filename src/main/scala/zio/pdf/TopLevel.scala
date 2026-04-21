/*
 * Minimal `TopLevel` analogue covering the PDF chunks that have
 * already been ported (Version, Comment, StartXref). The legacy
 * variant also covers `IndirectObj` and `Xref`, which depend on the
 * yet-to-be-ported `Prim` / `Obj` / `Xref` ADTs.
 *
 * The real value here is showing how the new `StreamDecoder`
 * (ZChannel-based) drives a streaming top-level decoder against a
 * `ZStream[Any, Throwable, Byte]` and emits values as they appear.
 */

package zio.pdf

import zio.pdf.{Comment as CommentCodec}
import zio.scodec.stream.syntax.*
import _root_.scodec.{Decoder, Err}
import _root_.scodec.bits.ByteVector
import zio.stream.ZPipeline

sealed trait TopLevel
object TopLevel {
  final case class VersionT(version: Version)         extends TopLevel
  final case class CommentT(data: ByteVector)         extends TopLevel
  final case class StartXrefT(startxref: StartXref)   extends TopLevel
  final case class WhitespaceT(byte: Byte)            extends TopLevel

  /**
   * Hand-rolled choice. We can't use `Codec.coproduct[TopLevel].choice`
   * (that's the macro the legacy code used) because that machinery
   * depends on the full set of constructors and a number of scodec
   * 2.x details we'd like to re-prove with explicit code.
   */
  val Decoder_TopLevel: Decoder[TopLevel] =
    Decoder.choiceDecoder(
      Version.codec.map(TopLevel.VersionT(_)),
      StartXref.codec.map(TopLevel.StartXrefT(_)),
      (CommentCodec.start ~> CommentCodec.line).map(TopLevel.CommentT(_)),
      // Skip a single whitespace byte (matches the legacy fallthrough
      // that swallows newlines / tabs between top-level elements).
      Decoder { bits =>
        if (bits.size < 8L)
          _root_.scodec.Attempt.failure(Err.InsufficientBits(8L, bits.size, Nil))
        else {
          val (head, rest) = bits.splitAt(8L)
          val byte         = head.bytes.head
          if (byte == ' '.toByte || byte == '\n'.toByte || byte == '\r'.toByte || byte == '\t'.toByte)
            _root_.scodec.Attempt.successful(_root_.scodec.DecodeResult(TopLevel.WhitespaceT(byte): TopLevel, rest))
          else
            _root_.scodec.Attempt.failure(Err(s"top-level: unrecognised byte ${byte.toInt & 0xff}"))
        }
      }
    )

  /**
   * Force the choice decoder to always re-request more bits on
   * failure, matching the original FS2 behaviour: a failing choice
   * inside the streaming decoder must look like `InsufficientBits`
   * so the loop will pull another chunk instead of giving up.
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
