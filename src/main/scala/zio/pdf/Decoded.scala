/*
 * Port of fs2.pdf.Decoded to Scala 3 + ZIO. The ADT itself is a
 * straight transliteration; the FS2-specific `objects`/`parts`
 * pipes are reimplemented in terms of ZStream + ZChannel.
 */

package zio.pdf

import _root_.scodec.bits.BitVector

import zio.stream.ZPipeline

sealed trait Decoded

object Decoded {
  final case class DataObj(obj: Obj)                                                        extends Decoded
  final case class ContentObj(obj: Obj, rawStream: BitVector, stream: Uncompressed)         extends Decoded
  final case class Meta(xrefs: List[Xref], trailer: Option[Trailer], version: Option[Version]) extends Decoded

  /**
   * Trivially turn `Decoded` back into encodable `Part[Trailer]`.
   * The raw stream is preserved verbatim - no re-compression is
   * attempted.
   */
  val part: RewriteState[Unit] => Decoded => (List[Part[Trailer]], RewriteState[Unit]) =
    state => {
      case Decoded.Meta(_, trailer, _) =>
        (Nil, state.copy(trailer = trailer))
      case Decoded.DataObj(obj) =>
        (List(Part.Obj(IndirectObj(obj, None))), state)
      case Decoded.ContentObj(obj, rawStream, _) =>
        (List(Part.Obj(IndirectObj(obj, Some(rawStream)))), state)
    }

  val parts: ZPipeline[Any, Throwable, Decoded, Part[Trailer]] =
    Rewrite.simpleParts(())(part)(u => Part.Meta(u.trailer))
}
