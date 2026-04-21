/*
 * Decoder events for the unified streaming PDF pipeline.
 *
 * Content streams use [[StreamingDecoded.ContentObjStart]]: if the
 * declared `/Length` is at most `inlineMaxBytes` (see
 * [[StreamingDecode.Config]]), the raw payload is buffered and
 * attached as `inlinePayload`; otherwise `inlinePayload` is `None`
 * and the payload follows as zero or more [[StreamingDecoded.ContentObjBytes]]
 * terminated by [[StreamingDecoded.ContentObjEnd]].
 */

package zio.pdf

import _root_.scodec.bits.BitVector
import zio.Chunk

sealed trait StreamingDecoded

object StreamingDecoded {

  /** A data-only object (no content stream). */
  final case class DataObj(obj: Obj) extends StreamingDecoded

  /** A version header. */
  final case class VersionT(version: Version) extends StreamingDecoded

  /** A textual xref. */
  final case class XrefT(xref: Xref) extends StreamingDecoded

  /** A `startxref / offset / %%EOF` triple. */
  final case class StartXrefT(startxref: StartXref) extends StreamingDecoded

  /** A comment line. */
  final case class CommentT(data: _root_.scodec.bits.ByteVector) extends StreamingDecoded

  /** Start of a content-stream-bearing object.
    *
    * @param inlinePayload
    *   If `Some`, the full raw stream (encoded) is already available and
    *   no `ContentObjBytes` / `ContentObjEnd` follow. If `None`, consume
    *   chunked payload until `ContentObjEnd`.
    */
  final case class ContentObjStart(obj: Obj, length: Long, inlinePayload: Option[BitVector])
    extends StreamingDecoded

  /** One chunk of the current content stream's raw payload. */
  final case class ContentObjBytes(bytes: Chunk[Byte]) extends StreamingDecoded

  /** End of the current content stream (only after `ContentObjStart` with no inline). */
  case object ContentObjEnd extends StreamingDecoded

  /** Final aggregate of accumulated metadata. Always the last event. */
  final case class Meta(
    xrefs: List[Xref],
    trailer: Option[Trailer],
    version: Option[Version]
  ) extends StreamingDecoded
}
