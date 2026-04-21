/*
 * Memory-bounded decoder events.
 *
 * The standard `Decoded` ADT carries each content stream's payload
 * as either an `Uncompressed` (lazy `BitVector`) or, for object-
 * stream extracts, a list of fully-decoded `DataObj`s. Both shapes
 * imply that the entire payload of any single content stream lives
 * in memory for at least as long as the consumer holds onto it.
 *
 * `StreamingDecoded` is the SAX-style alternative: instead of
 * materialising each content stream's payload, the pipeline emits
 * a `ContentObjHeader` event followed by a sequence of
 * `ContentObjBytes` events carrying chunks of the payload as they
 * are read from upstream, terminated by a `ContentObjEnd` event.
 * Peak memory is the upstream chunk size, regardless of how big
 * any individual content stream is.
 *
 * Use this when:
 *   - You're processing a PDF with very large embedded streams
 *     (font subsets, attachment files, raster images) and want a
 *     bounded-memory pipeline.
 *   - You want to forward content stream bytes straight to a sink
 *     (e.g. a CDC chunker, a S3 multipart upload, a hash digest)
 *     without materialising them in memory first.
 *
 * Stick with `Decoded` when:
 *   - The content streams are small (typical text-heavy PDFs).
 *   - You need lazy decompression (Decoded.ContentObj.stream.exec).
 *   - You need to extract object streams (/Type /ObjStm) without
 *     re-implementing the inner decoder yourself.
 */

package zio.pdf

import zio.Chunk

sealed trait StreamingDecoded

object StreamingDecoded {

  /** A data-only object (no content stream) - emitted whole, just
    * like `Decoded.DataObj`. */
  final case class DataObj(obj: Obj) extends StreamingDecoded

  /** A version header. */
  final case class VersionT(version: Version) extends StreamingDecoded

  /** A textual xref. */
  final case class XrefT(xref: Xref) extends StreamingDecoded

  /** A `startxref / offset / %%EOF` triple. */
  final case class StartXrefT(startxref: StartXref) extends StreamingDecoded

  /** A comment line. */
  final case class CommentT(data: _root_.scodec.bits.ByteVector) extends StreamingDecoded

  /** A content-stream-bearing object's header (number, generation,
    * data dict, `/Length`). The next zero-or-more events for this
    * object are `ContentObjBytes` events totalling exactly `length`
    * bytes, terminated by a `ContentObjEnd` event. */
  final case class ContentObjHeader(obj: Obj, length: Long) extends StreamingDecoded

  /** One chunk of the current content stream's raw payload. */
  final case class ContentObjBytes(bytes: Chunk[Byte]) extends StreamingDecoded

  /** End of the current content stream. */
  case object ContentObjEnd extends StreamingDecoded

  /** Final aggregate of accumulated metadata. Always the last event. */
  final case class Meta(
    xrefs: List[Xref],
    trailer: Option[Trailer],
    version: Option[Version]
  ) extends StreamingDecoded
}
