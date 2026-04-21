/*
 * Port of fs2.pdf.Decoded to Scala 3 + ZIO. The ADT itself is a
 * straight transliteration; the FS2-specific `objects`/`parts`
 * pipes are reimplemented in terms of ZStream + ZChannel.
 */

package zio.pdf

import _root_.scodec.bits.BitVector

sealed trait Decoded

object Decoded {
  final case class DataObj(obj: Obj)                                                        extends Decoded
  final case class ContentObj(obj: Obj, rawStream: BitVector, stream: Uncompressed)         extends Decoded
  final case class Meta(xrefs: List[Xref], trailer: Option[Trailer], version: Option[Version]) extends Decoded
}
