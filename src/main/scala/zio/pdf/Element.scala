/*
 * Port of fs2.pdf.Element (with Page / Pages / FontResource / Image)
 * to Scala 3.
 */

package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

/** Page geometry, mandatory in `/Type /Page` dicts. */
final case class MediaBox(x: BigDecimal, y: BigDecimal, w: BigDecimal, h: BigDecimal)

/** A `/Type /Page` object with its required `/MediaBox`. */
final case class Page(index: Obj.Index, data: Prim.Dict, mediaBox: MediaBox)

object Page {
  def fromData(index: Obj.Index): Prim => Attempt[Page] = {
    case Prim.tpe("Page", data) =>
      Prim.Dict.path("MediaBox")(data) {
        // A PDF MediaBox is a four-element array [llx lly urx ury] of
        // numbers. Guard keeps the partial function honest -- if the
        // shape doesn't match, `Prim.Dict.path`'s `.lift` returns None
        // and the outer machinery reports the error.
        case Prim.Array(elems)
            if elems.length == 4
              && elems.forall(_.isInstanceOf[Prim.Number]) =>
          val Vector(x, y, w, h) = elems.iterator
            .collect { case Prim.Number(n) => n }
            .toVector: @unchecked
          Page(index, data, MediaBox(x, y, w, h))
      }
    case _ =>
      Attempt.failure(Err("not a Page object"))
  }

  object obj {
    def unapply(o: IndirectObj): Option[Page] = fromData(o.obj.index)(o.obj.data).toOption
  }
}

/** A `/Type /Pages` object with its `/Kids` references. */
final case class Pages(index: Obj.Index, data: Prim.Dict, kids: List[Prim.Ref], root: Boolean)

object Pages {
  def fromData(index: Obj.Index): Prim => Attempt[Pages] = {
    case Prim.tpe("Pages", data) =>
      Prim.Dict.path("Kids")(data) {
        case Prim.refs(kids) => Pages(index, data, kids, !data.data.contains("Parent"))
      }
    case _ =>
      Attempt.failure(Err("not a Pages object"))
  }

  object obj {
    def unapply(o: IndirectObj): Option[Pages] = fromData(o.obj.index)(o.obj.data).toOption
  }
}

/** Marker for objects containing a `/Font` key. */
final case class FontResource(index: Obj.Index, data: Prim.Dict)

/** Indirect object whose data is a sole array. */
final case class IndirectArray(index: Obj.Index, data: Prim.Array)

/** Image with codec discriminator + uncompressed payload. */
final case class Image(data: Prim.Dict, stream: Uncompressed, codec: Image.Codec)

object Image {
  sealed trait Codec
  object Codec {
    case object Jpg   extends Codec
    case object Ccitt extends Codec

    def extension: Codec => String = {
      case Jpg   => "jpg"
      case Ccitt => "tiff"
    }
  }
}

/** Semantic abstraction of a decoded PDF object. */
sealed trait Element

object Element {
  sealed trait DataKind
  object DataKind {
    case object General                                       extends DataKind
    final case class Page(page: zio.pdf.Page)                 extends DataKind
    final case class Pages(pages: zio.pdf.Pages)              extends DataKind
    final case class Array(data: Prim.Array)                  extends DataKind
    final case class FontResource(res: zio.pdf.FontResource)  extends DataKind
    final case class Info(data: Prim.Dict)                    extends DataKind
  }

  final case class Data(obj: Obj, kind: DataKind) extends Element

  sealed trait ContentKind
  object ContentKind {
    case object General                              extends ContentKind
    final case class Image(image: zio.pdf.Image)     extends ContentKind
  }

  final case class Content(obj: Obj, rawStream: BitVector, stream: Uncompressed, kind: ContentKind) extends Element

  final case class Meta(trailer: Option[Trailer], version: Option[Version]) extends Element

  object obj {
    def unapply(e: Element): Option[IndirectObj] = e match {
      case Data(obj, _)               => Some(IndirectObj(obj, None))
      case Content(obj, stream, _, _) => Some(IndirectObj(obj, Some(stream)))
      case Meta(_, _)                 => None
    }
  }
}
