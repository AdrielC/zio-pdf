/*
 * Port of fs2.pdf.Obj to Scala 3 + scodec 2.3.
 *
 * `Obj` = the data part of an indirect object: an `Index` (number,
 * generation) plus the `Prim` data (usually a `Dict`).
 */

package zio.pdf

import zio.pdf.codec.{Text, Whitespace}
import _root_.scodec.Codec

final case class Obj(index: Obj.Index, data: Prim)

object Obj extends ObjCodec {

  /** Indexing metadata: object number + generation number. */
  final case class Index(number: Long, generation: Int)

  object Index {
    import Text.{ascii, str}
    import Whitespace.space

    given Codec[Index] =
      ((ascii.long <~ space) :: (ascii.int <~ space <~ str("obj")))
        .xmap({ case (n, g) => Index(n, g) }, i => (i.number, i.generation))
  }

  object tpe {
    def unapply(obj: Obj): Option[(String, Prim.Dict)] = Prim.tpe.unapply(obj.data)
  }

  object subtype {
    def unapply(obj: Obj): Option[(String, Prim.Dict)] = Prim.subtype.unapply(obj.data)
  }

  object dict {
    def unapply(obj: Obj): Option[(Long, Prim.Dict)] = obj match {
      case Obj(Index(num, _), d @ Prim.Dict(_)) => Some((num, d))
      case _                                      => None
    }
  }
}

private[pdf] trait ObjCodec {
  import Whitespace.{nlWs, skipWs}
  import Text.str

  val codecPreStream: Codec[Obj] =
    ((skipWs ~> summon[Codec[Obj.Index]] <~ nlWs) :: Prim.Codec_Prim <~ nlWs)
      .xmap({ case (i, p) => Obj(i, p) }, o => (o.index, o.data))

  given Codec[Obj] =
    (codecPreStream <~ str("endobj") <~ nlWs)
}
