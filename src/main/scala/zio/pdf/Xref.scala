/*
 * Port of fs2.pdf.Xref (Trailer + Xref + textual xref codec) to
 * Scala 3 + scodec 2.3. NonEmptyList from cats has been replaced
 * with `zio.NonEmptyChunk`.
 */

package zio.pdf

import zio.NonEmptyChunk
import zio.pdf.codec.{Many, Newline, Text, Whitespace}
import _root_.scodec.{Attempt, Codec, Err}

final case class Trailer(size: BigDecimal, data: Prim.Dict, root: Option[Prim.Ref])

object Trailer {

  def sanitize(trailers: NonEmptyChunk[Trailer]): Trailer = {
    val maxSize = trailers.foldLeft(trailers.head.size)((acc, t) => if (t.size > acc) t.size else acc)
    val merged  = trailers.tail.foldLeft(trailers.head.data.data) { (acc, t) =>
      acc ++ t.data.data
    }.removed("Prev").removed("DecodeParms")
    Trailer(maxSize, Prim.Dict(merged), trailers.head.root)
  }

  def fromData(data: Prim.Dict): Attempt[Trailer] =
    Prim.Dict.path("Size")(data) {
      case Prim.Number(size) =>
        val root = Prim.Dict.path("Root")(data) { case ref @ Prim.Ref(_, _) => ref }.toOption
        Trailer(size, data, root)
    }
}

final case class Xref(tables: NonEmptyChunk[Xref.Table], trailer: Trailer, startxref: StartXref)

object Xref extends XrefCodec {

  final case class Table(offset: Long, entries: NonEmptyChunk[Xref.Entry])

  final case class Entry(index: Index, `type`: EntryType)

  object Entry {
    def freeHead: Entry = entry(0, 65535, EntryType.Free)
    def dummy: Entry    = entry(0, 0, EntryType.InUse)
  }

  sealed trait Index
  object Index {
    final case class Regular(offset: String, generation: String) extends Index
    final case class Compressed(obj: Long, index: Int)            extends Index
  }

  sealed trait EntryType
  object EntryType {
    case object InUse extends EntryType
    case object Free  extends EntryType

    def to: EntryType => Attempt[Byte] = {
      case InUse => Attempt.successful('n'.toByte)
      case Free  => Attempt.successful('f'.toByte)
    }

    def from: Byte => Attempt[EntryType] = {
      case b if b == 'n'.toByte => Attempt.successful(InUse)
      case b if b == 'f'.toByte => Attempt.successful(Free)
      case b                     => Attempt.failure(Err(s"invalid xref entry type byte `${b.toChar}`"))
    }
  }

  def padZeroes[A: Numeric](max: Int)(number: A): String = {
    val numberString = number.toString
    val padding      = new String(Array.fill(max - numberString.length)('0'))
    padding + numberString
  }

  def entry(offset: Long, generation: Int, t: EntryType): Entry =
    Entry(Index.Regular(padZeroes(10)(offset), padZeroes(5)(generation)), t)

  def compressed(obj: Long, index: Int, t: EntryType): Entry =
    Entry(Index.Compressed(obj, index), t)
}

private[pdf] trait XrefCodec {
  import _root_.scodec.codecs.{choice, listOfN, provide, byte}
  import Newline.{crlf, lf, newline}
  import Whitespace.{nlWs, skipWs, space, ws}
  import Text.{ascii, str, stringOf}

  def offset: Codec[String]     = stringOf(10).withContext("offset") <~ space.withContext("offset space")
  def generation: Codec[String] = stringOf(5).withContext("generation") <~ space.withContext("generation space")

  given regularIndex: Codec[Xref.Index.Regular] =
    (offset :: generation)
      .xmap({ case (o, g) => Xref.Index.Regular(o, g) }, i => (i.offset, i.generation))
      .withContext("regular index")

  given compressedIndex: Codec[Xref.Index.Compressed] =
    (ascii.long :: ascii.int)
      .xmap({ case (o, i) => Xref.Index.Compressed(o, i) }, c => (c.obj, c.index))
      .withContext("compressed index")

  def index: Codec[Xref.Index] =
    Codec(
      _root_.scodec.Encoder { (i: Xref.Index) =>
        i match {
          case r: Xref.Index.Regular    => regularIndex.encode(r)
          case c: Xref.Index.Compressed => compressedIndex.encode(c)
        }
      },
      _root_.scodec.Decoder.choiceDecoder[Xref.Index](
        regularIndex.upcast[Xref.Index],
        compressedIndex.upcast[Xref.Index]
      )
    )

  def entryType: Codec[Xref.EntryType] =
    byte.exmap(Xref.EntryType.from, Xref.EntryType.to)

  def twoByteNewline: Codec[Unit] =
    choice(
      space.withContext("end space") <~ lf.withContext("end newline"),
      crlf.withContext("end newline")
    )

  def entry: Codec[Xref.Entry] =
    (index :: entryType <~ twoByteNewline)
      .xmap({ case (i, t) => Xref.Entry(i, t) }, e => (e.index, e.`type`))

  def range: Codec[(Long, Int)] =
    (ascii.long.withContext("offset") <~ ws) :: (ascii.int.withContext("size") <~ newline)

  def trailerDict: Codec[Trailer] =
    Prim.Codec_Dict
      .withContext("trailer")
      .exmap(Trailer.fromData, a => Attempt.successful(a.data))

  def trailerKw: Codec[Unit] = str("trailer") <~ nlWs

  def Codec_Trailer: Codec[Trailer] = trailerKw ~> trailerDict <~ nlWs

  def table: Codec[Xref.Table] =
    range
      .withContext("range")
      .flatZip { case (_, size) => listOfN(provide(size), entry).withContext("entries") }
      .withContext("table")
      .exmap[Xref.Table](
        { case ((o, _), es) =>
          NonEmptyChunk.fromIterableOption(es) match {
            case Some(nec) => Attempt.successful(Xref.Table(o, nec))
            case None      => Attempt.failure(Err("xref table entries: empty list"))
          }
        },
        t => Attempt.successful(((t.offset, t.entries.size), t.entries.toList))
      )

  def tables: Codec[NonEmptyChunk[Xref.Table]] = {
    // Decode tables until `trailer` keyword appears.
    val many = Many.till[Xref.Table](b => trailerKw.decode(b).isSuccessful)(table)
    many.exmap(
      ts =>
        NonEmptyChunk.fromIterableOption(ts) match {
          case Some(nec) => Attempt.successful(nec)
          case None      => Attempt.failure(Err("xref tables: empty"))
        },
      nec => Attempt.successful(nec.toList)
    ).withContext("xref tables")
  }

  given Codec[Xref] =
    (
      (str("xref") ~> nlWs) ~>
        tables ::
        (skipWs ~> Codec_Trailer) ::
        StartXref.codec
    ).xmap[Xref](
      { case (ts, t, sx) => Xref(ts, t, sx) },
      x => (x.tables, x.trailer, x.startxref)
    )
}
