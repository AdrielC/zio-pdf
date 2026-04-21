/*
 * Port of fs2.pdf.XrefStream to Scala 3 + scodec 2.3.
 */

package zio.pdf

import zio.NonEmptyChunk
import zio.pdf.codec.Codecs
import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder, Err}
import _root_.scodec.bits.BitVector

final case class XrefStream(tables: NonEmptyChunk[Xref.Table], trailer: Trailer)

object XrefStream {

  def xrefStreamOffsets(data: Prim.Dict): Attempt[List[(BigDecimal, BigDecimal)]] =
    Prim.Dict.number("Size")(data).flatMap { size =>
      val explicit = Prim.Dict.numbers("Index")(data).flatMap { index =>
        // Pair up consecutive elements as (offset, size).
        index.grouped(2).foldLeft[Attempt[List[(BigDecimal, BigDecimal)]]](Attempt.successful(Nil)) {
          case (Attempt.Successful(z), List(off, sz)) => Attempt.successful((off, sz) :: z)
          case (Attempt.Successful(_), _)              => Codecs.fail("broken xref stream index")
          case (f @ Attempt.Failure(_), _)             => f
        }
      }
      explicit.fold(_ => Attempt.successful(List((BigDecimal(0), size))), Attempt.successful(_))
    }

  def xrefStreamFieldWidths(size: Long)(offsets: List[(BigDecimal, BigDecimal)])(
    data: Prim.Dict
  ): Attempt[(BigDecimal, BigDecimal, BigDecimal)] = {
    val totalEntries = offsets.map(_._2).sum
    val entrySize    = if (totalEntries == BigDecimal(0)) BigDecimal(0) else BigDecimal(size) / totalEntries
    Prim.Dict.numbers("W")(data).flatMap {
      case List(w1, w2, w3) if entrySize == (w1 + w2 + w3) =>
        Attempt.successful(((if (w1 == 0) BigDecimal(1) else w1), w2, w3))
      case other =>
        Codecs.fail(s"invalid xref stream field widths: $other (size: $size, entry size: $entrySize)")
    }
  }

  def decodeXrefStream(data: Prim.Dict, stream: BitVector): Attempt[(BigDecimal, List[Xref.Table])] =
    for {
      offsets         <- xrefStreamOffsets(data)
      widths          <- xrefStreamFieldWidths(stream.bytes.size)(offsets)(data)
      (w1, w2, w3)     = widths
      DecodeResult(tables, _) <- XrefStreamCodec.decoder(offsets, w1, w2, w3).decode(stream)
    } yield (BigDecimal(tables.foldLeft(0)((acc, t) => acc + t.entries.size)), tables)

  val unwantedTrailerKeys: List[String] =
    List("W", "Index", "Filter", "Length", "Type", "Size", "Prev")

  def cleanTrailer(data: Prim.Dict): Prim.Dict =
    Prim.Dict(zio.blocks.chunk.ChunkMap.from(
      data.data.view.filterKeys(k => !unwantedTrailerKeys.contains(k))
    ))

  def trailerSize(data: Prim.Dict): Option[BigDecimal] =
    Prim.Dict.path("Size")(data) { case Prim.Number(s) => s }.toOption

  def apply(data: Prim.Dict)(stream: BitVector): Attempt[XrefStream] =
    for {
      r        <- decodeXrefStream(data, stream)
      (entryCount, tables) = r
      nec      <-
        NonEmptyChunk.fromIterableOption(tables) match {
          case Some(n) => Attempt.successful(n)
          case None    => Attempt.failure(Err("no tables in xref stream"))
        }
    } yield XrefStream(
      nec,
      Trailer(trailerSize(data).getOrElse(entryCount), cleanTrailer(data), data.ref("Root"))
    )
}

private[pdf] object XrefStreamCodec {
  import _root_.scodec.codecs.{provide, uint, ulong, ushort}

  def parseEntry: ((Short, Long), Int) => Attempt[Xref.Entry] = {
    case ((0, nextFree), generation) =>
      Attempt.successful(Xref.entry(nextFree, generation, Xref.EntryType.Free))
    case ((1, offset), generation) =>
      Attempt.successful(Xref.entry(offset, generation, Xref.EntryType.InUse))
    case ((2, number), index) =>
      Attempt.successful(Xref.compressed(number, index, Xref.EntryType.InUse))
    case other =>
      Attempt.failure(Err(s"invalid xref stream entry: $other"))
  }

  def entryField[A](num: Int, width: BigDecimal, default: A)(dec: => Codec[A]): Codec[A] =
    if (width == 0) provide(default) else dec.withContext(s"xref entry field $num ($width bytes)")

  def entryDecoder(width1: BigDecimal, width2: BigDecimal, width3: BigDecimal): Decoder[((Short, Long), Int)] =
    (entryField(1, width1, 1.toShort)(ushort(width1.toInt * 8)) ::
      entryField(2, width2, 0L)(ulong(width2.toInt * 8)) ::
      entryField(3, width3, 0)(uint(width3.toInt * 8)))
      .xmap[((Short, Long), Int)](
        tup => ((tup._1, tup._2), tup._3),
        { case ((s, l), i) => (s, l, i) }
      )

  def tableDecoder(width1: BigDecimal, width2: BigDecimal, width3: BigDecimal)
    : ((BigDecimal, BigDecimal)) => Decoder[Xref.Table] = { case (offset, size) =>
    val decoder = entryDecoder(width1, width2, width3)
    Decoder { bits =>
      // Collect `size.toInt` entries.
      val n = size.toInt
      var rem  = bits
      val buf  = scala.collection.mutable.ListBuffer.empty[Xref.Entry]
      var i    = 0
      var err: Attempt.Failure = null
      while (i < n && err == null) {
        decoder.decode(rem) match {
          case Attempt.Successful(DecodeResult(raw, r)) =>
            val ((s, l), idx) = raw
            parseEntry((s, l), idx) match {
              case Attempt.Successful(entry) => buf += entry; rem = r
              case f: Attempt.Failure         => err = f
            }
          case f: Attempt.Failure => err = f
        }
        i += 1
      }
      if (err != null) err
      else
        NonEmptyChunk.fromIterableOption(buf.toList) match {
          case Some(nec) => Attempt.successful(DecodeResult(Xref.Table(offset.toLong, nec), rem))
          case None      => Attempt.failure(Err("xref stream table: empty"))
        }
    }
  }

  def decoder(
    offsets: List[(BigDecimal, BigDecimal)],
    width1: BigDecimal,
    width2: BigDecimal,
    width3: BigDecimal
  ): Decoder[List[Xref.Table]] =
    Decoder { bits =>
      var rem = bits
      val buf = scala.collection.mutable.ListBuffer.empty[Xref.Table]
      var err: Attempt.Failure = null
      offsets.foreach { off =>
        if (err == null) {
          tableDecoder(width1, width2, width3)(off).decode(rem) match {
            case Attempt.Successful(DecodeResult(t, r)) => buf += t; rem = r
            case f: Attempt.Failure                       => err = f
          }
        }
      }
      if (err != null) err
      else Attempt.successful(DecodeResult(buf.toList, rem))
    }
}
