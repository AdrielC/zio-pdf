package zio.pdf

import zio.NonEmptyChunk
import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

final case class XrefStream(tables: NonEmptyChunk[Xref.Table], trailer: Trailer)

object XrefStream {
  /** Bounds decoded entries independently of compressed size, including /W [0 0 0]. */
  final case class Limits(maxEntries: Int = 1000000) {
    require(maxEntries > 0, "xref maxEntries must be positive")
  }
  object Limits {
    val default: Limits = Limits()
  }

  private def fail[A](detail: String): Attempt[A] = Attempt.failure(Err(s"xref stream: $detail"))

  private def natural(value: BigDecimal): Boolean =
    value >= 0 && value <= BigDecimal(Long.MaxValue) && value.isWhole

  def xrefStreamOffsets(data: Prim.Dict): Attempt[List[(BigDecimal, BigDecimal)]] =
    Prim.Dict.number("Size")(data).flatMap { size =>
      if !natural(size) then fail("Size must be a non-negative Long integer")
      else data("Index") match {
        case None => Attempt.successful(List((BigDecimal(0), size)))
        case Some(_) =>
          Prim.Dict.numbers("Index")(data).flatMap { index =>
            val ranges = scala.collection.mutable.ListBuffer.empty[(BigDecimal, BigDecimal)]
            var remaining = index
            var end = BigDecimal(0)
            var invalid = index.isEmpty
            while remaining.nonEmpty && !invalid do
              remaining match {
                case offset :: count :: tail =>
                  if !natural(offset) || !natural(count) || count == 0 || offset < end || offset + count > size then
                    invalid = true
                  else {
                    ranges += ((offset, count))
                    end = offset + count
                    remaining = tail
                  }
                case _ => invalid = true
              }
            if invalid then fail("Index must contain ordered, non-overlapping integer ranges within Size")
            else Attempt.successful(ranges.toList)
          }
      }
    }

  def xrefStreamFieldWidths(size: Long)(offsets: List[(BigDecimal, BigDecimal)])(
    data: Prim.Dict
  ): Attempt[(BigDecimal, BigDecimal, BigDecimal)] =
    Prim.Dict.numbers("W")(data).flatMap {
      case List(w1, w2, w3) if List(w1, w2, w3).forall(w => w.isWhole && w >= 0 && w <= 8) =>
        val expected = offsets.foldLeft(BigDecimal(0))((sum, range) => sum + range._2) * (w1 + w2 + w3)
        if expected == BigDecimal(size) then Attempt.successful((w1, w2, w3))
        else fail(s"payload length $size does not match W and Index (expected $expected bytes)")
      case _ => fail("W must contain exactly three integer widths between 0 and 8 bytes")
    }

  def decodeXrefStream(data: Prim.Dict, stream: BitVector): Attempt[(BigDecimal, List[Xref.Table])] =
    decodeXrefStream(data, stream, Limits.default)

  def decodeXrefStream(data: Prim.Dict, stream: BitVector, limits: Limits): Attempt[(BigDecimal, List[Xref.Table])] =
    for {
      offsets <- xrefStreamOffsets(data)
      count = offsets.foldLeft(BigDecimal(0))((sum, range) => sum + range._2)
      _ <- if count > 0 && count <= limits.maxEntries then Attempt.successful(())
           else fail(s"entry limit: expected 1 through ${limits.maxEntries}, declared $count")
      _ <- if stream.size % 8 == 0 then Attempt.successful(()) else fail("payload is not byte aligned")
      widths <- xrefStreamFieldWidths(stream.size / 8)(offsets)(data)
      (w1, w2, w3) = widths
      tables <- XrefStreamCodec.decode(offsets, w1.toInt, w2.toInt, w3.toInt, stream)
    } yield (count, tables)

  val unwantedTrailerKeys: List[String] =
    List("W", "Index", "Filter", "Length", "Type", "Size", "Prev")

  def cleanTrailer(data: Prim.Dict): Prim.Dict =
    Prim.Dict(zio.blocks.chunk.ChunkMap.from(
      data.data.view.filterKeys(k => !unwantedTrailerKeys.contains(k))
    ))

  def trailerSize(data: Prim.Dict): Option[BigDecimal] =
    Prim.Dict.path("Size")(data) { case Prim.Number(s) => s }.toOption

  def apply(data: Prim.Dict)(stream: BitVector): Attempt[XrefStream] =
    apply(data, Limits.default)(stream)

  def apply(data: Prim.Dict, limits: Limits)(stream: BitVector): Attempt[XrefStream] =
    decodeXrefStream(data, stream, limits).flatMap { case (entryCount, tables) =>
      NonEmptyChunk.fromIterableOption(tables) match {
        case Some(nec) => Attempt.successful(XrefStream(
          nec, Trailer(trailerSize(data).getOrElse(entryCount), cleanTrailer(data), data.ref("Root"))
        ))
        case None => fail("no tables")
      }
    }
}

private[pdf] object XrefStreamCodec {
  /** Widths and payload length are validated once before this loop. No per-field codec/bit slices. */
  def decode(
    offsets: List[(BigDecimal, BigDecimal)], width1: Int, width2: Int, width3: Int, bytes: BitVector
  ): Attempt[List[Xref.Table]] = {
    var cursor = 0L
    def unsigned(width: Int, default: Long): Long = {
      if width == 0 then default
      else {
        var value = 0L
        val end = cursor + width
        while cursor < end do
          value = (value << 8) | (bytes.getByte(cursor).toLong & 0xffL)
          cursor += 1
        value
      }
    }
    val tables = scala.collection.mutable.ListBuffer.empty[Xref.Table]
    var error: Option[Err] = None
    val ranges = offsets.iterator
    while ranges.hasNext && error.isEmpty do
      val (offset, size) = ranges.next()
      val entries = scala.collection.mutable.ListBuffer.empty[Xref.Entry]
      var i = 0
      val count = size.toInt // Total entry count was checked against Limits.maxEntries.
      while i < count && error.isEmpty do
        val kind = unsigned(width1, 1L)
        val second = unsigned(width2, 0L)
        val third = unsigned(width3, 0L)
        if kind == 0L || kind == 1L then
          if second < 0L then error = Some(Err(s"xref stream: offset exceeds Long.MaxValue at object ${offset.toLong + i}"))
          // DocuSign uses uint32 all-ones for object zero's permanently-free sentinel.
          // Normalize that one sentinel, never a generation of an in-use object.
          else if kind == 0L && offset == 0 && i == 0 && second == 0L && width3 == 4 && third == 0xffffffffL then
            entries += Xref.Entry.freeHead
          else if third < 0L || third > 65535L then
            error = Some(Err(s"xref stream: generation must be between 0 and 65535 at object ${offset.toLong + i}"))
          else entries += Xref.entry(second, third.toInt, if kind == 0L then Xref.EntryType.Free else Xref.EntryType.InUse)
        else if kind == 2L then
          if second < 0L || third < 0L || third > Int.MaxValue.toLong then
            error = Some(Err(s"xref stream: compressed object number or index exceeds supported range at object ${offset.toLong + i}"))
          else entries += Xref.compressed(second, third.toInt, Xref.EntryType.InUse)
        else
          // ISO 32000-2 7.5.8.3: unrecognized types denote a null reference.
          entries += Xref.Entry.freeHead
        i += 1
      if error.isEmpty then
        NonEmptyChunk.fromIterableOption(entries) match {
          case Some(values) => tables += Xref.Table(offset.toLong, values)
          case None => error = Some(Err("xref stream: empty table"))
        }
    error match {
      case Some(err) => Attempt.failure(err)
      case None => Attempt.successful(tables.toList)
    }
  }
}
