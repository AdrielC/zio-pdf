/*
 * Port of fs2.pdf.GenerateXref to Scala 3 + zio.NonEmptyChunk.
 *
 * Builds a deduplicated, padded, consecutive Xref from a list of
 * indexed XrefObjMeta entries.
 */

package zio.pdf

import zio.NonEmptyChunk

object GenerateXref {

  private[pdf] def xrefEntries(
    indexes: NonEmptyChunk[Obj.Index],
    sizes: NonEmptyChunk[Long],
    initialOffset: Long
  ): NonEmptyChunk[(Long, Xref.Entry)] = {
    val offsets = EncodeMeta.offsets(initialOffset)(sizes)
    val zipped  = indexes.zipWith(offsets) { case (index, offset) =>
      (index.number, EncodeMeta.objectXrefEntry(index, offset))
    }
    NonEmptyChunk.fromIterableOption(zipped.toList.sortBy(_._1)).get
  }

  private[pdf] def padEntries(
    entries: NonEmptyChunk[(Long, Xref.Entry)]
  ): NonEmptyChunk[Xref.Entry] = {
    // Walk left to right, inserting `dummy` entries for any gaps in
    // the object-number sequence.
    val it       = entries.iterator
    val first    = it.next()
    val builder  = scala.collection.mutable.ListBuffer[Xref.Entry](first._2)
    var prev     = first._1
    while (it.hasNext) {
      val (n, e) = it.next()
      val gap    = (n - prev - 1).toInt
      if (gap > 0) builder ++= List.fill(gap)(Xref.Entry.dummy)
      builder += e
      prev = n
    }
    NonEmptyChunk.fromIterableOption(builder.toList).get
  }

  private[pdf] def deduplicateEntries(
    entries: NonEmptyChunk[(Long, Xref.Entry)]
  ): NonEmptyChunk[(Long, Xref.Entry)] = {
    // Last-write-wins on duplicate object numbers.
    val it       = entries.iterator
    val first    = it.next()
    val out      = scala.collection.mutable.ListBuffer[(Long, Xref.Entry)](first)
    while (it.hasNext) {
      val (n, e) = it.next()
      if (out.last._1 == n) out(out.size - 1) = (n, e)
      else out += ((n, e))
    }
    NonEmptyChunk.fromIterableOption(out.toList).get
  }

  /**
   * @param meta indexes and byte offsets of referenced objects
   * @param trailerDict trailer that will be amended with the size of the xref
   * @param initialOffset bytes before the first referenced object (i.e. the version header)
   */
  def apply(
    meta: NonEmptyChunk[XrefObjMeta],
    trailerDict: Trailer,
    initialOffset: Long
  ): Xref = {
    val indexes        = meta.map(_.index)
    val sizes          = meta.map(_.size)
    val startxrefOffset = initialOffset + sizes.toList.sum
    val entries        = padEntries(deduplicateEntries(xrefEntries(indexes, sizes, initialOffset)))
    val firstNumber    = meta.toList.minBy(_.index.number).index.number
    val fromZero       = firstNumber == 1L
    val zeroIncrement  = if (fromZero) 1 else 0
    val trailer        = EncodeMeta.trailer(trailerDict, 0, entries.size + zeroIncrement)
    val withFree       =
      if (fromZero) NonEmptyChunk(Xref.Entry.freeHead, entries.toList*)
      else entries
    val tableOffset    = if (fromZero) 0L else firstNumber
    Xref(NonEmptyChunk(Xref.Table(tableOffset, withFree)), trailer, StartXref(startxrefOffset))
  }
}
