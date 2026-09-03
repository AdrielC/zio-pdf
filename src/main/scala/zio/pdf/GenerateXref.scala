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

  /** Group object numbers into consecutive runs for multi-subsection xref tables. */
  private[pdf] def sectionTables(
    entries: NonEmptyChunk[(Long, Xref.Entry)]
  ): NonEmptyChunk[Xref.Table] = {
    val sorted = entries.toList.sortBy(_._1)
    val runs   = scala.collection.mutable.ListBuffer[List[(Long, Xref.Entry)]]()
    var current = scala.collection.mutable.ListBuffer(sorted.head)
    sorted.tail.foreach { case (number, entry) =>
      if number == current.last._1 + 1L then current += ((number, entry))
      else
        runs += current.toList
        current = scala.collection.mutable.ListBuffer((number, entry))
    }
    runs += current.toList
    NonEmptyChunk.fromIterableOption(
      runs.toList.flatMap { run =>
        val firstNumber = run.head._1
        NonEmptyChunk.fromIterableOption(run.map(_._2)).map { tableEntries =>
          if firstNumber == 1L then
            Xref.Table(0L, NonEmptyChunk(Xref.Entry.freeHead, tableEntries.toList*))
          else
            Xref.Table(firstNumber, tableEntries)
        }
      }
    ).get
  }

  private[pdf] def trailerSize(entries: NonEmptyChunk[(Long, Xref.Entry)]): Long =
    entries.toList.map(_._1).max + 1L

  /** Build xref tables from absolute object offsets (for merged linearized tails). */
  def fromAbsolute(
    objects: NonEmptyChunk[(Long, Long, Long)],
    trailerDict: Trailer,
    startxrefOffset: Long
  ): Xref = {
    val numbered = objects.map { case (number, offset, _) =>
      (number, Xref.entry(offset, 0, Xref.EntryType.InUse))
    }
    val tables  = sectionTables(numbered)
    val trailer = EncodeMeta.trailer(trailerDict, 0, trailerSize(numbered).toInt)
    Xref(tables, trailer, StartXref(startxrefOffset))
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
    val indexes         = meta.map(_.index)
    val sizes           = meta.map(_.size)
    val startxrefOffset = initialOffset + sizes.toList.sum
    val numbered        = deduplicateEntries(xrefEntries(indexes, sizes, initialOffset))
    val tables          = sectionTables(numbered)
    val size            = trailerSize(numbered)
    val trailer         = EncodeMeta.trailer(trailerDict, 0, size.toInt)
    Xref(tables, trailer, StartXref(startxrefOffset))
  }
}
