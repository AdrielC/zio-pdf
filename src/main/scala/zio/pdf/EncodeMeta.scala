/*
 * Port of fs2.pdf.EncodeMeta to Scala 3 + zio.NonEmptyChunk.
 */

package zio.pdf

import zio.NonEmptyChunk

object EncodeMeta {

  def xrefEntry(offset: Long, generation: Int): Xref.Entry =
    Xref.entry(offset, generation, Xref.EntryType.InUse)

  private[pdf] def objectXrefEntry: (Obj.Index, Long) => Xref.Entry = {
    case (Obj.Index(_, generation), offset) => xrefEntry(offset, generation)
  }

  private[pdf] def trailerUpdate(previousSize: Long, newEntries: Int): Prim.Dict =
    Prim.dict("Size" -> Prim.num(previousSize + newEntries))

  private[pdf] def mergeTrailerDicts(update: Prim.Dict): Trailer => Prim.Dict = {
    case Trailer(_, Prim.Dict(data), _) => Prim.Dict(data ++ update.data)
  }

  /** Build a fresh trailer by merging /Size into the previous one. */
  def trailer(previousTrailer: Trailer, previousSize: Long, newEntries: Int): Trailer =
    Trailer(
      previousSize + newEntries,
      mergeTrailerDicts(trailerUpdate(previousSize, newEntries))(previousTrailer),
      previousTrailer.root
    )

  /** Cumulative byte offsets of a list of objects, given their
    * sizes. The result has the same length as `sizes` and the
    * head equals `base`. */
  def offsets(base: Long)(sizes: NonEmptyChunk[Long]): NonEmptyChunk[Long] = {
    val tail = sizes.tail.scanLeft(base + sizes.head)(_ + _)
    NonEmptyChunk(base, tail*)
  }
}
