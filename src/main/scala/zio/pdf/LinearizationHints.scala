/*
 * ISO linearization hint table builder (Annex F page offset + shared object tables).
 */

package zio.pdf

import _root_.scodec.bits.BitVector

object LinearizationHints {

  /** Skip full shared-object tables above this many non-first-page objects. */
  val MaxSharedObjects: Int = 512

  /** Cap per-page shared-object references encoded in the page hint table. */
  val MaxSharedIdsPerPage: Int = 32

  /** Refuse to emit a hint stream when the uncompressed tables exceed this size. */
  val MaxUncompressedHintBytes: Int = 256 * 1024

  final case class HintStreamBytes(
    uncompressed: BitVector,
    compressed: BitVector,
    sharedSectionOffset: Int
  )

  final case class PageEntry(
    objectCountDelta: Long,
    pageLengthDelta: Long,
    sharedObjectCount: Int,
    sharedObjectIds: List[Int],
    sharedNumerators: List[Int],
    contentStreamOffset: Long,
    contentStreamLength: Long
  )

  final case class PageOffsetHeader(
    leastObjectCount: Int,
    locationOfFirstPage: Long,
    bitsObjectCountDelta: Int,
    leastPageLength: Long,
    bitsPageLengthDelta: Int,
    leastContentOffset: Long,
    bitsContentOffsetDelta: Int,
    leastContentLength: Long,
    bitsContentLengthDelta: Int,
    bitsSharedObjectCount: Int,
    bitsSharedObjectId: Int,
    bitsNumerator: Int,
    denominator: Int
  )

  final case class PageOffsetHintTable(header: PageOffsetHeader, entries: List[PageEntry])

  final case class SharedObjectHeader(
    firstObjectNumber: Long,
    location: Long,
    firstPageEntries: Int,
    sectionEntries: Int,
    bitsGroupObjectCount: Int,
    leastLength: Long,
    bitsLengthDelta: Int
  )

  final case class SharedGroup(objectCount: Int)

  final case class SharedEntry(
    lengthMinusLeast: Long,
    signaturePresent: Boolean,
    nobjectsMinusOne: Int
  )

  final case class SharedObjectHintTable(
    header: SharedObjectHeader,
    groups: List[SharedGroup],
    objects: List[SharedEntry]
  )

  private final class HintStreamBuilder {
    private val buf         = scala.collection.mutable.ArrayBuffer.empty[Byte]
    private var pendingBits = 0
    private var pendingByte = 0

    def writeBits(value: Long, bits: Int): Unit =
      if bits > 0 then
        var remaining = bits
        while remaining > 0 do
          val free = 8 - pendingBits
          if remaining >= free then
            val shift = remaining - free
            val mask  = if free >= 64 then -1L else (1L << free) - 1L
            pendingByte |= ((value >> shift) & mask).toInt
            buf += pendingByte.toByte
            pendingByte = 0
            pendingBits = 0
            remaining -= free
          else
            val shift = free - remaining
            val mask  = if remaining >= 64 then -1L else (1L << remaining) - 1L
            pendingByte |= ((value & mask).toInt << shift)
            pendingBits += remaining
            remaining = 0

    def alignToByte(): Unit =
      if pendingBits > 0 then
        buf += pendingByte.toByte
        pendingByte = 0
        pendingBits = 0

    def byteLen: Int = buf.size + (if pendingBits > 0 then 1 else 0)

    def result: BitVector = BitVector.view(buf.toArray)
  }

  def encode(pageOffset: PageOffsetHintTable, sharedObject: SharedObjectHintTable): Either[String, HintStreamBytes] =
    try
      val builder = new HintStreamBuilder
      encodePageOffsetHeader(builder, pageOffset)
      builder.alignToByte()
      encodePageOffsetEntries(builder, pageOffset)
      builder.alignToByte()
      val sharedSectionOffset = builder.byteLen
      encodeSharedObjectHeader(builder, sharedObject)
      builder.alignToByte()
      encodeSharedObjectGroups(builder, sharedObject)
      builder.alignToByte()
      encodeSharedObjectEntries(builder, sharedObject)
      builder.alignToByte()
      val uncompressed = builder.result
      if uncompressed.size > MaxUncompressedHintBytes then
        Left(s"hint stream exceeds ${MaxUncompressedHintBytes}-byte cap (${uncompressed.size} bytes)")
      else
        FlateEncode(uncompressed) match {
          case _root_.scodec.Attempt.Successful(compressed) =>
            Right(HintStreamBytes(uncompressed, compressed, sharedSectionOffset))
          case _root_.scodec.Attempt.Failure(cause) =>
            Left(s"hint stream FlateEncode: ${cause.messageWithContext}")
        }
    catch {
      case ex: Throwable => Left(ex.getMessage)
    }

  def fromMeasured(
    measured: PartLayout.Measured,
    pageObjectNumbers: List[Long],
    firstPageObjectNumbers: Set[Long],
    graph: Option[PageDependencyGraph.Graph] = None
  ): Either[String, Option[(PageOffsetHintTable, SharedObjectHintTable)]] =
    if pageObjectNumbers.isEmpty then Left("linearization hints require at least one page")
    else
      val sharedEntries = measured.entries.filterNot(entry => firstPageObjectNumbers(entry.index.number))
      if sharedEntries.size > MaxSharedObjects then Right(None)
      else buildTables(measured, pageObjectNumbers, firstPageObjectNumbers, sharedEntries, graph).map(Some(_))

  private def buildTables(
    measured: PartLayout.Measured,
    pageObjectNumbers: List[Long],
    firstPageObjectNumbers: Set[Long],
    sharedEntries: List[PartLayout.Entry],
    graph: Option[PageDependencyGraph.Graph]
  ): Either[String, (PageOffsetHintTable, SharedObjectHintTable)] = {
    val sharedIndexByNumber = sharedEntries.map(_.index.number).zipWithIndex.toMap
    val defaultSharedIds    = sharedEntries.take(MaxSharedIdsPerPage).zipWithIndex.map(_._2).toList

    val pageEntriesEither = pageObjectNumbers.foldLeft[Either[String, List[PageEntry]]](Right(Nil)) {
      case (Left(error), _) => Left(error)
      case (Right(acc), pageNumber) =>
        measured.entry(pageNumber) match {
          case None =>
            graph.flatMap(_.pageNumbers.find(_ == pageNumber)) match {
              case Some(_) =>
                val contentEntry = measured.entries.headOption.getOrElse(
                  PartLayout.Entry(Obj.Index(pageNumber, 0), measured.headerSize, 0L)
                )
                Right(
                  acc :+ PageEntry(
                    objectCountDelta = math.max(1, firstPageObjectNumbers.size).toLong,
                    pageLengthDelta = 0L,
                    sharedObjectCount = 0,
                    sharedObjectIds = Nil,
                    sharedNumerators = Nil,
                    contentStreamOffset = contentEntry.offset,
                    contentStreamLength = contentEntry.size
                  )
                )
              case None =>
                Left(s"missing page object $pageNumber in measured layout")
            }
          case Some(pageEntry) =>
            val contentEntry = measured.entries.find(_.index.number != pageNumber).getOrElse(pageEntry)
            val pageSharedIds =
              graph.flatMap(_.pageSharedTopLevel.get(pageNumber)) match {
                case Some(numbers) =>
                  numbers.flatMap(sharedIndexByNumber.get).distinct.take(MaxSharedIdsPerPage).map(_.toInt)
                case None =>
                  if pageNumber == pageObjectNumbers.head then Nil else defaultSharedIds
              }
            Right(
              acc :+ PageEntry(
                objectCountDelta = math.max(1, firstPageObjectNumbers.size).toLong,
                pageLengthDelta = pageEntry.size,
                sharedObjectCount = pageSharedIds.size,
                sharedObjectIds = pageSharedIds,
                sharedNumerators = pageSharedIds.map(_ => 1),
                contentStreamOffset = contentEntry.offset,
                contentStreamLength = contentEntry.size
              )
            )
        }
    }

    pageEntriesEither.map { pageEntries =>
      val leastObjectCount      = pageEntries.map(_.objectCountDelta).min.toInt
      val leastPageLength       = pageEntries.map(_.pageLengthDelta).min
      val maxPageLengthDelta    = pageEntries.map(e => e.pageLengthDelta - leastPageLength).max
      val leastContentOffset    = pageEntries.map(_.contentStreamOffset).min
      val maxContentOffsetDelta = pageEntries.map(e => e.contentStreamOffset - leastContentOffset).max
      val leastContentLength    = pageEntries.map(_.contentStreamLength).min
      val maxContentLengthDelta = pageEntries.map(e => e.contentStreamLength - leastContentLength).max
      val maxSharedId           = pageEntries.flatMap(_.sharedObjectIds).foldLeft(0)(math.max)

      val pageHeader = PageOffsetHeader(
        leastObjectCount = leastObjectCount,
        locationOfFirstPage = measured.entry(pageObjectNumbers.head).map(_.offset).getOrElse(measured.headerSize),
        bitsObjectCountDelta = bitsNeeded(pageEntries.map(_.objectCountDelta - leastObjectCount).max),
        leastPageLength = leastPageLength,
        bitsPageLengthDelta = bitsNeeded(maxPageLengthDelta),
        leastContentOffset = leastContentOffset,
        bitsContentOffsetDelta = bitsNeeded(maxContentOffsetDelta),
        leastContentLength = leastContentLength,
        bitsContentLengthDelta = bitsNeeded(maxContentLengthDelta),
        bitsSharedObjectCount = bitsNeeded(pageEntries.map(_.sharedObjectCount.toLong).max),
        bitsSharedObjectId = bitsNeeded(maxSharedId.toLong),
        bitsNumerator = 1,
        denominator = 1
      )

      val normalizedPageEntries = pageEntries.map { entry =>
        entry.copy(
          objectCountDelta = entry.objectCountDelta - leastObjectCount,
          pageLengthDelta = entry.pageLengthDelta - leastPageLength,
          contentStreamOffset = entry.contentStreamOffset - leastContentOffset,
          contentStreamLength = entry.contentStreamLength - leastContentLength
        )
      }

      val leastSharedLength    = if sharedEntries.isEmpty then 0L else sharedEntries.map(_.size).min
      val maxSharedLengthDelta =
        if sharedEntries.isEmpty then 0L else sharedEntries.map(e => e.size - leastSharedLength).max
      val sharedHeader = SharedObjectHeader(
        firstObjectNumber = sharedEntries.headOption.map(_.index.number).getOrElse(0L),
        location = sharedEntries.headOption.map(_.offset).getOrElse(0L),
        firstPageEntries = sharedEntries.size,
        sectionEntries = sharedEntries.size,
        bitsGroupObjectCount = bitsNeeded(math.max(0, sharedEntries.size - 1).toLong),
        leastLength = leastSharedLength,
        bitsLengthDelta = bitsNeeded(maxSharedLengthDelta)
      )
      val sharedObjects = sharedEntries.map(entry => SharedEntry(entry.size - leastSharedLength, false, 0))
      (
        PageOffsetHintTable(pageHeader, normalizedPageEntries),
        SharedObjectHintTable(sharedHeader, List(SharedGroup(math.max(1, sharedEntries.size))), sharedObjects)
      )
    }
  }

  private def encodePageOffsetHeader(builder: HintStreamBuilder, table: PageOffsetHintTable): Unit = {
    val h = table.header
    builder.writeBits(h.leastObjectCount.toLong, 32)
    builder.writeBits(h.locationOfFirstPage, 32)
    builder.writeBits(h.bitsObjectCountDelta.toLong, 16)
    builder.writeBits(h.leastPageLength, 32)
    builder.writeBits(h.bitsPageLengthDelta.toLong, 16)
    builder.writeBits(h.leastContentOffset, 32)
    builder.writeBits(h.bitsContentOffsetDelta.toLong, 16)
    builder.writeBits(h.leastContentLength, 32)
    builder.writeBits(h.bitsContentLengthDelta.toLong, 16)
    builder.writeBits(h.bitsSharedObjectCount.toLong, 16)
    builder.writeBits(h.bitsSharedObjectId.toLong, 16)
    builder.writeBits(h.bitsNumerator.toLong, 16)
    builder.writeBits(h.denominator.toLong, 16)
  }

  private def encodePageOffsetEntries(builder: HintStreamBuilder, table: PageOffsetHintTable): Unit = {
    val h = table.header
    table.entries.foreach(entry => builder.writeBits(entry.objectCountDelta, h.bitsObjectCountDelta))
    builder.alignToByte()
    table.entries.foreach(entry => builder.writeBits(entry.pageLengthDelta, h.bitsPageLengthDelta))
    builder.alignToByte()
    table.entries.foreach(entry => builder.writeBits(entry.sharedObjectCount.toLong, h.bitsSharedObjectCount))
    builder.alignToByte()
    table.entries.foreach { entry =>
      entry.sharedObjectIds.take(entry.sharedObjectCount).foreach(id => builder.writeBits(id.toLong, h.bitsSharedObjectId))
    }
    builder.alignToByte()
    table.entries.foreach { entry =>
      entry.sharedNumerators.take(entry.sharedObjectCount).foreach(num => builder.writeBits(num.toLong, h.bitsNumerator))
    }
    builder.alignToByte()
    table.entries.foreach(entry => builder.writeBits(entry.contentStreamOffset, h.bitsContentOffsetDelta))
    builder.alignToByte()
    table.entries.foreach(entry => builder.writeBits(entry.contentStreamLength, h.bitsContentLengthDelta))
    builder.alignToByte()
  }

  private def encodeSharedObjectHeader(builder: HintStreamBuilder, table: SharedObjectHintTable): Unit = {
    val h = table.header
    builder.writeBits(h.firstObjectNumber, 32)
    builder.writeBits(h.location, 32)
    builder.writeBits(h.firstPageEntries.toLong, 32)
    builder.writeBits(h.sectionEntries.toLong, 32)
    builder.writeBits(h.bitsGroupObjectCount.toLong, 16)
    builder.writeBits(h.leastLength, 32)
    builder.writeBits(h.bitsLengthDelta.toLong, 16)
  }

  private def encodeSharedObjectGroups(builder: HintStreamBuilder, table: SharedObjectHintTable): Unit =
    table.groups.foreach(group => builder.writeBits(group.objectCount.toLong, table.header.bitsGroupObjectCount))

  private def encodeSharedObjectEntries(builder: HintStreamBuilder, table: SharedObjectHintTable): Unit = {
    val h = table.header
    table.objects.foreach(entry => builder.writeBits(entry.lengthMinusLeast, h.bitsLengthDelta))
    builder.alignToByte()
    table.objects.foreach(entry => builder.writeBits(if entry.signaturePresent then 1L else 0L, 1))
    builder.alignToByte()
    table.objects.foreach(entry => builder.writeBits(entry.nobjectsMinusOne.toLong, h.bitsGroupObjectCount))
    builder.alignToByte()
  }

  private def bitsNeeded(maxValue: Long): Int =
    if maxValue <= 0L then 1 else (math.floor(math.log(maxValue.toDouble) / math.log(2)).toInt + 1)
}
