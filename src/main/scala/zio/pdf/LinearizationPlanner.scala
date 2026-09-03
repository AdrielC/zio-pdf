/*
 * Full ISO linearization planner: first-page layout, hint stream, and params.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.*
import zio.stream.ZStream

object LinearizationPlanner {

  final case class FullPlan(
    firstPageCount: Int,
    totalCount: Int,
    params: WriteLinearized.LinearizationParams,
    hintStream: Option[IndirectObj]
  )

  def plan(
    parts: Chunk[Part[Trailer]],
    graph: Option[PageDependencyGraph.Graph] = None,
    enableHints: Boolean = true
  ): Either[String, FullPlan] =
    for {
      measured           <- PartLayout.measure(parts)
      _                  <- measured.trailer.toRight("linearization requires Part.Meta trailer")
      objectParts = parts.collect {
                      case o: Part.Obj        => o
                      case s: Part.StreamObj  => s
                      case p: Part.Preencoded => p
                    }
      availableNumbers   = objectParts.flatMap(objectNumber).toSet
      filteredFirstPage  = graph.map(_.firstPageTopLevel.filter(availableNumbers.contains)).getOrElse(Nil)
      rawFirstPageCount  = if filteredFirstPage.nonEmpty then filteredFirstPage.size else computeFirstPageCount(objectParts)
      firstPageCount     = math.min(rawFirstPageCount, math.max(1, objectParts.size - 1))
      firstPageNumbers   =
        if filteredFirstPage.nonEmpty then filteredFirstPage.take(firstPageCount).toSet
        else objectParts.take(firstPageCount).flatMap(objectNumber).toSet
      pageNumbers       <- pageNumbersFrom(objectParts, graph, firstPageNumbers)
      maybeTables <-
        if !enableHints then Right(None)
        else LinearizationHints.fromMeasured(measured, pageNumbers, firstPageNumbers, graph)
      hintStream        <- maybeTables match {
                             case None =>
                               Right(None)
                             case Some((pageTable, sharedTable)) =>
                               LinearizationHints.encode(pageTable, sharedTable).map { hintBytes =>
                                 val hintNumber = measured.objectNumbers.maxOption.getOrElse(0L) + 1L
                                 Some(hintObject(hintNumber, hintBytes))
                               }
                           }
      hintBytes = hintStream.flatMap(obj => EncodedObj.indirect(obj).toOption.map(_.bytes))
      params = buildParams(
                 measured,
                 pageNumbers,
                 hintBytes.getOrElse(ByteVector.empty),
                 firstPageCount,
                 objectParts.size,
                 hintStream.isDefined,
                 enableHints
               )
    } yield FullPlan(
      firstPageCount = firstPageCount,
      totalCount = objectParts.size + 1,
      params = params,
      hintStream = hintStream
    )

  /** Linearize with Annex F hint stream generation and measured `/L`. */
  def bytes(
    trailerData: Prim.Dict,
    parts: Chunk[Part[Trailer]],
    graph: Option[PageDependencyGraph.Graph] = None,
    enableHints: Boolean = true
  ): ZIO[Any, Throwable, Chunk[Byte]] =
    for {
      initial <- ZIO.fromEither(plan(parts, graph, enableHints)).mapError(msg => new RuntimeException(msg))
      pass1 <- ZStream
                 .fromChunk(parts)
                 .via(
                   WriteLinearized.pipeWithHints(
                     trailerData,
                     initial.firstPageCount,
                     initial.totalCount,
                     initial.params,
                     initial.hintStream
                   )
                 )
                 .runFold(ByteVector.empty)(_ ++ _)
      finalPlan = initial.copy(params = initial.params.copy(fileSize = pass1.size))
      pass2 <- ZStream
                 .fromChunk(parts)
                 .via(
                   WriteLinearized.pipeWithHints(
                     trailerData,
                     finalPlan.firstPageCount,
                     finalPlan.totalCount,
                     finalPlan.params,
                     finalPlan.hintStream
                   )
                 )
                 .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
    } yield pass2

  private def pageNumbersFrom(
    objectParts: Chunk[Part[Trailer]],
    graph: Option[PageDependencyGraph.Graph],
    fallback: Set[Long]
  ): Either[String, List[Long]] =
    graph match {
      case Some(value) if value.pageNumbers.nonEmpty => Right(value.pageNumbers)
      case _                                         => pageNumbersFromParts(objectParts, fallback)
    }

  private def pageNumbersFromParts(
    objectParts: Chunk[Part[Trailer]],
    fallback: Set[Long]
  ): Either[String, List[Long]] = {
    val pages = objectParts.flatMap {
      case Part.Obj(obj) if isPage(obj)                  => Some(obj.obj.index.number)
      case Part.Preencoded(index, bytes) if isPageBytes(bytes) => Some(index.number)
      case _                                             => None
    }.toList
    if pages.nonEmpty then Right(pages) else Right(fallback.toList.sorted)
  }

  private def buildParams(
    measured: PartLayout.Measured,
    pageNumbers: List[Long],
    hintBytes: ByteVector,
    firstPageCount: Int,
    objectCount: Int,
    hasHintStream: Boolean,
    enableHints: Boolean
  ): WriteLinearized.LinearizationParams = {
    val headerSize     = measured.headerSize
    val linEstimate    = WriteLinearized.linearizationSize
    val hintBytesLen   = if enableHints && hasHintStream then hintBytes.size else 0L
    val hintOffset     = if enableHints && hasHintStream then headerSize + linEstimate else 0L
    val firstPageData  = estimateFirstPageSection(firstPageCount, measured)
    val firstPageXref  = estimateFirstPageXref(firstPageCount)
    val firstPageStart = headerSize + linEstimate + hintBytesLen + firstPageXref
    val firstPageEnd   = firstPageStart + firstPageData
    val hintOverhead   = if enableHints && hasHintStream then hintBytesLen + linEstimate else linEstimate
    val mainXrefOffset = headerSize + estimateBody(objectCount, hintOverhead, measured)
    val fileSize       = mainXrefOffset + estimateXref(objectCount + 1)
    WriteLinearized.LinearizationParams(
      fileSize = fileSize,
      firstPageObjNumber = pageNumbers.headOption.getOrElse(1L),
      hintStreamOffset = hintOffset,
      hintStreamLength = hintBytesLen,
      firstPageEndOffset = firstPageEnd,
      pageCount = math.max(1, pageNumbers.size),
      mainXrefOffset = mainXrefOffset
    )
  }

  private def estimateFirstPageXref(firstPageCount: Int): Long =
    math.max(64L, firstPageCount.toLong * 24L + 64L)

  private def estimateFirstPageSection(firstPageCount: Int, measured: PartLayout.Measured): Long =
    measured.entries.take(firstPageCount).foldLeft(0L)((sum, entry) => sum + entry.size)

  private def estimateBody(objectCount: Int, hintSize: Long, measured: PartLayout.Measured): Long =
    measured.totalBodySize + hintSize + objectCount * 64L

  private def estimateXref(objectCount: Int): Long =
    objectCount.toLong * 20L + 256L

  private def hintObject(number: Long, hint: LinearizationHints.HintStreamBytes): IndirectObj =
    IndirectObj.stream(
      number,
      Prim.dict(
        "Length" -> Prim.Number(hint.compressed.toByteArray.length),
        "Filter" -> Prim.Name("FlateDecode"),
        "S"      -> Prim.Number(hint.sharedSectionOffset)
      ),
      hint.compressed
    )

  private def computeFirstPageCount(parts: Chunk[Part[Trailer]]): Int = {
    var count    = 0
    var seenPage = false
    parts.foreach {
      case Part.Obj(obj) =>
        if !seenPage then
          count += 1
          if isPage(obj) then seenPage = true
      case Part.Preencoded(_, bytes) =>
        if !seenPage then
          count += 1
          if isPageBytes(bytes) then seenPage = true
      case _: Part.StreamObj =>
        if !seenPage then count += 1
      case _ => ()
    }
    math.max(1, if seenPage then count else math.min(4, parts.size))
  }

  private def objectNumber(part: Part[Trailer]): Option[Long] =
    part match {
      case Part.Obj(obj)                  => Some(obj.obj.index.number)
      case Part.Preencoded(index, _)      => Some(index.number)
      case Part.StreamObj(index, _, _, _) => Some(index.number)
      case _                              => None
    }

  private def isPage(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Page", _) => true
      case _                   => false
    }

  private def isPageBytes(bytes: ByteVector): Boolean = {
    val sample = new String(bytes.toArray.take(4096), java.nio.charset.StandardCharsets.ISO_8859_1)
    sample.contains("/Type") && sample.contains("/Page") && !sample.contains("/Pages")
  }
}
