/*
 * Fast-web linearized PDF writer with ISO hint stream generation.
 */

package zio.pdf

import zio.*
import zio.stream.ZStream

object PdfLinearize {

  final case class Layout(
    firstPageCount: Int,
    totalCount: Int,
    fileSize: Long
  )

  /** Count object parts and estimate how many belong in the first-page prefix. */
  def layout(parts: Chunk[Part[Trailer]]): Layout = {
    val objectParts = parts.collect {
      case o: Part.Obj        => o: Part[Trailer]
      case s: Part.StreamObj  => s: Part[Trailer]
      case p: Part.Preencoded => p: Part[Trailer]
    }
    val totalObjects = objectParts.size + 2
    val firstPageCount = {
      var count    = 0
      var seenPage = false
      objectParts.foreach {
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
      math.max(1, if seenPage then count else math.min(4, objectParts.size))
    }
    Layout(
      firstPageCount = firstPageCount,
      totalCount = totalObjects,
      fileSize = math.max(4096L, totalObjects.toLong * 512L)
    )
  }

  /**
   * Linearize from existing PDF bytes using verbatim [[Part.Preencoded]] objects
   * (no object-stream expansion or stream re-encoding).
   */
  def fromBytes(bytes: Chunk[Byte]): ZIO[Any, Throwable, Chunk[Byte]] =
    for {
      raw         <- ZIO.fromEither(PdfGraft.rawObjectParts(bytes.toArray).left.map(new RuntimeException(_)))
      decoded     <- ZStream.fromChunk(bytes).via(PdfStream.decode()).runCollect
      _           <- ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded))
      trailerData <- ZIO.fromEither(trailerDataFromDecoded(decoded).toRight(new RuntimeException("missing trailer")))
      rootNumber  <- ZIO.fromEither(rootObjectNumber(trailerData).toRight(new RuntimeException("missing /Root")))
      topLevel     = raw.objects.map(_.index.number).toSet
      structural  <- ZIO.fromEither(hybridMissing(decoded, rootNumber, topLevel).left.map(new RuntimeException(_)))
      referenced   = referencedMissing(decoded, topLevel, structural)
      missing      = (structural ++ referenced).distinct.filterNot(topLevel.contains)
      extra       <-
        if missing.isEmpty then ZIO.succeed(Chunk.empty[Part.Preencoded])
        else ZIO.fromEither(synthesizeMissing(decoded, missing).left.map(new RuntimeException(_)))
      merged       = mergePreencoded(raw.objects, extra)
      topLevel2    = merged.map(_.index.number).toSet
      graph        = PageDependencyGraph.fromDecoded(decoded, topLevel2)
      reordered    = PageDependencyGraph.reorder(merged, graph.firstPageTopLevel.filter(topLevel2.contains))
      parts       <- partsFromRaw(raw, reordered, decoded)
      partNumbers  = objectNumbers(parts)
      output      <- LinearizationPlanner.bytes(
                       trailerData,
                       parts,
                       Some(PageDependencyGraph.fromDecoded(decoded, partNumbers)),
                       enableHints = false
                     )
    } yield output

  private def hybridMissing(
    decoded: Chunk[Decoded],
    rootNumber: Long,
    topLevel: Set[Long]
  ): Either[String, List[Long]] = {
    val objects   = decodedObjectMap(decoded)
    val pagesRoot = objects.get(rootNumber).flatMap(pagesRootNumber).toList
    val firstPage = TextExtract.orderedPageObjectNumbers(decoded).headOption
    val pagesPath = firstPage.toList.flatMap(page => pagesTreePath(page, objects))
    val seeds     = (rootNumber :: pagesRoot ++ pagesPath ++ firstPage.toList).distinct
    Right(seeds.filterNot(topLevel.contains))
  }

  /** Follow page-tree refs from synthesized structural objects; content streams stay grafted. */
  private def referencedMissing(
    decoded: Chunk[Decoded],
    topLevel: Set[Long],
    structural: List[Long]
  ): List[Long] = {
    val objects = decodedObjectMap(decoded)
    val missing = scala.collection.mutable.ListBuffer.empty[Long]
    val visited = scala.collection.mutable.Set.empty[Long]
    val queue   = scala.collection.mutable.Queue.from(structural)

    while queue.nonEmpty do
      val number = queue.dequeue()
      if visited(number) then ()
      else
        visited += number
        objects.get(number).foreach { obj =>
          refsIn(obj.obj.data).foreach { ref =>
            val target = ref.number
            objects.get(target).foreach { child =>
              if isPageTreeNode(child) then
                if !topLevel.contains(target) && !missing.contains(target) then missing += target
                if !visited.contains(target) then queue.enqueue(target)
            }
          }
        }

    missing.toList.distinct
  }

  private def isPageTreeNode(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Page", _) | Prim.tpe("Pages", _) => true
      case _                                          => false
    }

  private def refsIn(prim: Prim): List[Prim.Ref] =
    prim match {
      case ref: Prim.Ref      => List(ref)
      case Prim.Dict(data)    => data.values.toList.flatMap(refsIn)
      case Prim.Array(values) => values.toList.flatMap(refsIn)
      case _                  => Nil
    }

  private def pagesRootNumber(obj: IndirectObj): Option[Long] =
    obj.obj.data match {
      case dict: Prim.Dict =>
        dict.data.get("Pages").collect { case Prim.Ref(number, _) => number }
      case _ =>
        None
    }

  private def pagesTreePath(pageNumber: Long, objects: Map[Long, IndirectObj]): List[Long] = {
    val path    = scala.collection.mutable.ListBuffer.empty[Long]
    var current = objects.get(pageNumber)
    while current.nonEmpty do
      current.foreach { obj =>
        obj.obj.data match {
          case dict: Prim.Dict =>
            dict.data.get("Parent").collect { case Prim.Ref(parent, _) =>
              if objects.get(parent).exists(isPagesNode) then path.prepend(parent)
              current = objects.get(parent)
            }.getOrElse { current = None }
          case _ =>
            current = None
        }
      }
    path.toList
  }

  private def isPagesNode(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Pages", _) => true
      case _                    => false
    }

  private def synthesizeMissing(
    decoded: Chunk[Decoded],
    numbers: List[Long]
  ): Either[String, Chunk[Part.Preencoded]] = {
    val objects = decodedObjectMap(decoded)
    numbers.foldLeft[Either[String, Chunk[Part.Preencoded]]](Right(Chunk.empty)) {
      case (Left(error), _) =>
        Left(error)
      case (Right(acc), number) =>
        objects.get(number) match {
          case None =>
            Left(s"missing decoded object $number for hybrid linearization")
          case Some(obj) =>
            EncodedObj.indirect(obj) match {
              case _root_.scodec.Attempt.Successful(EncodedObj(_, bytes)) =>
                Right(acc :+ Part.Preencoded(obj.obj.index, bytes))
              case _root_.scodec.Attempt.Failure(cause) =>
                Left(s"encode synthesized object $number: ${cause.messageWithContext}")
            }
        }
    }
  }

  private def mergePreencoded(
    primary: Chunk[Part.Preencoded],
    extra: Chunk[Part.Preencoded]
  ): Chunk[Part.Preencoded] = {
    val primaryNumbers = primary.map(_.index.number).toSet
    primary ++ extra.filterNot(part => primaryNumbers(part.index.number))
  }

  private def decodedObjectMap(decoded: Chunk[Decoded]): Map[Long, IndirectObj] =
    decoded.foldLeft(Map.empty[Long, IndirectObj]) {
      case (acc, Decoded.DataObj(obj)) =>
        acc.updated(obj.index.number, IndirectObj(obj, None))
      case (acc, Decoded.ContentObj(obj, rawStream, _)) =>
        acc.updated(obj.index.number, IndirectObj(obj, Some(rawStream)))
      case (acc, _) =>
        acc
    }

  private def objectNumbers(parts: Chunk[Part[Trailer]]): Set[Long] =
    parts.collect {
      case Part.Obj(obj)                  => obj.obj.index.number
      case Part.Preencoded(index, _)      => index.number
      case Part.StreamObj(index, _, _, _) => index.number
    }.toSet

  private def trailerDataFromDecoded(decoded: Chunk[Decoded]): Option[Prim.Dict] =
    decoded.collectFirst { case Decoded.Meta(_, Some(trailer), _) => trailer.data }

  private def rootObjectNumber(trailerData: Prim.Dict): Option[Long] =
    trailerData.data.get("Root").collect { case Prim.Ref(number, _) => number }

  /** Build a linearizable part stream that preserves donor object bytes. */
  def partsFromPdf(bytes: Chunk[Byte]): ZIO[Any, Throwable, Chunk[Part[Trailer]]] =
    for {
      raw     <- ZIO.fromEither(PdfGraft.rawObjectParts(bytes.toArray).left.map(new RuntimeException(_)))
      decoded <- ZStream.fromChunk(bytes).via(PdfStream.decode()).runCollect
      parts   <- partsFromRaw(raw, raw.objects, decoded)
    } yield parts

  /** Encode with hint streams, measured `/L`, and Annex F tables. */
  def bytes(trailerData: Prim.Dict, parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    LinearizationPlanner.bytes(trailerData, parts)

  /** Legacy layout writer without hint stream generation. */
  def encode(
    trailerData: Prim.Dict,
    parts: Chunk[Part[Trailer]],
    layout: Layout
  ): ZIO[Any, Throwable, Chunk[Byte]] =
    ZStream
      .fromChunk(parts)
      .via(WriteLinearized.pipe(trailerData, layout.firstPageCount, layout.totalCount, layout.fileSize))
      .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))

  /** First-page byte span in a linearized file (reads `/E` from the linearization dictionary). */
  def firstPageByteLength(bytes: Chunk[Byte]): Either[String, Long] =
    parseLinearizationEndOffset(bytes.toArray) match {
      case Some(value) => Right(value)
      case None =>
        PdfGraft.rawObjectParts(bytes.toArray).flatMap { raw =>
          val parts = raw.version.fold(Chunk.empty[Part[Trailer]])(v => Chunk.single(Part.Version(v))) ++ raw.objects
          PartLayout.measure(parts).map { measured =>
            val firstCount = layout(parts).firstPageCount
            val header     = measured.headerSize
            val lin        = WriteLinearized.linearizationSize
            val first      = measured.entries.take(firstCount).foldLeft(0L)(_ + _.size)
            header + lin + first
          }
        }
    }

  private def parseLinearizationEndOffset(bytes: Array[Byte]): Option[Long] =
    val sample = new String(bytes.take(math.min(bytes.length, 8192)), java.nio.charset.StandardCharsets.ISO_8859_1)
    val endPattern = "/E\\s+(\\d+)".r
    endPattern.findFirstMatchIn(sample).map(_.group(1).toLong)

  private def partsFromRaw(
    raw: PdfGraft.RawParts,
    objects: Chunk[Part.Preencoded],
    decoded: Chunk[Decoded]
  ): ZIO[Any, Throwable, Chunk[Part[Trailer]]] =
    ZIO.fromEither {
      val trailer = decoded.collectFirst { case Decoded.Meta(_, Some(t), _) => Part.Meta(t) }
      trailer.toRight(new RuntimeException("missing trailer in source PDF")).map { meta =>
        val versionPart = raw.version.fold(Chunk.empty[Part[Trailer]])(v => Chunk.single(Part.Version(v)))
        versionPart ++ objects ++ Chunk.single(meta)
      }
    }

  private def isPage(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Page", _) => true
      case _                   => false
    }

  private def isPageBytes(bytes: _root_.scodec.bits.ByteVector): Boolean = {
    val sample = new String(bytes.toArray.take(4096), java.nio.charset.StandardCharsets.ISO_8859_1)
    sample.contains("/Type") && sample.contains("/Page") && !sample.contains("/Pages")
  }
}
