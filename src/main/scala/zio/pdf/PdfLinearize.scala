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
      topLevel     = raw.objects.map(_.index.number).toSet
      graph        = PageDependencyGraph.fromDecoded(decoded, topLevel)
      prefixNumbers = graph.firstPageTopLevel.filter(topLevel.contains)
      reordered    = PageDependencyGraph.reorder(raw.objects, prefixNumbers)
      parts       <- partsFromRaw(raw, reordered, decoded)
      trailerData <- ZIO.fromEither(trailerDataFrom(parts).toRight(new RuntimeException("missing trailer")))
      output      <- LinearizationPlanner.bytes(trailerData, parts, Some(graph))
    } yield output

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

  /** First-page byte span in a linearized file (header through first-page section). */
  def firstPageByteLength(bytes: Chunk[Byte]): Either[String, Long] =
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

  private def trailerDataFrom(parts: Chunk[Part[Trailer]]): Option[Prim.Dict] =
    parts.collectFirst { case Part.Meta(trailer) => trailer.data }

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
