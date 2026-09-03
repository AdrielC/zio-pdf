/*
 * Semantic merge of multiple PDF filings into one page-ordered document.
 *
 * Each source is decoded to objects, pages are collected in order, and every
 * page's dependency closure is renumbered into a single fresh catalog/pages tree.
 */

package zio.pdf

import zio.*
import zio.stream.ZStream

object PdfMerge {

  sealed abstract class Error(message: String) extends Exception(message)

  final case class NoPages(sourceIndex: Int) extends Error(s"source $sourceIndex has no pages")

  /**
   * Merge caller-owned PDF bytes. Each source is decoded under
   * [[PdfEngine.Options.maxMaterializedDocumentBytes]] and rejected if encrypted.
   */
  def fromBytes(
    sources: NonEmptyChunk[Chunk[Byte]],
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] = {
    val total = sources.foldLeft(0L)((sum, source) => sum + source.size.toLong)
    if total > opts.maxMaterializedDocumentBytes.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, total))
    else
      ZIO
        .foreach(sources) { source =>
          PdfEngine.decode(source, opts).tap { decoded =>
            ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded))
          }
        }
        .flatMap { decoded =>
          decoded.toList match {
            case head :: tail => bytes(NonEmptyChunk(head, tail*))
            case Nil          => ZIO.fail(new IllegalArgumentException("merge requires at least one source"))
          }
        }
  }

  /** Merge decoded documents; pages appear in source order. */
  def parts(sources: NonEmptyChunk[Chunk[Decoded]]): Either[Error, Chunk[Part[Trailer]]] =
    mergeSources(sources).map { case (objects, trailer) =>
      Chunk.fromIterable(objects.map(Part.Obj(_))) :+ Part.Meta(trailer)
    }

  /** Encode merged filings to PDF bytes. */
  def bytes(sources: NonEmptyChunk[Chunk[Decoded]]): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.fromEither(parts(sources)).flatMap { parts =>
      ZStream
        .fromChunk(parts)
        .via(WritePdf.parts)
        .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
    }

  private final case class MergeState(
    nextNumber: Long,
    mergedObjects: List[IndirectObj],
    pageRefs: List[Prim.Ref]
  )

  private def mergeSources(
    sources: NonEmptyChunk[Chunk[Decoded]]
  ): Either[Error, (List[IndirectObj], Trailer)] = {
    val catalogNumber = 1L
    val pagesNumber   = 2L

    sources.toList.zipWithIndex
      .foldLeft[Either[Error, MergeState]](Right(MergeState(3L, Nil, Nil))) {
        case (Left(error), _) =>
          Left(error)
        case (Right(state), (decoded, sourceIndex)) =>
          val objects   = objectMap(decoded)
          val pageOrder = TextExtract.orderedPageObjectNumbers(decoded)
          if pageOrder.isEmpty then Left(NoPages(sourceIndex))
          else
            val skip = structureRoots(objects)
            pageOrder.foldLeft[Either[Error, MergeState]](Right(state)) { (acc, pageNumber) =>
              acc.map { current =>
                val bundle = collectClosure(pageNumber, objects, skip)
                val (renumbered, pageRef, after) = renumberBundle(bundle, pageNumber, current.nextNumber)
                current.copy(
                  nextNumber = after,
                  mergedObjects = current.mergedObjects ++ renumbered,
                  pageRefs = current.pageRefs :+ pageRef
                )
              }
            }
      }
      .map { state =>
        val pages = IndirectObj.nostream(
          pagesNumber,
          Prim.dict(
            "Type"  -> Prim.Name("Pages"),
            "Kids"  -> Prim.Array(state.pageRefs*),
            "Count" -> Prim.Number(BigDecimal(state.pageRefs.size))
          )
        )
        val catalog = IndirectObj.nostream(
          catalogNumber,
          Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(pagesNumber, 0))
        )
        val withParents = state.mergedObjects.map(withPagesParent(pagesNumber, _))
        val trailer =
          Trailer(
            BigDecimal(state.nextNumber),
            Prim.dict("Root" -> Prim.Ref(catalogNumber, 0)),
            Some(Prim.Ref(catalogNumber, 0))
          )
        (catalog :: pages :: withParents, trailer)
      }
  }

  private def objectMap(decoded: Chunk[Decoded]): Map[Long, IndirectObj] =
    decoded.foldLeft(Map.empty[Long, IndirectObj]) {
      case (acc, Decoded.DataObj(obj)) =>
        acc.updated(obj.index.number, IndirectObj(obj, None))
      case (acc, Decoded.ContentObj(obj, raw, _)) =>
        acc.updated(obj.index.number, IndirectObj(obj, Some(raw)))
      case (acc, _) =>
        acc
    }

  private def structureRoots(objects: Map[Long, IndirectObj]): Set[Long] = {
    val catalogNumber = objects.collectFirst { case (number, obj) if isCatalog(obj) => number }
    val pagesNumber = catalogNumber.flatMap { catalog =>
      objects.get(catalog).flatMap(_.obj.data match {
        case dict: Prim.Dict =>
          dict.data.get("Pages").collect { case Prim.Ref(number, _) => number }
        case _ =>
          None
      })
    }
    Set(catalogNumber, pagesNumber).flatten
  }

  private def isCatalog(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Catalog", _) => true
      case _                      => false
    }

  private def isPage(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Page", _) => true
      case _                   => false
    }

  private def refsIn(prim: Prim): List[Prim.Ref] =
    prim match {
      case ref: Prim.Ref     => List(ref)
      case Prim.Dict(data)   => data.values.toList.flatMap(refsIn)
      case Prim.Array(values) => values.toList.flatMap(refsIn)
      case _                 => Nil
    }

  private def collectClosure(
    root: Long,
    objects: Map[Long, IndirectObj],
    skip: Set[Long]
  ): List[IndirectObj] = {
    val seen   = scala.collection.mutable.Set.empty[Long]
    val buffer = scala.collection.mutable.LinkedHashMap.empty[Long, IndirectObj]

    def visit(number: Long): Unit =
      if skip(number) || seen(number) then ()
      else
        objects.get(number).foreach { obj =>
          seen += number
          buffer(number) = obj
          refsIn(obj.obj.data).foreach(ref => visit(ref.number))
        }

    visit(root)
    buffer.values.toList
  }

  private def renumberBundle(
    bundle: List[IndirectObj],
    pageNumber: Long,
    startAt: Long
  ): (List[IndirectObj], Prim.Ref, Long) = {
    val sorted  = bundle.sortBy(_.obj.index.number)
    val mapping = sorted.zipWithIndex.map { case (obj, index) => obj.obj.index.number -> (startAt + index) }.toMap
    val pageRefNumber = mapping(pageNumber)
    val remapped = sorted.map { obj =>
      val newNumber = mapping(obj.obj.index.number)
      val newData   = mapRefs(obj.obj.data, mapping)
      IndirectObj(Obj(Obj.Index(newNumber, 0), newData), obj.stream)
    }
    (remapped, Prim.Ref(pageRefNumber, 0), startAt + sorted.size)
  }

  private def withPagesParent(pagesNumber: Long, obj: IndirectObj): IndirectObj =
    if !isPage(obj) then obj
    else
      obj.obj.data match {
        case dict: Prim.Dict =>
          IndirectObj(obj.obj.copy(data = Prim.Dict(dict.data.updated("Parent", Prim.Ref(pagesNumber, 0)))), obj.stream)
        case _ =>
          obj
      }

  private def mapRefs(prim: Prim, mapping: Map[Long, Long]): Prim =
    prim match {
      case ref: Prim.Ref =>
        mapping.get(ref.number).fold(ref)(number => Prim.Ref(number, ref.generation))
      case Prim.Dict(data) =>
        Prim.Dict(data.map { case (key, value) => key -> mapRefs(value, mapping) })
      case Prim.Array(data) =>
        Prim.Array(data.map(mapRefs(_, mapping)))
      case other =>
        other
    }
}
