/*
 * Per-page indirect-object dependency analysis for linearization hints and layout.
 */

package zio.pdf

import zio.Chunk

object PageDependencyGraph {

  final case class Graph(
    pageNumbers: List[Long],
    topLevelNumbers: Set[Long],
    firstPageTopLevel: List[Long],
    pageSharedTopLevel: Map[Long, List[Long]]
  )

  /** Build a page dependency graph from a decoded timeline and top-level object numbers. */
  def fromDecoded(decoded: Chunk[Decoded], topLevelNumbers: Set[Long]): Graph = {
    val objects         = objectMap(decoded)
    val objStmCarriers  = objectStreamCarriers(decoded)
    val pageNumbers     = TextExtract.orderedPageObjectNumbers(decoded)
    val catalogNumber   = objects.collectFirst { case (number, obj) if isCatalog(obj) => number }
    val pagesRoot       = catalogNumber.flatMap(catalogPagesNumber(objects, _))
    val structureSkip   = Set(catalogNumber, pagesRoot).flatten

    def toTopLevel(number: Long): Option[Long] =
      if topLevelNumbers.contains(number) then Some(number)
      else objStmCarriers.get(number).filter(topLevelNumbers.contains)

    def topLevelClosure(root: Long, skip: Set[Long]): List[Long] = {
      val logical = collectClosure(root, objects, skip)
      logical.flatMap(toTopLevel).distinct
    }

    val pagesPath   = pageNumbers.headOption.toList.flatMap(first => pagesTreePath(first, objects))
    val firstPage   = pageNumbers.headOption.getOrElse(0L)
    val firstPrefix = dedupePreserveOrder(
      catalogNumber.toList ++ pagesPath ++ topLevelClosure(firstPage, structureSkip)
    ).filter(topLevelNumbers)

    val firstSet = firstPrefix.toSet

    val pageShared = pageNumbers.map { pageNumber =>
      val needed = if pageNumber == firstPage then Nil
      else
        topLevelClosure(pageNumber, structureSkip)
          .filterNot(firstSet)
          .distinct
          .take(LinearizationHints.MaxSharedIdsPerPage)
      pageNumber -> needed
    }.toMap

    Graph(
      pageNumbers = pageNumbers,
      topLevelNumbers = topLevelNumbers,
      firstPageTopLevel = firstPrefix,
      pageSharedTopLevel = pageShared
    )
  }

  /** Reorder preencoded top-level objects with the first-page prefix first. */
  def reorder(objects: Chunk[Part.Preencoded], firstPageTopLevel: List[Long]): Chunk[Part.Preencoded] = {
    val byNumber  = objects.map(part => part.index.number -> part).toMap
    val prefixSet = firstPageTopLevel.toSet
    val prefix    = Chunk.fromIterable(firstPageTopLevel.flatMap(byNumber.get))
    val rest      = objects.filterNot(part => prefixSet(part.index.number))
    prefix ++ rest
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

  private def objectStreamCarriers(decoded: Chunk[Decoded]): Map[Long, Long] =
    decoded.foldLeft(Map.empty[Long, Long]) {
      case (acc, Decoded.ContentObj(obj, rawStream, stream)) =>
        obj.data match {
          case dict: Prim.Dict if dict.data.get("Type").contains(Prim.Name("ObjStm")) =>
            Content.extractObjectStream(stream)(dict) match {
              case Some(_root_.scodec.Attempt.Successful(objStream)) =>
                objStream.objs.foldLeft(acc) { (inner, embedded) =>
                  inner.updated(embedded.index.number, obj.index.number)
                }
              case _ =>
                acc
            }
          case _ =>
            acc
        }
      case (acc, _) =>
        acc
    }

  private def isCatalog(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Catalog", _) => true
      case _                      => false
    }

  private def catalogPagesNumber(objects: Map[Long, IndirectObj], catalogNumber: Long): Option[Long] =
    objects.get(catalogNumber).flatMap(_.obj.data match {
      case dict: Prim.Dict =>
        dict.data.get("Pages").collect { case Prim.Ref(number, _) => number }
      case _ =>
        None
    })

  private def pagesTreePath(pageNumber: Long, objects: Map[Long, IndirectObj]): List[Long] = {
    val path = scala.collection.mutable.ListBuffer.empty[Long]
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

  private def refsIn(prim: Prim): List[Prim.Ref] =
    prim match {
      case ref: Prim.Ref        => List(ref)
      case Prim.Dict(data)      => data.values.toList.flatMap(refsIn)
      case Prim.Array(values)   => values.toList.flatMap(refsIn)
      case _                    => Nil
    }

  private def collectClosure(
    root: Long,
    objects: Map[Long, IndirectObj],
    skip: Set[Long]
  ): List[Long] = {
    val seen   = scala.collection.mutable.Set.empty[Long]
    val buffer = scala.collection.mutable.ListBuffer.empty[Long]

    def visit(number: Long): Unit =
      if skip(number) || seen(number) then ()
      else
        objects.get(number).foreach { obj =>
          seen += number
          buffer += number
          refsIn(obj.obj.data).foreach(ref => visit(ref.number))
        }

    visit(root)
    buffer.toList
  }

  private def dedupePreserveOrder(values: List[Long]): List[Long] = {
    val seen = scala.collection.mutable.Set.empty[Long]
    values.filter { value =>
      if seen(value) then false
      else
        seen += value
        true
    }
  }
}
