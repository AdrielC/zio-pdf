package zio.pdf.page

import zio.pdf.*

private[page] object PageSelection {

  final case class Acc(
    pages: Map[Long, Page],
    pageTrees: Map[Long, Pages],
    contents: Map[Long, Element.Content]
  )

  object Acc {
    val empty: Acc = Acc(Map.empty, Map.empty, Map.empty)
  }

  def add(acc: Acc, element: Element): Acc =
    element match {
      case Element.Data(_, Element.DataKind.Page(page)) =>
        acc.copy(pages = acc.pages.updated(page.index.number, page))
      case Element.Data(_, Element.DataKind.Pages(pages)) =>
        acc.copy(pageTrees = acc.pageTrees.updated(pages.index.number, pages))
      case content @ Element.Content(obj, _, _, _) =>
        acc.copy(contents = acc.contents.updated(obj.index.number, content))
      case _ =>
        acc
    }

  def firstPage(acc: Acc): Option[Page] = {
    val fromRoot =
      acc.pageTrees.values.find(_.root).flatMap(firstPageFromTree(acc, _))
    fromRoot.orElse(acc.pages.values.toList.sortBy(_.index.number).headOption)
  }

  def contentRefs(page: Page): List[Long] =
    page.data("Contents") match {
      case Some(Prim.Ref(number, _)) => List(number)
      case Some(Prim.Array(values))  => values.collect { case Prim.Ref(number, _) => number }.toList
      case _                         => Nil
    }

  private def firstPageFromTree(acc: Acc, tree: Pages): Option[Page] =
    tree.kids.iterator
      .flatMap { case Prim.Ref(number, _) =>
        acc.pages.get(number).orElse(acc.pageTrees.get(number).flatMap(firstPageFromTree(acc, _)))
      }
      .toSeq
      .headOption
}
