package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.{PdfArrow, PdfPipeline}
import PdfArrow.*
import volga.{Iso, MonoidalObjects, SymmetricCat}
import volga.syntax.smc
import volga.tags

/** The actual Volga category and SMC macro over typed, reusable PDF arrows. */
object PipelineWiring:
  type Objects[T] = T match
    case tags.Obj[a] => Ports[a]
    case tags.One => Unit
    case tags.Tensor[a, b] => (a, b)

  given objects: MonoidalObjects[Objects] with
    given unitOb: Ports[Unit] = Ports.unit
    given tensorOb[A: Ports, B: Ports]: Ports[(A, B)] = Ports.pair[A, B]

  given category: SymmetricCat[PdfArrow, Objects] with
    def identity[A: Ports]: PdfArrow[A, A] = PdfArrow.id[A]
    def compose[A: Ports, B: Ports, C: Ports](f: PdfArrow[B, C], g: PdfArrow[A, B]): PdfArrow[A, C] = g >>> f
    def tensor[A: Ports, B: Ports, C: Ports, D: Ports](f: PdfArrow[A, B], g: PdfArrow[C, D]): PdfArrow[(A, C), (B, D)] = f >< g
    def assocLeft[A: Ports, B: Ports, C: Ports]: PdfArrow[(A, (B, C)), ((A, B), C)] = PdfArrow.assocLeft[A, B, C]
    def braiding[A: Ports, B: Ports]: PdfArrow[(A, B), (B, A)] = PdfArrow.swap[A, B]
    def leftUnit[A: Ports]: Iso[PdfArrow, Objects, (Unit, A), A] = Iso(PdfArrow.leftUnit[A], PdfArrow.spawnLeft[A])

  val syntax = smc.syntax[PdfArrow, Objects, Tuple2]

  def recipe[B](name: String, arrow: PdfArrow[Unit, B]): Either[Vector[PdfPipeline.Issue], PdfPipeline.Recipe] = PdfArrow.recipe(name, arrow)
  private def checked[B](name: String, arrow: PdfArrow[Unit, B]): PdfPipeline.Recipe =
    recipe(name, arrow).fold(errors => throw IllegalStateException(errors.mkString(", ")), identity)

  def example: PdfPipeline.Recipe =
    val read = input("body")
    val cut = select(4, 1, 3)
    val mark = stamp("DRAFT")
    val numbers = number(1, "bates", "DOC-")
    val split = fork[File]
    val checks = validate("3b", "2b", "3u")
    val graph = syntax.of0:
      val body = read()
      val selected = cut(body)
      val marked = mark(selected)
      val numbered = numbers(marked)
      val bytes = write(numbered)
      val (download, check) = split(bytes)
      val receipt = checks(check)
      (download, receipt)
    checked("Marked packet", graph)

  def conditionalArrow: PdfArrow[Document, Document] =
    choose(pages(1, 3))(
      stamp("SHORT PACKET").named("short"),
      stamp("LONG PACKET").named("long"),
      stop[Document, Document]("The document page count could not be determined").named("unknown")
    ).named("route")

  def conditionalExample: PdfPipeline.Recipe =
    val read = input("body")
    val route = conditionalArrow
    val split = fork[File]
    val checks = validate("3b")
    val graph = syntax.of0:
      val body = read()
      val selected = route(body)
      val bytes = write(selected)
      val (download, check) = split(bytes)
      val receipt = checks(check)
      (download, receipt)
    checked("Route by page count", graph)
