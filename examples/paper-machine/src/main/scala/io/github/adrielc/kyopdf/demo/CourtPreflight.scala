package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.{CourtProfile, CourtRules}
import CourtRules.{Metric, Observation, Scope}
import java.awt.geom.Rectangle2D
import org.apache.pdfbox.Loader
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.pdmodel.interactive.documentnavigation.outline.PDOutlineItem
import org.apache.pdfbox.text.PDFTextStripperByArea
import scala.collection.mutable
import scala.util.{Try, Using}

private[demo] object CourtPreflight:
  private val profile = CourtProfile.CaliforniaSecondDistrictPetition
  private val MaxPages = 64
  private val MaxBookmarks = 256
  private val Number = """([1-9][0-9]{0,5})""".r
  private val Decorated = """(?i)page\s+([1-9][0-9]{0,5})(?:\s+of\s+[1-9][0-9]{0,5})?""".r
  private val Exhibit = """(?i)^exhibit\s+([a-z0-9]+)\b.*""".r

  private lazy val defaultPlan = CourtRules.compile(profile.rules).fold(
    issues => throw IllegalStateException(issues.mkString(", ")), identity)

  def inspect(bytes: Array[Byte]): String = inspect(bytes, defaultPlan)

  def inspect(bytes: Array[Byte], plan: CourtRules.Checked): String =
    val measured = Try(Using.resource(Loader.loadPDF(bytes))(inspectDocument)).getOrElse(
      Vector(Observation(Scope.Document, Map.empty, notes = Vector("The PDF could not be inspected."))))
    val observations = measured.map { observation =>
      if observation.scope == Scope.Document then observation.copy(values = observation.values.updated(Metric.FileBytes, bytes.length.toLong))
      else observation
    }
    val findings = plan.run(observations)
    val status = if findings.exists(_.status == "fix-required") then "fix-required"
      else if findings.exists(_.status == "needs-review") then "needs-review" else "pass"
    CourtRules.encodeReport(CourtRules.Report("court-preflight", plan.profile, PdfValidation.sha256(bytes),
      PdfValidation.sha256(CourtRules.encode(plan.profile).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
      bytes.length.toLong, status, findings))

  private def inspectDocument(pdf: PDDocument): Vector[Observation] =
    val count = pdf.getNumberOfPages
    if count < 1 || count > MaxPages then
      return Vector(Observation(Scope.Document, Map(Metric.PageCount -> count.toLong),
        notes = Vector(s"This preflight inspects PDFs of up to $MaxPages pages.")))

    val observations = Vector.newBuilder[Observation]
    val exhibitPages = mutable.Map.empty[String, Vector[Int]].withDefaultValue(Vector.empty)
    val pageLabels = Option(pdf.getDocumentCatalog.getPageLabels).map(_.getLabelsByPageIndices)
    for index <- 0 until count do
      val number = index + 1
      val page = pdf.getPage(index)
      val box = page.getCropBox
      val sideways = Math.floorMod(page.getRotation, 180) != 0
      val width = (if sideways then box.getHeight else box.getWidth).toDouble
      val height = (if sideways then box.getWidth else box.getHeight).toDouble
      val margin = height * 0.12
      val stripper = new PDFTextStripperByArea()
      stripper.setSortByPosition(true)
      stripper.addRegion("whole", new Rectangle2D.Double(0, 0, width, height))
      stripper.addRegion("top", new Rectangle2D.Double(0, 0, width, margin))
      stripper.addRegion("bottom", new Rectangle2D.Double(0, height - margin, width, margin))
      stripper.extractRegions(page)
      val text = stripper.getTextForRegion("whole")
      text.linesIterator.map(_.trim).find(_.nonEmpty).foreach {
        case Exhibit(label) =>
          val key = label.toUpperCase(java.util.Locale.ROOT)
          exhibitPages(key) = exhibitPages(key) :+ number
        case _ => ()
      }
      val labels = Vector("top", "bottom").flatMap(region =>
        stripper.getTextForRegion(region).linesIterator.map(_.trim).collect {
          case Number(value) => value -> false
          case Decorated(value) => value -> true
        })
      val pdfLabelMatches = pageLabels.flatMap(_.lift(index)).forall(_ == number.toString)
      val visibleMatches = labels match
        case Vector((value, false)) => Map(Metric.VisibleNumberMatches -> (if value == number.toString then 1L else 0L))
        case _ => Map.empty[Metric, Long]
      val values = Map(Metric.PageNumber -> number.toLong,
        Metric.TextCharacters -> text.count(c => !c.isWhitespace).toLong,
        Metric.PageLabelMatches -> (if pdfLabelMatches then 1L else 0L)) ++ visibleMatches
      val notes = labels.map((value, _) => s"Margin number $value on PDF page $number.")
      observations += Observation(Scope.Page, values, Some(number), notes = notes)

    val (bookmarkEvidence, bookmarkCount) = bookmarks(pdf, exhibitPages.toMap)
    observations += Observation(Scope.Document,
      Map(Metric.PageCount -> count.toLong) ++ bookmarkCount.map(n => Metric.BookmarkCount -> n.toLong))
    observations ++= bookmarkEvidence
    observations.result()

  private def bookmarks(pdf: PDDocument, exhibitPages: Map[String, Vector[Int]]): (Vector[Observation], Option[Int]) =
    val pending = mutable.ArrayDeque.empty[PDOutlineItem]
    Option(pdf.getDocumentCatalog.getDocumentOutline).flatMap(outline => Option(outline.getFirstChild)).foreach(pending.append)
    if pending.isEmpty then
      return (Vector.empty, Some(0))
    val seen = new java.util.IdentityHashMap[org.apache.pdfbox.cos.COSDictionary, java.lang.Boolean]()
    val observations = Vector.newBuilder[Observation]
    while pending.nonEmpty && seen.size < MaxBookmarks do
      val item = pending.removeHead()
      if seen.put(item.getCOSObject, java.lang.Boolean.TRUE) == null then
        Option(item.getFirstChild).foreach(pending.append)
        Option(item.getNextSibling).foreach(pending.append)
        val title = Option(item.getTitle).map(_.trim.take(120)).filter(_.nonEmpty).getOrElse("(untitled)")
        val page = Try(Option(item.findDestinationPage(pdf)).map(p => pdf.getPages.indexOf(p) + 1).filter(_ > 0)).toOption.flatten
        val expected = title match
          case Exhibit(label) => exhibitPages.get(label.toUpperCase(java.util.Locale.ROOT)).collect { case Vector(n) => n }
          case _ => None
        val resolves = Map(Metric.DestinationResolves -> (if page.isDefined then 1L else 0L))
        val matches = for
          actual <- page
          target <- expected
        yield Metric.ExhibitDestinationMatches -> (if actual == target then 1L else 0L)
        val note = for
          actual <- page
          target <- expected
        yield s"Bookmark $title opens page $actual. A matching exhibit heading was found on page $target."
        observations += Observation(Scope.Bookmark, resolves ++ matches, page, s"Bookmark $title.", note.toVector)
      else observations += Observation(Scope.Bookmark, Map.empty, notes = Vector("Repeated bookmark reference."))
    val complete = pending.isEmpty
    if !complete then observations += Observation(Scope.Bookmark, Map.empty, notes = Vector(s"Inspection stopped after $MaxBookmarks bookmarks."))
    (observations.result(), Option.when(complete)(seen.size))
