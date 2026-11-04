package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.PdfPipeline.Outcome
import java.awt.geom.Rectangle2D
import org.apache.pdfbox.Loader
import org.apache.pdfbox.text.PDFTextStripperByArea
import scala.util.{Try, Using}

/** Conservative evidence from extractable page text in the outer margins. */
private[demo] object PageNumberEvidence:
  private val Explicit = """(?i)page\s+([1-9][0-9]{0,5})(?:\s+(?:of|/)\s+[1-9][0-9]{0,5})?""".r
  private val Bare = """([1-9][0-9]{0,5})""".r

  def inspect(bytes: Array[Byte]): (Outcome, String) =
    Try(Using.resource(Loader.loadPDF(bytes)) { pdf =>
      val total = pdf.getNumberOfPages
      if total < 1 || total > 64 then (Outcome.Unknown, "Page count exceeds the inspection budget")
      else
        val pages = Vector.tabulate(total) { index =>
          val page = pdf.getPage(index)
          val box = page.getCropBox
          val width = box.getWidth.toDouble
          val height = box.getHeight.toDouble
          val band = height * 0.12
          val stripper = new PDFTextStripperByArea()
          stripper.addRegion("top", new Rectangle2D.Double(0, 0, width, band))
          stripper.addRegion("middle", new Rectangle2D.Double(0, band, width, height - 2 * band))
          stripper.addRegion("bottom", new Rectangle2D.Double(0, height - band, width, band))
          stripper.extractRegions(page)
          val margin = Vector("top", "bottom").flatMap(region => stripper.getTextForRegion(region).linesIterator.map(_.trim).filter(_.nonEmpty))
          val labels = margin.collect { case Explicit(n) => n.toInt -> true; case Bare(n) => n.toInt -> false }
          val readable = page.getRotation == 0 && stripper.getTextForRegion("middle").count(_.isLetter) >= 12
          val graphical = Option(page.getResources).exists(_.getXObjectNames.iterator().hasNext)
          val ambiguous = margin.exists(line => line.exists(_.isDigit) || line.matches("(?i)[ivxlcdm]+"))
          (labels, readable && !graphical && !ambiguous)
        }
        val labels = pages.map(_._1)
        val sequential = labels.forall(_.size == 1) &&
          (total > 1 || labels.head.head._2) &&
          labels.map(_.head._1).sliding(2).forall(pair => pair.size < 2 || pair(1) == pair(0) + 1)
        if sequential then (Outcome.Yes, s"Found consecutive margin page labels on $total pages")
        else if pages.forall(_._2) then (Outcome.No, s"No page labels found in the outer margins of $total text-readable pages")
        else (Outcome.Unknown, "Page labels are ambiguous or some pages have insufficient extractable text")
    }).getOrElse((Outcome.Unknown, "Could not inspect visible page labels"))
