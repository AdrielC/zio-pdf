package zio.pdf.testkit

import java.io.ByteArrayOutputStream

import javax.imageio.ImageIO
import org.apache.pdfbox.Loader
import org.apache.pdfbox.pdmodel.{PDDocument, PDPage, PDPageContentStream}
import org.apache.pdfbox.pdmodel.common.PDRectangle
import org.apache.pdfbox.pdmodel.font.{PDType1Font, Standard14Fonts}
import org.apache.pdfbox.rendering.{ImageType, PDFRenderer}
import zio.*

object GeneratedPdf {

  val secondPageSentinel: String = "SECOND PAGE SHOULD NOT BE READ"

  def legalFiling: ZIO[Any, Throwable, Chunk[Byte]] =
    twoPage(
      firstPageLines = List(
        "UNITED STATES DISTRICT COURT",
        "SOUTHERN DISTRICT OF NEW YORK",
        "ACME LENDING LLC,",
        "Plaintiff,",
        "v.",
        "JANE DOE,",
        "Defendant."
      ),
      secondPageLines = List(secondPageSentinel)
    )

  def twoPage(firstPageLines: List[String], secondPageLines: List[String]): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.attemptBlocking {
      val doc = new PDDocument()
      try {
        addTextPage(doc, firstPageLines)
        addTextPage(doc, secondPageLines)
        val out = new ByteArrayOutputStream()
        doc.save(out)
        Chunk.fromArray(out.toByteArray)
      } finally doc.close()
    }

  def renderFirstPagePng(bytes: Chunk[Byte], dpi: Float = 36f): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.attemptBlocking {
      val doc = Loader.loadPDF(bytes.toArray)
      try {
        val image = new PDFRenderer(doc).renderImageWithDPI(0, dpi, ImageType.RGB)
        val out   = new ByteArrayOutputStream()
        ImageIO.write(image, "png", out)
        Chunk.fromArray(out.toByteArray)
      } finally doc.close()
    }

  private def addTextPage(doc: PDDocument, lines: List[String]): Unit = {
    val page = new PDPage(PDRectangle.LETTER)
    doc.addPage(page)
    val font = new PDType1Font(Standard14Fonts.FontName.HELVETICA)
    val cs   = new PDPageContentStream(doc, page)
    try {
      cs.beginText()
      cs.setFont(font, 12f)
      cs.newLineAtOffset(72f, 720f)
      lines.foreach { line =>
        cs.showText(line)
        cs.newLineAtOffset(0f, -16f)
      }
      cs.endText()
    } finally cs.close()
  }
}
