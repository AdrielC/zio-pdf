package zio.pdf.page

import zio.*
import zio.pdf.*
import zio.pdf.testkit.GeneratedPdf
import zio.test.*

object FirstPageSpec extends ZIOSpecDefault {

  private final case class Parties(plaintiff: String, defendant: String)

  private def inferPartiesForTest(text: String): Option[Parties] = {
    val lines = text.linesIterator.map(_.trim.stripSuffix(",").stripSuffix(".")).filter(_.nonEmpty).toList
    val vAt   = lines.indexWhere(line => line.equalsIgnoreCase("v") || line.equalsIgnoreCase("v."))
    Option.when(vAt > 0 && vAt + 1 < lines.length) {
      val plaintiff = lines.take(vAt).reverse.find(!_.equalsIgnoreCase("plaintiff")).getOrElse("")
      val defendant = lines.drop(vAt + 1).find(!_.equalsIgnoreCase("defendant")).getOrElse("")
      Parties(plaintiff, defendant)
    }
  }

  private def hasPngHeader(bytes: Chunk[Byte]): Boolean = {
    val expected = Array[Byte](-119, 80, 78, 71, 13, 10, 26, 10)
    bytes.take(expected.length).toArray.toSeq == expected.toSeq
  }

  def spec: Spec[Any, Throwable] = suite("FirstPage")(
    test("extracts text only from the first page using the zio-pdf parser") {
      for {
        bytes <- GeneratedPdf.legalFiling
        page  <- FirstPage.fromBytes(bytes)
      } yield assertTrue(
        page.mediaType == PdfMedia.textPlain,
        page.contentObjectNumbers.nonEmpty,
        page.text.contains("ACME LENDING LLC"),
        page.text.contains("JANE DOE"),
        !page.text.contains(GeneratedPdf.secondPageSentinel),
        inferPartiesForTest(page.text).contains(Parties("ACME LENDING LLC", "JANE DOE"))
      )
    },
    test("builds a first-page-only PDF slice that can be rendered by a PDFBox oracle") {
      for {
        bytes <- GeneratedPdf.legalFiling
        slice <- FirstPage.sliceFromBytes(bytes)
        text  <- FirstPage.fromBytes(slice.bytes)
        png   <- GeneratedPdf.renderFirstPagePng(slice.bytes)
      } yield assertTrue(
        slice.mediaType == PdfMedia.pdf,
        new String(slice.bytes.take(5).toArray) == "%PDF-",
        text.text.contains("ACME LENDING LLC"),
        !text.text.contains(GeneratedPdf.secondPageSentinel),
        hasPngHeader(png)
      )
    } @@ TestAspect.timeout(30.seconds)
  )
}
