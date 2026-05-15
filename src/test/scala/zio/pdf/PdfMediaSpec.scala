package zio.pdf

import zio.test.*

object PdfMediaSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("PdfMedia")(
    test("detects common file extensions with zio-blocks MediaType") {
      assertTrue(
        PdfMedia.fromExtension(".pdf").contains(PdfMedia.pdf),
        PdfMedia.fromFileName("filing.PDF").exists(_.matches(PdfMedia.pdf)),
        PdfMedia.fromFileName("first-page.png").contains(PdfMedia.png)
      )
    },
    test("parses content types and keeps parameters") {
      val parsed = PdfMedia.parseContentType("text/plain; charset=utf-8")
      assertTrue(
        parsed.exists(_.matches(PdfMedia.textPlain, ignoreParameters = true)),
        parsed.toOption.exists(_.parameters.get("charset").contains("utf-8"))
      )
    },
    test("decodes PDF-name hex escapes in embedded file subtypes") {
      val dict = Prim.dict("Subtype" -> Prim.Name("application#2Fpdf"))
      assertTrue(
        PdfMedia.fromEmbeddedFileSubtype(dict).contains(PdfMedia.pdf),
        PdfMedia.pdfNameHexDecode("image#2Fpng").contains("image/png"),
        PdfMedia.pdfNameHexDecode("bad#2").isEmpty
      )
    }
  )
}
