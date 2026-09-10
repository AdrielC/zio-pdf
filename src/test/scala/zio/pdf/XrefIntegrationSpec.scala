package zio.pdf

import org.apache.pdfbox.Loader
import zio.{Chunk, ZIO}
import zio.stream.ZStream
import zio.test.*

object XrefIntegrationSpec extends ZIOSpecDefault:
  /** A tiny public-domain synthetic PDF using the same /W layout as the reported signed PDF. */
  private def fixture: Array[Byte] =
    val header = "%PDF-1.7\n"
    val objects = List(
      "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
      "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
      "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>\nendobj\n"
    )
    val offsets = objects.scanLeft(header.length)((offset, obj) => offset + obj.length)
    val xrefOffset = offsets.last
    val payload = new Array[Byte](5 * 9)
    (5 until 9).foreach(i => payload(i) = 255.toByte)
    offsets.zipWithIndex.foreach { (offset, i) =>
      val start = (i + 1) * 9
      payload(start) = 1
      (0 until 4).foreach(b => payload(start + 1 + b) = (offset >>> ((3 - b) * 8)).toByte)
    }
    (header + objects.mkString + "4 0 obj\n<< /Type /XRef /Size 5 /Root 1 0 R /W [1 4 4] /Length 45 >>\nstream\n")
      .getBytes("US-ASCII") ++ payload ++
      s"\nendstream\nendobj\nstartxref\n$xrefOffset\n%%EOF\n".getBytes("US-ASCII")

  def spec: Spec[Any, Any] = suite("xref streams through public PDF APIs")(
    test("independent PDFBox reader and zio-pdf agree on the synthetic document") {
      val bytes = fixture
      for
        pages <- ZIO.scoped(ZIO.fromAutoCloseable(ZIO.attempt(Loader.loadPDF(bytes))).map(_.getNumberOfPages))
        evidence <- PdfEngine.evidence(Chunk.fromArray(bytes)).provide(PdfEngine.live)
      yield assertTrue(pages == 1, evidence.validation.isSuccess, evidence.decodedEvents > 0)
    },
    test("streaming decode agrees with whole-buffer decode across byte boundaries") {
      val bytes = fixture
      for
        expected <- PdfEngine.decode(Chunk.fromArray(bytes)).provide(PdfEngine.live)
        actual <- ZIO.foreach(List(1, 7, 64, 1024)) { size =>
          val source = ZStream.fromIterable(bytes.grouped(size).map(Chunk.fromArray).toList).flattenChunks
          PdfEngine.decode(source).runCollect.provide(PdfEngine.live)
        }
      yield assertTrue(actual.forall(_ == expected))
    }
  )
