package com.tybera.kyopdf.zio

import java.nio.charset.StandardCharsets.ISO_8859_1
import com.tybera.kyopdf.{ByteLimit, Facts, PdfError, RetentionLimits}
import _root_.zio.stream.ZStream
import _root_.zio.test.*

object PdfZIOSpec extends ZIOSpecDefault:
  private val bytes =
    """%PDF-1.7
      |1 0 obj
      |<< /Type /Catalog /Pages 2 0 R >>
      |endobj
      |2 0 obj
      |<< /Type /Pages /Kids [3 0 R] /Count 1 >>
      |endobj
      |3 0 obj
      |<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>
      |endobj
      |%%EOF
      |""".stripMargin.getBytes(ISO_8859_1)

  private val documentBytes =
    new String(bytes, ISO_8859_1)
      .replace("%%EOF", "trailer\n<< /Size 4 /Root 1 0 R >>\nstartxref\n0\n%%EOF")
      .getBytes(ISO_8859_1)

  def spec = suite("kyo-pdf ZIO compatibility")(
    test("ZIOs.run preserves Kyo typed failures and successful reports") {
      for
        limit <- PdfZIO.run(kyo.Async.defer(ByteLimit.mebibytes(1)))
        report <- PdfZIO.scan(bytes, limit)
        invalid <- PdfZIO.scan("not a PDF".getBytes(ISO_8859_1), limit).either
      yield assertTrue(report.facts == Facts(3, 0, 1), invalid.left.exists(_.isInstanceOf[PdfError.InvalidPdf]))
    },
    test("ZStream input is collected only through the configured bound") {
      for
        limit <- PdfZIO.run(kyo.Async.defer(ByteLimit.fromBytes(bytes.length.toLong - 1L)))
        result <- PdfZIO.scanStream(ZStream.fromIterable(bytes), limit).either
      yield assertTrue(result.left.exists(_.isInstanceOf[PdfError.TooLarge]))
    },
    test("a parser effect can be reused across direct and ZIO-lifted scans") {
      val direct = kyo.Abort.run[PdfError](ByteLimit.mebibytes(1).map(limit => com.tybera.kyopdf.PdfParser.scan(bytes, limit))).eval
      for
        limit <- PdfZIO.run(kyo.Async.defer(ByteLimit.mebibytes(1)))
        first <- PdfZIO.scanStream(ZStream.fromIterable(bytes), limit)
        second <- PdfZIO.scanStream(ZStream.fromIterable(bytes), limit)
      yield assertTrue(direct.exists(_.facts == Facts(3, 0, 1)), first.facts == Facts(3, 0, 1), second.facts == Facts(3, 0, 1))
    },
    test("chunked ZStream decode, page selection, and write stay typed") {
      for
        limit <- PdfZIO.run(kyo.Async.defer(ByteLimit.mebibytes(1)))
        decoded <- PdfZIO.decodeStream(ZStream.fromIterable(documentBytes).rechunk(7), limit, RetentionLimits())
        pages <- PdfZIO.pageRefs(decoded)
        written <- PdfZIO.selectPagesStream(ZStream.fromIterable(documentBytes).rechunk(11), limit, 1, 1).runCollect
        roundTrip <- PdfZIO.decode(written.toArray, limit)
        selected <- PdfZIO.pageRefs(roundTrip)
      yield assertTrue(pages.length == 1, selected.length == 1, written.nonEmpty)
    }
  )
