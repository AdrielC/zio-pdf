package com.tybera.kyopdf

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.zip.DeflaterOutputStream
import kyo.*
import zio.test.*

object PdfParserSpec extends ZIOSpecDefault:
  private def assemble(objects: List[String], trailer: String = ""): Array[Byte] =
    val out = new StringBuilder("%PDF-1.7\n")
    val offsets = objects.zipWithIndex.map { (obj, index) =>
      val offset = out.length
      out.append(s"${index + 1} 0 obj\n$obj\nendobj\n")
      offset
    }
    val xref = out.length
    out.append(s"xref\n0 ${objects.size + 1}\n0000000000 65535 f \n")
    offsets.foreach(offset => out.append(f"$offset%010d 00000 n \n"))
    out.append(s"trailer\n<< /Size ${objects.size + 1} /Root 1 0 R $trailer>>\nstartxref\n$xref\n%%EOF\n")
    out.toString.getBytes(ISO_8859_1)

  private def pdf(content: String = "BT (Kyo) Tj ET", indirectLength: Boolean = false, trailer: String = ""): Array[Byte] =
    val base = List(
      "<< /Type /Catalog /Pages 2 0 R >>",
      "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
      s"<< /Length ${if indirectLength then "5 0 R" else content.length.toString} >>\nstream\n$content\nendstream"
    )
    assemble(if indirectLength then base :+ content.length.toString else base, trailer)

  private def deflate(value: String): String =
    val output = new ByteArrayOutputStream()
    val stream = new DeflaterOutputStream(output)
    stream.write(value.getBytes(ISO_8859_1))
    stream.close()
    new String(output.toByteArray, ISO_8859_1)

  private def objectStreamPdf: Array[Byte] =
    val expanded = "5 0 << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>"
    val compressed = deflate(expanded)
    assemble(List(
      "<< /Type /Catalog /Pages 2 0 R >>",
      "<< /Type /Pages /Kids [5 0 R] /Count 1 >>",
      "null",
      s"<< /Type /ObjStm /N 1 /First 4 /Filter /FlateDecode /Length ${compressed.getBytes(ISO_8859_1).length} >>\nstream\n$compressed\nendstream"
    ))

  private def result[A](value: A < Abort[PdfError]): Either[PdfError, A] =
    Abort.run[PdfError](value).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => Left(PdfError.InvalidPdf(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  private def thumbnail(value: PdfThumbnail.ImageObject < (Abort[PdfError] & Sync)): Either[PdfError, PdfThumbnail.ImageObject] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(value)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => Left(PdfError.ThumbnailFailed(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  def spec = suite("kyo-pdf")(
    test("kyo-parse scans objects, streams, pages, and bounded retention") {
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(pdf(), limit)))
      assertTrue(parsed.exists(_.facts == Facts(4, 1, 1)), parsed.exists(_.version == "1.7"), parsed.exists(_.retention.nodes > 0))
    },
    test("zio-pdf writer binary marker may immediately follow the version") {
      val written = new String(pdf(), ISO_8859_1)
        .replace("%PDF-1.7\n", "%PDF-1.7%\u00e2\u00e3\u00cf\u00d3\n")
        .getBytes(ISO_8859_1)
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(written, limit)))
      assertTrue(parsed.exists(_.version == "1.7"), parsed.exists(_.facts == Facts(4, 1, 1)))
    },
    test("stream bytes cannot inject fake object or page markers") {
      val payload = "5 0 obj << /Type /Page >> endobj"
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(pdf(payload), limit)))
      assertTrue(parsed.exists(_.facts == Facts(4, 1, 1)))
    },
    test("classic xref resolves an indirect stream length") {
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(pdf(indirectLength = true), limit)))
      assertTrue(parsed.exists(_.facts == Facts(5, 1, 1)))
    },
    test("bounded Flate object streams contribute embedded objects and pages") {
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(objectStreamPdf, limit)))
      assertTrue(parsed.exists(_.facts == Facts(4, 0, 1)), parsed.exists(_.retention.payloadBytes > objectStreamPdf.length))
    },
    test("object-stream integer fields cannot wrap their bounds") {
      val malformed = new String(objectStreamPdf, ISO_8859_1).replace("/N 1", "/N 4294967296").getBytes(ISO_8859_1)
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(malformed, limit)))
      assertTrue(parsed.left.exists(_.isInstanceOf[PdfError.InvalidPdf]))
    },
    test("encryption is a typed retention-policy failure") {
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(pdf(trailer = "/Encrypt 9 0 R "), limit))).toOption.get
      assertTrue(parsed.encrypted, result(PdfParser.validate(parsed, RetentionLimits())).left.exists(_.isInstanceOf[PdfError.InvalidPdf]))
    },
    test("input and expanded retention limits fail explicitly") {
      val bytes = pdf()
      val tooLarge = result(ByteLimit.fromBytes(8)).toOption.get
      val parsed = result(ByteLimit.mebibytes(1).map(limit => PdfParser.scan(bytes, limit))).toOption.get
      assertTrue(
        result(PdfParser.scan(bytes, tooLarge)).left.exists(_.isInstanceOf[PdfError.TooLarge]),
        result(PdfParser.validate(parsed, RetentionLimits(maxObjects = 1))).left.exists(_.isInstanceOf[PdfError.RetentionExceeded]),
        result(PdfParser.validate(parsed, RetentionLimits(maxDepth = 0))).left.exists(_.isInstanceOf[PdfError.RetentionExceeded])
      )
    },
    test("linearization boundary is parsed by kyo-parse") {
      val bytes = ("%PDF-1.7\n1 0 obj\n<< /Linearized 1 /L 300 /E 180 /N 1 >>\nendobj\n" + " " * 240).getBytes(ISO_8859_1)
      assertTrue(result(PdfParser.firstPageByteLength(bytes)) == Right(180L))
    },
    test("renderer-neutral thumbnail builder emits a Flate DeviceGray XObject") {
      val built = thumbnail(PdfThumbnail.imageObject(9, 0, PdfThumbnail.Options(8, 6)))
      assertTrue(built.exists(_.info == ThumbnailInfo(9, 0, 8, 6, built.toOption.get.info.compressedBytes)),
        built.exists(value => new String(value.bytes.take(180), ISO_8859_1).contains("/Subtype /Image")))
    },
    test("content operators and arrays parse through typed Abort values") {
      val valid = result(PdfContentParser.parse("BT /F1 12 Tf [(Kyo) -20 <504446>] TJ ET".getBytes(ISO_8859_1)))
      val unsupported = result(PdfContentParser.parse("BT (\\101) Tj ET".getBytes(ISO_8859_1)))
      assertTrue(valid.exists(_.exists(_ == ContentToken.Operator("TJ"))), unsupported.isLeft)
    },
    test("kyo-schema derives the public parser report") {
      val schema = Schema[ScanReport]
      assertTrue(schema != null)
    }
  )
