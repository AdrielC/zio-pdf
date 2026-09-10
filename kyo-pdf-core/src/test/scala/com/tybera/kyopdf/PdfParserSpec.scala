package com.tybera.kyopdf

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.{Files, Path}
import java.util.zip.DeflaterOutputStream
import kyo.*
import zio.test.*

object PdfParserSpec extends ZIOSpecDefault:
  private val qpdf = sys.env.get("PATH").toVector.flatMap(_.split(File.pathSeparator)).map(directory => Path.of(directory, "qpdf"))
    .find(path => Files.isRegularFile(path) && Files.isExecutable(path))

  private val courtFixtures = Vector(
    "court-corpus/scotus-atlantic-richfield-slip-opinion.pdf",
    "court-corpus/scotus-order-list-2025-05-19.pdf",
    "court-corpus/ca4-bayramov-v-american-credit-acceptance.pdf",
    "court-corpus/cafc-janich-v-collins.pdf",
    "court-corpus/govinfo-district-court-order.pdf",
    "court-corpus/oknd-general-order-2024-09.pdf"
  )

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

  private def twoPagePdf: Array[Byte] =
    val first = "BT (First) Tj ET"
    val second = "BT (Second) Tj ET"
    assemble(List(
      "<< /Type /Catalog /Pages 2 0 R >>",
      "<< /Type /Pages /Kids [3 0 R 5 0 R] /Count 2 >>",
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R >>",
      s"<< /Length ${first.length} >>\nstream\n$first\nendstream",
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 6 0 R >>",
      s"<< /Length ${second.length} >>\nstream\n$second\nendstream"
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

  private def syncResult[A](value: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(value)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => Left(PdfError.InvalidPdf(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  private def qpdfCheck(bytes: Array[Byte], fixture: String): Either[PdfError, Unit] = qpdf match
    case None => Right(())
    case Some(executable) =>
      val path = Files.createTempFile("kyo-pdf-round-trip-", ".pdf")
      try
        val written = Files.write(path, bytes)
        val process = ProcessBuilder(executable.toString, "--check", written.toString).redirectErrorStream(true).start()
        val output = new String(process.getInputStream.readAllBytes(), ISO_8859_1)
        val exit = process.waitFor()
        Either.cond(exit == 0, (), PdfError.InvalidPdf(s"qpdf rejected $fixture: $output"))
      finally
        val _ = Files.deleteIfExists(path)

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
    test("decoded document graph preserves page and stream objects") {
      val decoded = result(ByteLimit.mebibytes(1).map(limit => PdfDocument.decode(twoPagePdf, limit)))
      val pages = decoded.flatMap(document => result(PdfPages.pageRefs(document)))
      val content = decoded.flatMap { document =>
        document.byRef.get(ObjectRef(6)).toRight(PdfError.InvalidPdf("missing content stream"))
          .flatMap(stream => syncResult(PdfDocument.decodedStream(stream)))
      }
      assertTrue(
        decoded.exists(_.objects.length == 6),
        pages.exists(_.map(_.number) == Vector(3L, 5L)),
        content.exists(bytes => new String(bytes.toArray, ISO_8859_1).contains("Second"))
      )
    },
    test("name escapes stop at a compact array boundary") {
      val compact = assemble(List(
        "<< /Type /Catalog /Pages 2 0 R /Prop_Build << /App << /OS [/Windows#20NT#20#28unknown#29] /TrustedMode false /Name /Adobe#20LiveCycle /REx /11.0 >> >> >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>"
      ))
      assertTrue(result(ByteLimit.mebibytes(1).map(limit => PdfDocument.decode(compact, limit))).isRight)
    },
    test("chunked input decodes without an unbounded adapter collection") {
      val bytes = twoPagePdf
      val limit = result(ByteLimit.mebibytes(1)).toOption.get
      val first = result(PdfInput.empty(limit).feed(bytes.take(31))).toOption.get
      val second = result(first.feed(bytes.slice(31, 137))).toOption.get
      val decoded = result(second.feed(bytes.drop(137))).flatMap(input => result(input.finish()))
      assertTrue(decoded.exists(_.objects.length == 6))
    },
    test("page selection writes a self-contained PDF that decodes again") {
      val limit = result(ByteLimit.mebibytes(1)).toOption.get
      val selected = for
        original <- result(PdfDocument.decode(twoPagePdf, limit))
        document <- result(PdfPages.select(original, 2, 2))
        bytes <- syncResult(PdfWriter.write(document))
        decoded <- result(PdfDocument.decode(bytes.toArray, limit))
        pages <- result(PdfPages.pageRefs(decoded))
      yield (decoded, pages, bytes)
      val content = selected.flatMap { (document, _, _) =>
        document.objects.find(_.stream.nonEmpty).toRight(PdfError.InvalidPdf("selected content stream is missing"))
          .flatMap(stream => syncResult(PdfDocument.decodedStream(stream)))
      }
      assertTrue(
        selected.exists(_._2.length == 1),
        selected.exists(_._1.root.contains(ObjectRef(1))),
        content.exists(bytes => new String(bytes.toArray, ISO_8859_1).contains("Second"))
      )
    },
    test("page selection materializes inherited page attributes") {
      val inherited = assemble(List(
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 /MediaBox [0 0 612 792] /Rotate 90 >>",
        "<< /Type /Page /Parent 2 0 R >>"
      ))
      val limit = result(ByteLimit.mebibytes(1)).toOption.get
      val selected = for
        original <- result(PdfDocument.decode(inherited, limit))
        rewritten <- result(PdfPages.select(original, 1, 1))
        page <- result(PdfPages.pageRefs(rewritten)).flatMap(_.headOption.toRight(PdfError.InvalidPdf("missing page")))
        dict <- rewritten.byRef.get(page).flatMap(_.dictionary).toRight(PdfError.InvalidPdf("missing page dictionary"))
      yield dict
      assertTrue(
        selected.toOption.flatMap(_.get("MediaBox")).contains(PdfValue.Array(Vector(
          PdfValue.Number(0), PdfValue.Number(0), PdfValue.Number(612), PdfValue.Number(792)))),
        selected.toOption.flatMap(_.get("Rotate")).contains(PdfValue.Number(90))
      )
    },
    test("decoded graph rejects encrypted rewrite through a typed error") {
      val decoded = result(ByteLimit.mebibytes(1).map(limit => PdfDocument.decode(pdf(trailer = "/Encrypt 9 0 R "), limit)))
      assertTrue(decoded.left.exists(_.isInstanceOf[PdfError.InvalidPdf]))
    },
    test("public court PDFs decode, select page one, write, and decode again") {
      val limit = result(ByteLimit.mebibytes(20)).toOption.get
      val checked = courtFixtures.map { fixture =>
        val input = Option(getClass.getResourceAsStream(s"/$fixture")).toRight(PdfError.InvalidPdf(s"Missing $fixture"))
        input.flatMap { stream =>
          try
            for
              decoded <- result(PdfDocument.decode(stream.readAllBytes(), limit)).left.map(error => PdfError.InvalidPdf(s"$fixture decode: ${error.message}"))
              pages <- result(PdfPages.pageRefs(decoded)).left.map(error => PdfError.InvalidPdf(s"$fixture pages: ${error.message}"))
              _ <- Either.cond(pages.nonEmpty, (), PdfError.InvalidPdf(s"$fixture has no pages"))
              selected <- result(PdfPages.select(decoded, 1, 1)).left.map(error => PdfError.InvalidPdf(s"$fixture select: ${error.message}"))
              written <- syncResult(PdfWriter.write(selected)).left.map(error => PdfError.InvalidPdf(s"$fixture write: ${error.message}"))
              _ <- qpdfCheck(written.toArray, fixture)
              roundTrip <- result(PdfDocument.decode(written.toArray, limit)).left.map(error => PdfError.InvalidPdf(s"$fixture round trip decode: ${error.message}"))
              roundTripPages <- result(PdfPages.pageRefs(roundTrip)).left.map(error => PdfError.InvalidPdf(s"$fixture round trip pages: ${error.message}"))
              _ <- Either.cond(roundTripPages.length == 1, (), PdfError.InvalidPdf(s"$fixture round trip has ${roundTripPages.length} pages"))
            yield ()
          finally stream.close()
        }
      }
      assertTrue(checked.forall(_.isRight))
    },
    test("kyo-schema derives the public parser report") {
      val schema = Schema[ScanReport]
      assertTrue(schema != null)
    }
  )
