package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1
import kyo.*
import zio.test.*

object PdfSharedParserSpec extends ZIOSpecDefault:
  private def result[A](effect: A < Abort[PdfError]): Either[PdfError, A] =
    Abort.run[PdfError](effect).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def bytes(text: String): Array[Byte] = text.getBytes(ISO_8859_1)
  private val input = bytes("%PDF-1.7\n1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n" +
    "2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj\n" +
    "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 10 10] /Contents 4 0 R >> endobj\n" +
    "4 0 obj << /Length 3 >> stream\nq Q\nendstream endobj\ntrailer << /Size 5 /Root 1 0 R >>\n%%EOF\n")
  private def limit = result(ByteLimit.mebibytes(8)).toOption.get

  private def assemble(objects: List[String]): Array[Byte] =
    val text = new StringBuilder("%PDF-1.7\n")
    val offsets = objects.zipWithIndex.map { (body, i) =>
      val offset = text.length
      text.append(s"${i + 1} 0 obj\n$body\nendobj\n")
      offset
    }
    val xref = text.length
    text.append(s"xref\n0 ${objects.size + 1}\n0000000000 65535 f \n")
    offsets.foreach(offset => text.append(f"$offset%010d 00000 n \n"))
    text.append(s"trailer << /Size ${objects.size + 1} /Root 1 0 R >>\nstartxref\n$xref\n%%EOF\n")
    bytes(text.result())

  def spec = suite("Shared PDF parsing")(
    test("a header at offset 1024 works with one-byte transport chunks") {
      val prefixed = bytes("x" * 1024) ++ input
      val reader = PdfRead.open(limit)
      val admitted = prefixed.indices.map(i => reader.offer(1)(_ => prefixed(i)))
      assertTrue(admitted.forall(_.isRight), reader.decode() == result(PdfDocument.decode(prefixed, limit)))
    },
    test("the supported streaming size is checked before a logical oversized read") {
      val reader = PdfRead.open(result(ByteLimit.mebibytes(128)).toOption.get)
      var reads = 0
      val failed = reader.offer(PdfRead.MaxInputBytes + 1)(_ => { reads += 1; 0.toByte })
      assertTrue(failed == Left(PdfError.TooLarge(PdfRead.MaxInputBytes, PdfRead.MaxInputBytes.toLong + 1)),
        reads == 0, reader.stats.inputBlocks == 0)
    },
    test("forward indirect Length resolution is shared across scan and typed decode") {
      val document = assemble(List(
        "<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        "<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>",
        "<< /Length 5 0 R >>\nstream\nq Q\nendstream", "3"))
      val direct = result(PdfDocument.decode(document, limit))
      val reader = PdfRead.open(limit)
      document.grouped(7).foreach(chunk => reader.offer(chunk.length)(chunk.apply))
      assertTrue(direct.isRight, reader.decode() == direct,
        direct.exists(_.byRef(ObjectRef(4)).stream.contains(bytes("q Q").toVector)))
    },
    test("compressed object streams decode on the shared source") {
      val expanded = bytes("5 0 << /Type /Page /Parent 2 0 R >>")
      val out = new java.io.ByteArrayOutputStream()
      val deflater = new java.util.zip.DeflaterOutputStream(out)
      deflater.write(expanded)
      deflater.close()
      val compressed = out.toByteArray
      val document = assemble(List(
        "<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Kids [5 0 R] /Count 1 >>", "null",
        s"<< /Type /ObjStm /N 1 /First 4 /Filter /FlateDecode /Length ${compressed.length} >>\nstream\n" +
          new String(compressed, ISO_8859_1) + "\nendstream"))
      val reader = PdfRead.open(limit)
      document.grouped(11).foreach(chunk => reader.offer(chunk.length)(chunk.apply))
      val decoded = reader.decode()
      assertTrue(decoded == result(PdfDocument.decode(document, limit)), decoded.exists(_.byRef.contains(ObjectRef(5))))
    },
    test("every two-way fragmentation decodes from the same owned blocks") {
      val expected = result(PdfDocument.decode(input, limit))
      val found = (0 to input.length).map { split =>
        val reader = PdfRead.open(limit)
        val offered = reader.offer(split)(input.apply).flatMap(_ => reader.offer(input.length - split)(i => input(split + i)))
        offered.flatMap(_ => reader.decode())
      }
      assertTrue(expected.isRight, found.forall(_ == expected))
    },
    test("header markers and versions survive one-byte fragmentation") {
      val reader = PdfRead.open(limit)
      val offered = input.indices.map(index => reader.offer(1)(_ => input(index)))
      assertTrue(offered.forall(_.isRight), reader.decode().isRight, reader.stats.inputBlocks == 0)
    },
    test("a partial structural query exits before malformed later objects") {
      val damaged = bytes(new String(input, ISO_8859_1).replace("trailer", "99 0 obj (unterminated\ntrailer"))
      val prefix = result(PdfParser.scanUntil(damaged, limit)(_.facts.pages >= 1))
      val complete = result(PdfParser.scan(damaged, limit))
      assertTrue(prefix.exists(_.facts == Facts(3, 0, 1)), complete.isLeft)
    },
    test("a consumer error stops structural production before a later malformed object") {
      val badValue = bytes(new String(input, ISO_8859_1).replace("/Type /Catalog", "/Type invalid")) ++ bytes("99 0 obj (")
      val decoded = result(PdfDocument.decode(badValue, limit))
      assertTrue(decoded.left.exists(_.message.contains("Unsupported PDF primitive token: invalid")))
    },
    test("retention errors stop at the admitted boundary before typed payload construction") {
      val reader = PdfRead.open(limit)
      val _ = reader.offer(input.length)(input.apply)
      val failed = reader.decode(RetentionLimits(maxObjects = 1))
      assertTrue(failed.left.exists(_.isInstanceOf[PdfError.RetentionExceeded]), reader.stats.inputBlocks == 0)
    },
    test("header errors and byte bounds reject input early and release admitted blocks") {
      val reader = PdfRead.open(limit)
      var reads = 0
      val wrong = bytes("%PDF-X\n")
      val header = reader.offer(wrong.length)(wrong.apply)
      val next = reader.offer(1)(_ => { reads += 1; 1.toByte })
      val small = PdfRead.open(result(ByteLimit.fromBytes(4)).toOption.get)
      val oversized = small.offer(5)(_ => { reads += 1; 1.toByte })
      assertTrue(header.left.exists(_.isInstanceOf[PdfError.InvalidPdf]), next.isLeft,
        oversized == Left(PdfError.TooLarge(4, 5)), reads == 0, reader.stats.inputBlocks == 0)
    },
    test("stream payload markers and NUL bytes never become structural syntax") {
      val payload = "endobj 9 0 obj /Type /Page\u0000"
      val changed = bytes(new String(input, ISO_8859_1).replace("/Length 3", s"/Length ${payload.length}").replace("q Q", payload))
      val direct = result(PdfDocument.decode(changed, limit))
      val reader = PdfRead.open(limit)
      changed.grouped(13).foreach(chunk => reader.offer(chunk.length)(chunk.apply))
      val chunked = reader.decode()
      assertTrue(chunked == direct, direct.exists(_.objects.find(_.stream.nonEmpty).flatMap(_.stream).contains(bytes(payload).toVector)))
    },
    test("bounded content tokens retain binary literals without a whole content String") {
      val content = bytes("BT /F1 12 Tf (A\u00ff\u0000B) Tj [3 <f>] TJ ET")
      val parsed = result(PdfContentParser.parse(content))
      assertTrue(parsed.exists(_.contains(ContentToken.Literal(Vector[Byte](65, -1, 0, 66)))))
    }
  )
