package io.github.adrielc.kyopdf

import java.io.RandomAccessFile
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import kyo.*
import zio.test.*

/** Real file transfers use fixed-size buffers; the >2 GiB case is sparse and
  * exercises indexing/offsets only. No large Array or Vector is constructed.
  */
object PdfPacketLargeSpec extends ZIOSpecDefault:
  private def run[A](value: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(value)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def get[A](value: A < (Abort[PdfError] & Sync)): A = run(value).fold(e => throw AssertionError(e.message), identity)
  private val small = PdfPacket.Bounds(memoryBytes = 8L * 1024 * 1024)
  private def text(file: RandomAccessFile, value: String): Unit = file.write(value.getBytes(ISO_8859_1))
  private def fixture(path: Path, payload: Long, sparse: Boolean = false): Unit =
    val file = new RandomAccessFile(path.toFile, "rw")
    val offsets = new Array[Long](6)
    def obj(id: Int, value: String): Unit =
      offsets(id) = file.getFilePointer
      text(file, s"$id 0 obj\n$value\nendobj\n")
    text(file, "%PDF-1.7\n")
    obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
    obj(2, "<< /Type /Pages /Count 1 /Kids [3 0 R] >>")
    obj(3, "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Contents 4 0 R >>")
    offsets(4) = file.getFilePointer
    text(file, "4 0 obj\n<< /Length 5 0 R >>\nstream\n")
    if sparse then file.seek(file.getFilePointer + payload)
    else
      val buffer = Array.fill[Byte](65536)(' '.toByte)
      var remaining = payload
      while remaining > 0 do
        val size = math.min(buffer.length.toLong, remaining).toInt
        file.write(buffer, 0, size)
        remaining -= size
    text(file, "\nendstream\nendobj\n")
    obj(5, payload.toString)
    val xref = file.getFilePointer
    text(file, "xref\n0 6\n0000000000 65535 f \n")
    (1 to 5).foreach(id => text(file, f"${offsets(id)}%010d 00000 n \n"))
    text(file, s"trailer\n<< /Size 6 /Root 1 0 R >>\nstartxref\n$xref\n%%EOF\n")
    file.close()
  private def consume(session: PdfPacket.Session)(pull: session.Pull, output: RandomAccessFile): Unit =
    var done = false
    while !done do get(pull.read()) match
      case None => done = true
      case Some(chunk) => output.write(chunk.array, 0, chunk.size)
  private def digest(path: Path): String =
    val file = new RandomAccessFile(path.toFile, "r")
    val hash = MessageDigest.getInstance("SHA-256")
    val buffer = new Array[Byte](65536)
    var count = file.read(buffer)
    while count >= 0 do
      if count > 0 then hash.update(buffer, 0, count)
      count = file.read(buffer)
    file.close()
    hash.digest().map(b => f"${b & 255}%02x").mkString

  def spec = suite("Large file-backed packet inputs")(
    test("two 96 MiB payloads produce a complete >192 MiB packet with an 8 MiB working budget") {
      val input = Files.createTempFile("packet-large-input-", ".pdf")
      val output = Files.createTempFile("packet-large-output-", ".pdf")
      val payload = 96L * 1024 * 1024
      fixture(input, payload)
      val session = get(PdfPacket.open(bounds = small))
      val file = new RandomAccessFile(output.toFile, "rw")
      val observations = Vector.tabulate(2) { i =>
        val source = get(PdfPacketSource.open(input, small))
        val lazyPayload = source.document.objects.forall(_.stream.isEmpty)
        consume(session)(get(session.append(s"Document ${i + 1}", source)), file)
        val stats = source.stats
        source.close()
        lazyPayload && stats.windowBytes == 65536 && stats.externalPayloadBytes == payload && source.isClosed
      }
      consume(session)(get(session.finish()), file)
      file.close()
      val receipt = session.receipt.get
      val indexedOutput = get(PdfPacketSource.open(output, small))
      val pages = get(PdfPages.pageRefs(indexedOutput.document))
      val copied = indexedOutput.streams.values.filter(_.length == payload).toVector
      val rawIntact = copied.forall(range => indexedOutput.bytes(range).forall(_ == ' '.toByte))
      indexedOutput.close()
      val correctHash = receipt.sha256 == digest(output)
      session.close()
      sys.env.get("KYO_PDF_LARGE_PACKET_OUTPUT").foreach(destination =>
        Files.copy(output, Path.of(destination), java.nio.file.StandardCopyOption.REPLACE_EXISTING))
      Files.delete(input); Files.delete(output)
      assertTrue(observations.forall(identity), receipt.bytes > 192L * 1024 * 1024, receipt.pages == 3,
        pages.size == 3, copied.size == 2, rawIntact, correctHash)
    },
    test("indexing a sparse >2 GiB stream uses Long offsets and skips the payload") {
      val input = Files.createTempFile("packet-long-offset-", ".pdf")
      val payload = Int.MaxValue.toLong + 65537
      fixture(input, payload, sparse = true)
      val source = get(PdfPacketSource.open(input, small))
      val stats = source.stats
      val objects = source.document.objects.size
      val length = source.streams(ObjectRef(4)).length
      source.close()
      Files.delete(input)
      val largeOffset = (1L << 40) + 123
      val entry = PdfPacket.xrefEntry(1, largeOffset, 0).toVector
      val restored = entry.slice(1, 9).foldLeft(0L)((n, b) => (n << 8) | (b & 255).toLong)
      assertTrue(length == payload, stats.externalPayloadBytes == payload, stats.windowLoads < 32,
        objects == 5, restored == largeOffset, entry.length == 11)
    },
    test("source and metadata limits reject before retaining large payloads") {
      val input = Files.createTempFile("packet-source-limit-", ".pdf")
      fixture(input, 2L * 1024 * 1024, sparse = true)
      val tooLarge = run(PdfPacketSource.open(input, small.copy(sourceBytes = 1024)))
      val file = new RandomAccessFile(input.toFile, "rw")
      file.setLength(0)
      text(file, "%PDF-1.7\n1 0 obj\n<< /Type /Catalog /Giant (")
      val buffer = Array.fill[Byte](65536)('a'.toByte)
      (0 until 16).foreach(_ => file.write(buffer))
      text(file, ") >>\nendobj\n")
      file.close()
      val metadata = run(PdfPacketSource.open(input, small.copy(memoryBytes = 2L * 1024 * 1024, objects = 10000, pages = 1024)))
      Files.delete(input)
      assertTrue(tooLarge.left.toOption.exists(_.isInstanceOf[PdfError.TooLarge]), metadata.isLeft,
        PdfPacket.validBounds(small.copy(sourceBytes = 256L * 1024 * 1024 * 1024, outputBytes = 256L * 1024 * 1024 * 1024)))
    },
    test("compressed metadata is decoded, but expansion and closed sources fail safely") {
      val input = Files.createTempFile("packet-compressed-metadata-", ".pdf")
      def compressed(expanded: Array[Byte]): Unit =
        val out = new java.io.ByteArrayOutputStream()
        val zip = new java.util.zip.DeflaterOutputStream(out)
        zip.write(expanded); zip.close()
        val raw = out.toByteArray
        val file = new RandomAccessFile(input.toFile, "rw")
        file.setLength(0)
        text(file, "%PDF-1.7\n1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n" +
          "2 0 obj << /Type /Pages /Kids [5 0 R] /Count 1 >> endobj\n" +
          s"4 0 obj << /Type /ObjStm /N 1 /First 4 /Filter /FlateDecode /Length ${raw.length} >>\nstream\n")
        file.write(raw)
        text(file, "\nendstream\nendobj\ntrailer << /Root 1 0 R >>\n%%EOF\n")
        file.close()
      compressed("5 0 << /Type /Page /Parent 2 0 R /MediaBox [0 0 10 10] >>".getBytes(ISO_8859_1))
      val source = get(PdfPacketSource.open(input, small))
      val decoded = source.document.byRef.contains(ObjectRef(5))
      val session = get(PdfPacket.open(bounds = small))
      val pull = get(session.append("Compressed metadata", source))
      source.close()
      val closed = run(pull.read())
      session.close()
      compressed(Array.fill[Byte](1024 * 1024)(' '.toByte))
      val bomb = run(PdfPacketSource.open(input, small))
      Files.delete(input)
      assertTrue(decoded, closed.isLeft, bomb.isLeft)
    },
    test("a newer non-stream revision clears an older payload range") {
      val input = Files.createTempFile("packet-revised-object-", ".pdf")
      val file = new RandomAccessFile(input.toFile, "rw")
      text(file, "%PDF-1.7\n1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n" +
        "2 0 obj << /Type /Pages /Count 1 /Kids [3 0 R] >> endobj\n" +
        "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 10 10] /Resources << /ExtGState << /State 4 0 R >> >> >> endobj\n" +
        "4 0 obj << /Length 3 >> stream\nold\nendstream endobj\n" +
        "4 0 obj << /Type /ExtGState /CA 1 >> endobj\ntrailer << /Root 1 0 R >>\n%%EOF\n")
      file.close()
      val source = get(PdfPacketSource.open(input, small))
      val stale = source.streams.contains(ObjectRef(4))
      val session = get(PdfPacket.open(bounds = small))
      val pull = get(session.append("Revised source", source))
      var bytes = 0L
      var done = false
      while !done do get(pull.read()) match
        case None => done = true
        case Some(chunk) => bytes += chunk.size
      source.close(); session.close(); Files.delete(input)
      assertTrue(!stale, bytes > 0)
    }
  ) @@ TestAspect.sequential @@ TestAspect.withLiveClock
