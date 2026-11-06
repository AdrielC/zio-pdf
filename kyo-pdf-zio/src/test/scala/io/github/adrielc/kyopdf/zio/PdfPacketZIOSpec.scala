package io.github.adrielc.kyopdf.zio

import io.github.adrielc.kyopdf.*
import java.nio.file.{Files, Path}
import kyo.Async
import _root_.zio.{Chunk, Promise, Ref as ZRef, ZIO}
import _root_.zio.stream.{ZSink, ZStream}
import _root_.zio.test.*

object PdfPacketZIOSpec extends ZIOSpecDefault:
  import PdfValue.*
  private def fixture(padding: Int = 0) =
    val doc = PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> PdfValue.Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)),
        "MediaBox" -> PdfValue.Array(Vector(Number(0), Number(0), Number(300), Number(200))), "Contents" -> Ref(ObjectRef(4)))),
      PdfObject(ObjectRef(4), dict(), Some(Vector.fill(padding)(' '.toByte)))
    ), dict("Root" -> Ref(ObjectRef(1))))
    PdfZIO.write(doc)
  private def temporary[A, E](use: Path => ZIO[Any, E, A]): ZIO[Any, E | Throwable, A] = ZIO.scoped {
    ZIO.acquireRelease(ZIO.attemptBlockingIO(Files.createTempDirectory("packet-test-")))(path =>
      ZIO.attemptBlockingIO(Files.delete(path)).orDie).flatMap(use)
  }
  private def empty(path: Path) = ZIO.scoped {
    ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(Files.list(path))).map(stream => !stream.findAny().isPresent)
  }

  private def largeFixture(payload: Long): ZStream[Any, Nothing, Byte] =
    val values = Vector("<< /Type /Catalog /Pages 2 0 R >>", "<< /Type /Pages /Count 1 /Kids [3 0 R] >>",
      "<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 200] /Contents 4 0 R >>")
    val header = new StringBuilder("%PDF-1.7\n")
    val offsets = new scala.Array[Long](5)
    values.zipWithIndex.foreach { (value, index) =>
      offsets(index + 1) = header.length
      header.append(s"${index + 1} 0 obj\n$value\nendobj\n")
    }
    offsets(4) = header.length
    header.append(s"4 0 obj\n<< /Length $payload >>\nstream\n")
    val end = "\nendstream\nendobj\n"
    val xref = header.length.toLong + payload + end.length
    val footer = new StringBuilder(end + "xref\n0 5\n0000000000 65535 f \n")
    (1 to 4).foreach(id => footer.append(f"${offsets(id)}%010d 00000 n \n"))
    footer.append(s"trailer\n<< /Size 5 /Root 1 0 R >>\nstartxref\n$xref\n%%EOF\n")
    val block = Chunk.fill[Byte](65536)(' '.toByte)
    ZStream.fromIterable(header.result().getBytes(java.nio.charset.StandardCharsets.ISO_8859_1)) ++
      ZStream.repeat(block).take(payload / block.size).flattenChunks ++
      ZStream.fromChunk(block.take((payload % block.size).toInt)) ++
      ZStream.fromIterable(footer.result().getBytes(java.nio.charset.StandardCharsets.ISO_8859_1))

  def spec = suite("Packet live and staged ZIO delivery")(
    test("both delivery modes produce the same complete PDF") {
      for
        bytes <- fixture(128 * 1024)
        parts = ZStream.fromIterable(Vector("First", "Second").map(name => PdfPacketZIO.Part(name, ZStream.fromIterable(bytes))))
        live <- PdfPacketZIO.stitch(parts, delivery = PdfPacket.Delivery.Live).runCollect
        staged <- PdfPacketZIO.stitch(parts, delivery = PdfPacket.Delivery.Staged).runCollect
        limit <- PdfZIO.run(Async.defer(ByteLimit.fromBytes(PdfPacket.DefaultOutputBytes)))
        doc <- PdfZIO.decode(live.toArray, limit)
        pages <- PdfZIO.pageRefs(doc)
      yield assertTrue(live == staged, pages.length == 3)
    },
    test("live delivery emits before later inputs and releases an early consumer") {
      for
        bytes <- fixture()
        opened <- ZRef.make(0)
        closed <- ZRef.make(0)
        first = PdfPacketZIO.Part("First", (ZStream.fromZIO(opened.update(_ + 1)).drain ++
          ZStream.fromIterable(bytes)).ensuring(closed.update(_ + 1)))
        second = PdfPacketZIO.Part("Later", ZStream.fromZIO(opened.update(_ + 100)).drain ++ ZStream.fail("late input"))
        stream = PdfPacketZIO.stitch(ZStream(first, second), delivery = PdfPacket.Delivery.Live)
        before <- opened.get
        prefix <- stream.take(1).runCollect
        opens <- opened.get
        closes <- closed.get
      yield assertTrue(before == 0, prefix.toVector == Vector('%'.toByte), opens == 1, closes == 1)
    },
    test("staged delivery exposes zero bytes on a later input failure and cleans its spool") {
      temporary { directory =>
        for
          bytes <- fixture()
          emitted <- ZRef.make(0L)
          parts = ZStream(PdfPacketZIO.Part("First", ZStream.fromIterable(bytes)), PdfPacketZIO.Part("Bad", ZStream.fail("late input")))
          result <- PdfPacketZIO.stitch(parts, stagingDirectory = Some(directory)).tap(_ => emitted.update(_ + 1)).runDrain.either
          count <- emitted.get
          cleaned <- empty(directory)
        yield assertTrue(result.left.toOption.contains("late input"), count == 0, cleaned)
      }
    },
    test("successful staged delivery removes its file when the consumer stops early") {
      temporary { directory =>
        for
          bytes <- fixture()
          prefix <- PdfPacketZIO.stitch(ZStream(PdfPacketZIO.Part("First", ZStream.fromIterable(bytes))),
            stagingDirectory = Some(directory)).take(1).runCollect
          cleaned <- empty(directory)
        yield assertTrue(prefix.length == 1, cleaned)
      }
    },
    test("source admission fails early before pulling another PDF") {
      for
        bytes <- fixture()
        chunks <- ZRef.make(0)
        later <- ZRef.make(0)
        first = PdfPacketZIO.Part("First", ZStream.fromIterable(bytes).rechunk(7).chunks.tap(_ => chunks.update(_ + 1)).flattenChunks)
        second = PdfPacketZIO.Part("Later", ZStream.fromZIO(later.update(_ + 1)).drain ++ ZStream.fromIterable(bytes))
        result <- PdfPacketZIO.stitch(ZStream(first, second), bounds = PdfPacket.Bounds(sourceBytes = 8)).runDrain.either
        pulls <- chunks.get
        others <- later.get
      yield assertTrue(result.isLeft, pulls == 2, others == 0)
    },
    test("64 sequential PDF sources stitch more than 8 MiB through the file sink") {
      temporary { directory =>
        for
          bytes <- fixture(128 * 1024)
          opened <- ZRef.make(0)
          active <- ZRef.make(0)
          peak <- ZRef.make(0)
          source = (ZStream.fromZIO(opened.update(_ + 1) *> active.updateAndGet(_ + 1).flatMap(n => peak.update(_.max(n)))).drain ++
            ZStream.fromIterable(bytes)).ensuring(active.update(_ - 1))
          parts = ZStream.fromIterable(Vector.tabulate(64)(i => PdfPacketZIO.Part(s"Document ${i + 1}", source)))
          path = directory.resolve("transfer.pdf")
          count <- PdfPacketZIO.stitch(parts, delivery = PdfPacket.Delivery.Live).run(ZSink.fromPath(path))
          limit <- PdfZIO.run(Async.defer(ByteLimit.fromBytes(PdfPacket.DefaultOutputBytes)))
          facts <- PdfZIO.scanStream(ZStream.fromPath(path), limit)
          n <- opened.get
          max <- peak.get
          remaining <- active.get
          _ <- ZIO.attemptBlockingIO {
            sys.env.get("KYO_PDF_PACKET_TRANSFER").foreach(destination => Files.copy(path, Path.of(destination), java.nio.file.StandardCopyOption.REPLACE_EXISTING))
            Files.delete(path)
          }
        yield assertTrue(count > 8 * 1024 * 1024, facts.facts.pages == 68, n == 64, max == 1, remaining == 0)
      }
    },
    test("an 80 MiB lazy source transfers through staged delivery without materializing its payload") {
      temporary { directory =>
        val payload = 80L * 1024 * 1024
        val bounds = PdfPacket.Bounds(memoryBytes = 8L * 1024 * 1024, sourceBytes = 128L * 1024 * 1024,
          outputBytes = 128L * 1024 * 1024, spoolBytes = 192L * 1024 * 1024)
        val path = directory.resolve("large.pdf")
        for
          count <- PdfPacketZIO.stitch(ZStream(PdfPacketZIO.Part("Large document", largeFixture(payload))),
            bounds = bounds, stagingDirectory = Some(directory)).run(ZSink.fromPath(path))
          source <- PdfZIO.run(Async.defer(PdfPacketSource.open(path, bounds)))
          pages <- PdfZIO.pageRefs(source.document)
          stats = source.stats
          _ <- ZIO.attemptBlockingIO {
            source.close()
            sys.env.get("KYO_PDF_LARGE_ZIO_OUTPUT").foreach(destination =>
              Files.copy(path, Path.of(destination), java.nio.file.StandardCopyOption.REPLACE_EXISTING))
            Files.delete(path)
          }
          cleaned <- empty(directory)
        yield assertTrue(count > payload, pages.size == 2, stats.externalPayloadBytes >= payload,
          stats.windowBytes == 65536, cleaned)
      }
    },
    test("staged spool quota accounts for output and the next source together") {
      temporary { directory =>
        for
          emitted <- ZRef.make(0L)
          opened <- ZRef.make(0)
          bytes = ZStream.fromZIO(opened.update(_ + 1)).drain ++ largeFixture(512 * 1024)
          parts = ZStream(PdfPacketZIO.Part("First", bytes), PdfPacketZIO.Part("Second", bytes))
          result <- PdfPacketZIO.stitch(parts, bounds = PdfPacket.Bounds(spoolBytes = 1280 * 1024),
            stagingDirectory = Some(directory)).tapChunks(c => emitted.update(_ + c.size)).runDrain.either
          count <- emitted.get
          cleaned <- empty(directory)
          n <- opened.get
        yield assertTrue(result.left.toOption.exists(_.isInstanceOf[PdfError.RetentionExceeded]), count == 0, n == 2, cleaned)
      }
    },
    test("output quota and invalid headers clean all files without reading a later source") {
      temporary { directory =>
        for
          pulls <- ZRef.make(0)
          invalid = ZStream.repeat(Chunk.fill[Byte](65536)('x'.toByte)).tap(_ => pulls.update(_ + 1)).flattenChunks
          bad <- PdfPacketZIO.stitch(ZStream(PdfPacketZIO.Part("Invalid", invalid)), stagingDirectory = Some(directory)).runDrain.either
          n <- pulls.get
          limited <- PdfPacketZIO.stitch(ZStream(PdfPacketZIO.Part("Body", largeFixture(128 * 1024))),
            bounds = PdfPacket.Bounds(outputBytes = 1024), stagingDirectory = Some(directory)).runDrain.either
          cleaned <- empty(directory)
        yield assertTrue(bad.isLeft, n == 1, limited.left.toOption.exists(_.isInstanceOf[PdfError.TooLarge]), cleaned)
      }
    },
    test("interrupting an unfinished source removes both input and output spools") {
      temporary { directory =>
        for
          started <- Promise.make[Nothing, Unit]
          bytes = ZStream.fromIterable("%PDF-1.7\n".getBytes(java.nio.charset.StandardCharsets.ISO_8859_1)) ++
            ZStream.fromZIO(started.succeed(())).drain ++ ZStream.never
          fiber <- PdfPacketZIO.stitch(ZStream(PdfPacketZIO.Part("Waiting", bytes)), stagingDirectory = Some(directory)).runDrain.fork
          _ <- started.await
          _ <- fiber.interrupt
          cleaned <- empty(directory)
        yield assertTrue(cleaned)
      }
    }
  ) @@ TestAspect.sequential
