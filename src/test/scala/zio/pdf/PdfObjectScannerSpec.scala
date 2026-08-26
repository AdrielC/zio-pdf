package zio.pdf

import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.test.*

object PdfObjectScannerSpec extends ZIOSpecDefault {

  private val ascii = StandardCharsets.US_ASCII

  private def bytes(value: String): Chunk[Byte] =
    Chunk.fromArray(value.getBytes(ascii))

  private def scan(parts: Chunk[Chunk[Byte]], config: PdfObjectScanner.Config = PdfObjectScanner.Config.default) =
    parts.foldLeft[Either[PdfObjectScanner.Error, (PdfObjectScanner.Cursor, Chunk[PdfObjectScanner.Boundary])]](
      Right((PdfObjectScanner.initial, Chunk.empty))
    ) { (result, part) =>
      result.flatMap { case (cursor, boundaries) =>
        PdfObjectScanner.step(config, cursor, part).map { case (next, emitted) =>
          (next, boundaries ++ emitted)
        }
      }
    }

  def spec: Spec[Any, Any] = suite("PdfObjectScanner")(
    test("reports stable absolute boundaries across arbitrary input fragmentation") {
      val pdf = bytes(
        "%PDF-1.7\n" +
          "1 0 obj\n<</Length 6>>\nstream\nendobj\nendstream\nendobj\n" +
          "2 0 obj\n<</Type /Catalog>>\nendobj\n"
      )
      val oneChunk = scan(Chunk.single(pdf))
      val fragmented = scan(
        Chunk.fromIterator(pdf.grouped(3).map(group => Chunk.fromIterable(group)))
      )

      assertTrue(
        oneChunk.isRight,
        fragmented.isRight,
        oneChunk.map(_._2) == fragmented.map(_._2),
        oneChunk.exists { case (_, boundaries) =>
          boundaries.map(_.index.number) == Chunk(1L, 2L) &&
          boundaries.map(_.nextByteOffset).toList == boundaries.map(_.nextByteOffset).toList.sorted
        }
      )
    },
    test("does not treat endobj bytes inside a declared stream payload as a boundary") {
      val prefix = "%PDF-1.7\n1 0 obj\n<</Length 6>>\nstream\n"
      val pdf    = bytes(prefix + "endobj\nendstream\nendobj\n")
      val fakePayloadEnd = prefix.getBytes(ascii).length.toLong + "endobj".getBytes(ascii).length.toLong

      assertTrue(
        scan(Chunk.single(pdf)).exists { case (_, boundaries) =>
          boundaries.length == 1 &&
          boundaries.head.index == Obj.Index(1L, 0) &&
          boundaries.head.nextByteOffset > fakePayloadEnd
        }
      )
    },
    test("fails closed at the configured structural carry limit") {
      val unterminated = bytes("%PDF-1.7\n1 0 obj\n(" + ("a" * 256))
      val result       = scan(
        Chunk.fromIterator(unterminated.grouped(32).map(group => Chunk.fromIterable(group))),
        PdfObjectScanner.Config(maxCarryBytes = 64)
      )

      assertTrue(result.left.exists(_.isInstanceOf[PdfObjectScanner.Error.CarryLimit]))
    },
    test("enforces carry headroom even when the caller supplies one large chunk") {
      val unterminated = bytes("%PDF-1.7\n1 0 obj\n(" + ("a" * 4096))
      val result       = PdfObjectScanner.step(
        PdfObjectScanner.Config(maxCarryBytes = 64),
        PdfObjectScanner.initial,
        unterminated
      )

      assertTrue(
        result.left.exists {
          case PdfObjectScanner.Error.CarryLimit(maxBytes, observedBytes) =>
            maxBytes == 64 && observedBytes == 65L
          case _                                                          => false
        }
      )
    },
    test("keeps object offsets as Long values beyond one tebibyte without allocating the prefix") {
      val base   = 1L << 40
      val suffix = bytes("1 0 obj\n<</Type /Catalog>>\nendobj\n")
      val cursor = new PdfObjectScanner.Cursor(StreamingDecode.initialFinalState.copy(bytesSeen = base))
      val result = PdfObjectScanner.step(PdfObjectScanner.Config.default, cursor, suffix)

      assertTrue(
        result.exists { case (_, boundaries) =>
          boundaries.length == 1 && boundaries.head.nextByteOffset == base + suffix.length.toLong
        }
      )
    },
    test("boundary mode skips content payload events") {
      val pdf    = bytes("1 0 obj\n<</Length 3>>\nstream\nabc\nendstream\nendobj\n")
      val config = StreamingDecode.Config(
        inlineMaxBytes = 0L,
        emitObjectEnds = true,
        maxCarryBytes = Some(1024),
        emitContentEvents = false
      )
      val (events, _) = StreamingDecode.stepChunk(config, StreamingDecode.initialFinalState, pdf)

      assertTrue(
        events.exists(_.isInstanceOf[StreamingDecoded.ObjectEnd]),
        !events.exists(_.isInstanceOf[StreamingDecoded.ContentObjStart]),
        !events.exists(_.isInstanceOf[StreamingDecoded.ContentObjBytes]),
        !events.contains(StreamingDecoded.ContentObjEnd)
      )
    }
  )
}
