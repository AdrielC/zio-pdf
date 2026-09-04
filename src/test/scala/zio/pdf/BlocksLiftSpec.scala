package zio.pdf

import java.nio.charset.StandardCharsets

import zio.blocks.chunk.{Chunk as BlocksChunk}
import zio.blocks.streams.Stream as BlocksStream
import zio.blocks.streams.io.{Reader, Writer}
import zio.stream.ZStream
import zio.test.*
import zio.Chunk

object BlocksLiftSpec extends ZIOSpecDefault {

  private val ascii = StandardCharsets.US_ASCII

  private def pdfBytes: Chunk[Byte] =
    Chunk.fromArray(
      ("%PDF-1.7\n" +
        "1 0 obj\n<</Length 6>>\nstream\nendobj\nendstream\nendobj\n" +
        "2 0 obj\n<</Type /Catalog>>\nendobj\n").getBytes(ascii)
    )

  def spec: Spec[Any, Any] = suite("BlocksLift")(
    test("fromReader pulls a Blocks Reader into a ZStream") {
      val reader = Reader.fromChunk(BlocksChunk(1, 2, 3, 4))
      BlocksLift.fromReader(reader, -1).runCollect.map { values =>
        assertTrue(values == Chunk(1, 2, 3, 4))
      }
    },
    test("fromStream compiles a Blocks Stream and lifts it through ZChannel") {
      val source = BlocksStream.fromChunk(BlocksChunk("a", "b", "c"))
      BlocksLift.fromStream(source, null).runCollect.map { values =>
        assertTrue(values == Chunk("a", "b", "c"))
      }
    },
    test("toWriter pushes ZStream elements into a Blocks Writer") {
      val out = scala.collection.mutable.ArrayBuffer.empty[String]
      val writer = new Writer[String] {
        private var closed = false
        def isClosed: Boolean = closed
        def write(value: String): Boolean =
          if closed then false
          else
            out += value
            true
        def close(): Unit = closed = true
      }
      ZStream("x", "y")
        .run(BlocksLift.toWriter(writer))
        .as(assertTrue(out.toList == List("x", "y"), writer.isClosed))
    },
    test("Options load from Blocks Config into a Context") {
      val loaded = BlocksLift.Options.fromMap(Map("mailboxCapacity" -> "16"))
      val ctx    = loaded.map(BlocksLift.Options.context(_))
      assertTrue(
        loaded == Right(BlocksLift.Options(16)),
        ctx.exists(_.get[BlocksLift.Options].mailboxCapacity == 16)
      )
    },
    test("MpscMailbox offers and polls on the sequential JS-safe path") {
      val mailbox = BlocksLift.MpscMailbox[String](BlocksLift.Options.context())
      for {
        _      <- mailbox.offerZIO("one")
        _      <- mailbox.offerZIO("two")
        first  <- mailbox.pollZIO
        second <- mailbox.pollZIO
        empty  <- mailbox.pollZIO
      } yield assertTrue(first.contains("one"), second.contains("two"), empty.isEmpty)
    },
    test("PdfObjectScanner.stream matches step() through a Blocks Reader") {
      val windows = BlocksChunk(BlocksLift.toBlocksChunk(pdfBytes))
      val oneChunk = PdfObjectScanner.step(PdfObjectScanner.Config.default, PdfObjectScanner.initial, pdfBytes)
      PdfObjectScanner.stream(Reader.fromChunk(windows)).runCollect.map { streamed =>
        assertTrue(
          oneChunk.exists { case (_, boundaries) =>
            streamed.map(_.index.number) == boundaries.map(_.index.number) &&
            streamed.map(_.index.number) == Chunk(1L, 2L)
          }
        )
      }
    }
  )
}
