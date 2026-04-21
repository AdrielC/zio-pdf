/*
 * End-to-end test of the partially-ported PDF top-level decoder
 * driven by the new `zio.scodec.stream.StreamDecoder` (ZChannel-backed).
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.stream.*
import zio.test.*

object PdfTopLevelSpec extends ZIOSpecDefault {

  /** Crafts a tiny but realistic PDF prologue + epilogue. */
  private val pdfPrologue: String =
    """|%PDF-1.7
       |%âãÏÓ
       |% a comment line
       |""".stripMargin

  private val pdfEpilogue: String =
    """|startxref
       |1493726
       |%%EOF
       |""".stripMargin

  private def bytesOf(s: String): Chunk[Byte] =
    Chunk.fromArray(s.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1))

  def spec: Spec[Any, Throwable] = suite("zio.pdf TopLevel pipe")(

    test("parses %PDF-1.7 / binary marker / comment from a Byte ZStream") {
      val source: ZStream[Any, Throwable, Byte] =
        ZStream.fromChunk(bytesOf(pdfPrologue))
      for {
        out <- source.via(TopLevel.pipe).runCollect
      } yield {
        val versions  = out.collect { case TopLevel.VersionT(v)  => v }
        val comments  = out.collect { case TopLevel.CommentT(c)  => c }
        assertTrue(
          versions.size == 1,
          versions(0).major == 1,
          versions(0).minor == 7,
          versions(0).binaryMarker.isDefined,
          comments.exists(c => new String(c.toArray) == " a comment line")
        )
      }
    },

    test("parses startxref / offset / %%EOF") {
      val source = ZStream.fromChunk(bytesOf(pdfEpilogue))
      for {
        out <- source.via(TopLevel.pipe).runCollect
      } yield {
        val sx = out.collect { case TopLevel.StartXrefT(s) => s }
        assertTrue(
          sx.size == 1,
          sx(0).offset == 1493726L
        )
      }
    },

    test("parses prologue + epilogue together, even when fed in tiny chunks") {
      val full = bytesOf(pdfPrologue + pdfEpilogue)
      // Feed one byte at a time - this is what truly exercises
      // the carry-buffer in the StreamDecoder.
      val source: ZStream[Any, Throwable, Byte] =
        ZStream.fromChunk(full).rechunk(1)
      for {
        out <- source.via(TopLevel.pipe).runCollect
      } yield {
        val versions  = out.collect { case TopLevel.VersionT(v)  => v }
        val sx        = out.collect { case TopLevel.StartXrefT(s) => s }
        assertTrue(
          versions.size == 1,
          versions(0).major == 1,
          versions(0).minor == 7,
          sx.size == 1,
          sx(0).offset == 1493726L
        )
      }
    },

    test("Version codec round-trips through scodec strict") {
      val v       = Version.default
      val encoded = Version.codec.encode(v).require
      val decoded = Version.codec.decode(encoded).require
      assertTrue(decoded.value == v, decoded.remainder == BitVector.empty)
    },

    test("StartXref codec round-trips") {
      val sx      = StartXref(424242L)
      val encoded = StartXref.codec.encode(sx).require
      val decoded = StartXref.codec.decode(encoded).require
      assertTrue(decoded.value == sx, decoded.remainder == BitVector.empty)
    },

    test("the legacy xref-stream PDF fixture parses its prologue") {
      // We only need the very first bytes of the legacy fixture to
      // validate that the streaming pipe handles a real PDF header.
      val fixturePath = java.nio.file.Path.of("legacy/src/test/resources/xref-stream.pdf")
      for {
        bytes <- ZIO.attemptBlocking(java.nio.file.Files.readAllBytes(fixturePath))
        out   <- ZStream.fromChunk(Chunk.fromArray(bytes))
                   .via(TopLevel.pipe)
                   .takeWhile {
                     case _: TopLevel.VersionT => true
                     case _: TopLevel.CommentT => true
                     case _: TopLevel.WhitespaceT => true
                     case _ => false
                   }
                   .runCollect
        versions = out.collect { case TopLevel.VersionT(v) => v }
      } yield assertTrue(versions.nonEmpty, versions.head.major == 1)
    }
  )
}
