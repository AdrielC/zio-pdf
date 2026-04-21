/*
 * End-to-end decoder test: read one of the legacy PDF fixtures
 * through PdfStream.decode and assert what comes out.
 */

package zio.pdf

import java.nio.file.{Files, Path}

import zio.*
import zio.stream.*
import zio.test.*

object DecodeSpec extends ZIOSpecDefault {

  private def loadFixture(name: String): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.attemptBlocking(Chunk.fromArray(Files.readAllBytes(Path.of(s"src/test/resources/$name"))))

  def spec: Spec[Any, Throwable] = suite("PdfStream.decode")(

    test("decodes the legacy xref-stream.pdf fixture into Decoded values") {
      for {
        bytes <- loadFixture("xref-stream.pdf")
        out   <- ZStream.fromChunk(bytes).via(PdfStream.decode(Log.noop)).runCollect
      } yield {
        val versions = out.collect { case Decoded.Meta(_, _, Some(v)) => v }
        val data     = out.collect { case Decoded.DataObj(o) => o }
        val content  = out.collect { case Decoded.ContentObj(o, _, _) => o }
        val metas    = out.collect { case m: Decoded.Meta => m }
        assertTrue(
          versions.nonEmpty,
          versions.head.major == 1,
          // The fixture is a tiny PDF with at least one indirect object.
          (data.size + content.size) >= 1,
          // Exactly one Meta is emitted at end-of-stream.
          metas.size == 1
        )
      }
    },

    test("decodes the legacy empty-kids.pdf fixture without errors") {
      for {
        bytes <- loadFixture("empty-kids.pdf")
        out   <- ZStream.fromChunk(bytes).via(PdfStream.decode(Log.noop)).runCollect
      } yield {
        val metas = out.collect { case m: Decoded.Meta => m }
        val objs  = out.collect {
          case Decoded.DataObj(_)        => 1
          case Decoded.ContentObj(_, _, _) => 1
        }
        assertTrue(
          // We should have decoded at least a handful of objects.
          objs.size >= 1,
          // And exactly one terminating Meta.
          metas.size == 1
        )
      }
    },

    test("decodes the same fixture even when fed in 64 byte chunks") {
      for {
        bytes  <- loadFixture("xref-stream.pdf")
        large  <- ZStream.fromChunk(bytes).via(PdfStream.decode(Log.noop)).runCollect
        small  <- ZStream.fromChunk(bytes).rechunk(64).via(PdfStream.decode(Log.noop)).runCollect
      } yield {
        // Same number of objects either way.
        assertTrue(large.size == small.size)
      }
    }
  )
}
