/*
 * Tests for the memory-bounded streaming decoder
 * (PdfStream.streamingDecode).
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.stream.*
import zio.test.*

object StreamingDecodeSpec extends ZIOSpecDefault {

  /** Build a simple PDF whose object 4 has a streaming content of
    * exactly `payloadSize` deterministic bytes. We then encode it
    * with WritePdf so the bytes on the wire match what the
    * streaming decoder will see in the wild. */
  private def buildPdf(payloadSize: Int): ZIO[Any, Throwable, ByteVector] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Kids"  -> Prim.Array(List(Prim.Ref(3, 0))),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array(
          List(
            Prim.Number(BigDecimal(0)),
            Prim.Number(BigDecimal(0)),
            Prim.Number(BigDecimal(612)),
            Prim.Number(BigDecimal(792))
          )
        ),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(
      4,
      Prim.dict(),
      BitVector.view(Array.tabulate[Byte](payloadSize)(i => (i & 0xff).toByte))
    )
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content)
      .via(WritePdf.objects(trailer))
      .runFold(ByteVector.empty)(_ ++ _)
  }

  def spec: Spec[Any, Throwable] = suite("streamingDecode (memory-bounded)")(

    test("emits ContentObjStart (inline for small payload) and a final Meta") {
      for {
        bytes  <- buildPdf(2048)
        events <- ZStream
                    .fromChunk(Chunk.fromArray(bytes.toArray))
                    .via(PdfStream.streamingDecode())
                    .runCollect
      } yield {
        val headers   = events.collect { case h: StreamingDecoded.ContentObjStart => h }
        val ends      = events.collect { case StreamingDecoded.ContentObjEnd      => () }
        val data      = events.collect { case d: StreamingDecoded.DataObj          => d }
        val metas     = events.collect { case m: StreamingDecoded.Meta             => m }
        // 3 data objects + 1 streaming content object + 1 Meta.
        assertTrue(
          headers.size == 1,
          ends.size == 0,
          data.size == 3,
          metas.size == 1,
          // Small payload is inlined on start (2048 <= default inline window).
          headers(0).length == 2048L,
          headers(0).obj.index.number == 4L,
          headers(0).inlinePayload.exists(_.size == 2048L * 8L)
        )
      }
    },

    test("ContentObjStart inline payload is byte-perfect for a 4 KiB stream") {
      val payloadSize = 4096
      for {
        bytes  <- buildPdf(payloadSize)
        events <- ZStream
                    .fromChunk(Chunk.fromArray(bytes.toArray))
                    .via(PdfStream.streamingDecode())
                    .runCollect
        inline = events.collect { case s: StreamingDecoded.ContentObjStart => s.inlinePayload }.flatten.headOption
      } yield {
        val expected = (0 until payloadSize).map(i => (i & 0xff).toByte).toArray
        val got      = inline.map(_.toByteArray).getOrElse(Array.empty[Byte])
        var same     = got.length == expected.length
        var i        = 0
        while (same && i < expected.length) {
          if (got(i) != expected(i)) same = false
          i += 1
        }
        assertTrue(same)
      }
    },

    test("large stream uses ContentObjBytes + End (no inline)") {
      val payloadSize = 512 * 1024
      for {
        bytes  <- buildPdf(payloadSize)
        events <- ZStream
                    .fromChunk(Chunk.fromArray(bytes.toArray))
                    .via(PdfStream.streamingDecode())
                    .runCollect
        starts = events.collect { case s: StreamingDecoded.ContentObjStart => s }
        ends   = events.collect { case StreamingDecoded.ContentObjEnd      => () }
        chunks = events.collect { case StreamingDecoded.ContentObjBytes(c) => c }.flatten
      } yield assertTrue(
        starts.size == 1,
        starts(0).inlinePayload.isEmpty,
        ends.size == 1,
        chunks.size == payloadSize.toLong
      )
    },

    test("decodes a 1 MiB streaming payload without materialising it (memory-bounded)") {
      val size = 1024 * 1024
      for {
        bytes <- buildPdf(size)
        // Track total byte count + chunk count of the payload events,
        // never collecting the chunks themselves.
        stats <- ZStream
                   .fromChunk(Chunk.fromArray(bytes.toArray))
                   .rechunk(64 * 1024)
                   .via(PdfStream.streamingDecode())
                   .runFold((0L, 0L)) { case ((n, total), ev) =>
                     ev match {
                       case StreamingDecoded.ContentObjBytes(c) => (n + 1L, total + c.size.toLong)
                       case _                                    => (n, total)
                     }
                   }
        (chunkCount, totalBytes) = stats
      } yield assertTrue(
        totalBytes == size.toLong,
        // We expect roughly (size / upstream chunk size) chunks for
        // the payload; allow a wide range.
        chunkCount >= 1L,
        chunkCount <= 200L
      )
    },

    test("decodes a 10 MiB streaming payload without materialising it (memory-bounded)") {
      val size = 10 * 1024 * 1024
      for {
        bytes <- buildPdf(size)
        stats <- ZStream
                   .fromChunk(Chunk.fromArray(bytes.toArray))
                   .rechunk(64 * 1024)
                   .via(PdfStream.streamingDecode())
                   .runFold(0L) { (acc, ev) =>
                     ev match {
                       case StreamingDecoded.ContentObjBytes(c) => acc + c.size.toLong
                       case _                                    => acc
                     }
                   }
      } yield assertTrue(stats == size.toLong)
    } @@ TestAspect.timeout(60.seconds),

    test("the streamingDecode pipeline can be hashed straight to a digest (no buffering)") {
      // Above default inline window (256 KiB) so payload arrives as ContentObjBytes.
      val payloadSize = 300 * 1024
      val expectedSha = {
        val md = java.security.MessageDigest.getInstance("SHA-256")
        var i  = 0
        while (i < payloadSize) { md.update((i & 0xff).toByte); i += 1 }
        md.digest()
      }
      for {
        bytes  <- buildPdf(payloadSize)
        digest <- ZStream
                    .fromChunk(Chunk.fromArray(bytes.toArray))
                    .via(PdfStream.streamingDecode())
                    .collect { case StreamingDecoded.ContentObjBytes(c) => c }
                    .runFold(java.security.MessageDigest.getInstance("SHA-256")) { (md, c) =>
                      md.update(c.toArray); md
                    }
                    .map(_.digest())
      } yield {
        var same = digest.length == expectedSha.length
        var i    = 0
        while (same && i < digest.length) {
          if (digest(i) != expectedSha(i)) same = false
          i += 1
        }
        assertTrue(same)
      }
    } @@ TestAspect.timeout(60.seconds)
  )
}
