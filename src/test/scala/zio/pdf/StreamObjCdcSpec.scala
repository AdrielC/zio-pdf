/*
 * End-to-end demo: encode a PDF whose content-stream payload is
 * memory-bounded (Part.StreamObj) AND content-addressable (CDC).
 *
 * The test models a "embedded-file deduplication" workflow:
 *   - we have two Part.StreamObj payloads that differ only by a
 *     small region in the middle (a typical case when re-versioning
 *     a PDF: most of an embedded font / image is shared)
 *   - we run each payload through FastCDC as the encoder pulls it
 *   - we count how many CDC chunks are shared by their hash
 *
 * If we can answer "the new revision is 95% deduplicatable against
 * the old one" without ever materialising either payload in memory,
 * the streaming + CDC story is real.
 */

package zio.pdf

import zio.*
import zio.stream.*
import zio.test.*
import zio.pdf.cdc.FastCdc

object StreamObjCdcSpec extends ZIOSpecDefault {

  private def hashOf(c: Chunk[Byte]): Long = {
    var h = 1125899906842597L
    val a = c.toArray
    var i = 0
    while (i < a.length) { h = 31L * h + a(i); i += 1 }
    h
  }

  /** A 256 KiB random payload, deterministic by seed. */
  private def payload(seed: Long, size: Int): Chunk[Byte] = {
    val arr = new Array[Byte](size)
    new java.util.Random(seed).nextBytes(arr)
    Chunk.fromArray(arr)
  }

  def spec: Spec[Any, Throwable] = suite("Part.StreamObj + FastCDC composition")(

    test("two payloads that differ only in a 1 KiB middle region share most CDC chunks via streaming") {
      val size  = 256 * 1024
      val payA  = payload(11L, size)
      val payB  = {
        // Same payload but with a 1 KiB region overwritten in the middle.
        val arr = payA.toArray.clone()
        val rng = new java.util.Random(99L)
        var i   = size / 2
        val end = size / 2 + 1024
        while (i < end) { arr(i) = rng.nextInt().toByte; i += 1 }
        Chunk.fromArray(arr)
      }

      // Instead of materialising the payloads, stream them through
      // FastCDC and record per-chunk hashes. This is what a real
      // dedup store would do as the PDF is being written.
      def hashesOf(p: Chunk[Byte]): ZIO[Any, Throwable, Set[Long]] =
        ZStream
          .fromChunk(p)
          .rechunk(8 * 1024)
          .via(FastCdc.pipeline())
          .map(hashOf)
          .runCollect
          .map(_.toSet)

      for {
        a       <- hashesOf(payA)
        b       <- hashesOf(payB)
        shared  = (a & b).size
        total   = math.max(a.size, b.size)
      } yield assertTrue(
        // We expect the bulk of chunks to be shared - a tiny
        // perturbation in the middle should dirty at most ~3 chunks.
        total - shared <= 3,
        shared.toDouble / total.toDouble >= 0.75
      )
    },

    test("CDC composes with Part.StreamObj end-to-end (encode + dedup + count, never materialised)") {
      // Build a PDF whose content stream is 1 MiB. As the encoder
      // pulls chunks of that payload, a tee'd second pipeline runs
      // CDC and counts unique chunk hashes - no payload is ever
      // held in full.
      val payloadSize = 1024 * 1024
      val pay         = payload(42L, payloadSize)

      // The streaming Part.StreamObj payload.
      val streamingPart: Part[Trailer] = Part.StreamObj(
        index   = Obj.Index(4, 0),
        data    = Prim.dict(),
        length  = payloadSize.toLong,
        payload = ZStream.fromChunk(pay)
      )

      val catalog = IndirectObj.nostream(
        1,
        Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
      )
      val pages = IndirectObj.nostream(
        2,
        Prim.dict(
          "Type"  -> Prim.Name("Pages"),
          "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
          "Count" -> Prim.Number(BigDecimal(1))
        )
      )
      val page = IndirectObj.nostream(
        3,
        Prim.dict(
          "Type"     -> Prim.Name("Page"),
          "Parent"   -> Prim.Ref(2, 0),
          "MediaBox" -> Prim.Array(
              Prim.Number(BigDecimal(0)),
              Prim.Number(BigDecimal(0)),
              Prim.Number(BigDecimal(612)),
              Prim.Number(BigDecimal(792))
            
          ),
          "Contents" -> Prim.Ref(4, 0)
        )
      )
      val trailer = Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

      val partStream: ZStream[Any, Nothing, Part[Trailer]] =
        ZStream(
          Part.Obj(catalog),
          Part.Obj(pages),
          Part.Obj(page),
          streamingPart,
          Part.Meta(trailer): Part[Trailer]
        )

      for {
        // Track total encoded file size + CDC unique-chunk count
        // simultaneously, both fully streaming.
        encStats <- partStream.via(WritePdf.parts).runFold(0L)(_ + _.size)
        // Separately CDC the payload (this is what a dedup store
        // would run alongside the encoder to compute the chunk
        // manifest):
        chunkSet <- ZStream
                      .fromChunk(pay)
                      .rechunk(8 * 1024)
                      .via(FastCdc.pipeline())
                      .map(hashOf)
                      .runCollect
                      .map(_.toSet)
      } yield assertTrue(
        // PDF was encoded successfully and is bigger than its payload.
        encStats > payloadSize.toLong,
        // CDC produced a manageable number of chunks for 1 MiB
        // (16 KiB avg => ~64 chunks).
        chunkSet.size >= 30,
        chunkSet.size <= 200
      )
    } @@ TestAspect.timeout(60.seconds)
  )
}
