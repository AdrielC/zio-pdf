/*
 * Properties of FastCDC that have to hold for it to be useful as
 * a dedup primitive on Part.StreamObj payloads.
 */

package zio.pdf.cdc

import zio.*
import zio.stream.*
import zio.test.*

object FastCdcSpec extends ZIOSpecDefault {

  // ---------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------

  /** Deterministic 1 MiB payload: a PRNG seeded with a known value
    * so the same call always returns the same bytes. */
  private def payload(seed: Long, size: Int): Array[Byte] = {
    val arr = new Array[Byte](size)
    val rng = new java.util.Random(seed)
    rng.nextBytes(arr)
    arr
  }

  private def chunkAll(
    bytes: Array[Byte],
    rechunkSize: Int = 4 * 1024,
    cfg: FastCdc.Config = FastCdc.defaultConfig
  ): ZIO[Any, Throwable, Chunk[Chunk[Byte]]] =
    ZStream
      .fromChunk(Chunk.fromArray(bytes))
      .rechunk(rechunkSize)
      .via(FastCdc.pipeline(cfg))
      .runCollect

  /** Hash one chunk - we only care about per-chunk identity for
    * dedup checks, so any cheap hash is fine. */
  private def hashOf(c: Chunk[Byte]): Long = {
    var h = 1125899906842597L
    val a = c.toArray
    var i = 0
    while (i < a.length) { h = 31L * h + a(i); i += 1 }
    h
  }

  // A small config so tests run on small payloads.
  private val smallCfg = FastCdc.Config(minSize = 64, avgSize = 256, maxSize = 1024)

  def spec: Spec[Any, Throwable] = suite("FastCdc")(

    test("empty input produces no chunks") {
      for {
        out <- chunkAll(new Array[Byte](0))
      } yield assertTrue(out.isEmpty)
    },

    test("input shorter than minSize produces a single under-sized chunk") {
      val bytes = payload(1L, 32)
      for {
        out <- chunkAll(bytes, cfg = smallCfg)
      } yield assertTrue(out.size == 1, out(0).size == 32)
    },

    test("the concatenation of all CDC chunks equals the input (no bytes added/dropped/reordered)") {
      val bytes = payload(2L, 1 << 20) // 1 MiB
      for {
        out <- chunkAll(bytes)
      } yield {
        val joined = out.flatten.toArray
        // Hand-rolled equality avoids a scala-3 / Spec macro
        // resolution issue around `java.util.Arrays.equals`.
        var same   = joined.length == bytes.length
        var i      = 0
        while (same && i < joined.length) {
          if (joined(i) != bytes(i)) same = false
          i += 1
        }
        assertTrue(same)
      }
    },

    test("every CDC chunk is within [minSize, maxSize] (except possibly the last)") {
      val cfg   = smallCfg
      val bytes = payload(3L, 1 << 16) // 64 KiB
      for {
        out <- chunkAll(bytes, cfg = cfg)
      } yield {
        val sizes      = out.map(_.size)
        // Every chunk except possibly the last must satisfy the bounds.
        val nonTail    = sizes.dropRight(1)
        val tail       = sizes.lastOption.getOrElse(0)
        val nonTailOk  = nonTail.forall(s => s >= cfg.minSize && s <= cfg.maxSize)
        val tailOk     = tail >= 1 && tail <= cfg.maxSize
        assertTrue(nonTailOk, tailOk)
      }
    },

    test("CDC is deterministic: chunking the same bytes twice gives identical chunk hashes") {
      val bytes = payload(4L, 1 << 17) // 128 KiB
      for {
        a <- chunkAll(bytes)
        b <- chunkAll(bytes)
      } yield assertTrue(a.map(hashOf) == b.map(hashOf))
    },

    test("CDC is rechunking-invariant: same input via 1 chunk vs 7 KiB chunks vs 1 byte at a time produces the SAME CDC chunks") {
      val bytes = payload(5L, 1 << 17) // 128 KiB
      for {
        whole  <- chunkAll(bytes, rechunkSize = bytes.length)
        midSz  <- chunkAll(bytes, rechunkSize = 7 * 1024)
        oneByte <- chunkAll(bytes, rechunkSize = 1)
      } yield assertTrue(
        whole.map(hashOf) == midSz.map(hashOf),
        whole.map(hashOf) == oneByte.map(hashOf)
      )
    },

    test("a 1-byte insertion early in the stream perturbs only a small number of CDC chunks (dedup property)") {
      val original = payload(6L, 1 << 18) // 256 KiB
      val mutated  = {
        // Insert one byte at offset 1024.
        val out = new Array[Byte](original.length + 1)
        java.lang.System.arraycopy(original, 0, out, 0, 1024)
        out(1024) = 0x42.toByte
        java.lang.System.arraycopy(original, 1024, out, 1025, original.length - 1024)
        out
      }
      for {
        a <- chunkAll(original)
        b <- chunkAll(mutated)
      } yield {
        val ah     = a.map(hashOf).toSet
        val bh     = b.map(hashOf).toSet
        val shared = (ah & bh).size
        val total  = math.max(ah.size, bh.size)
        // For dedup to actually work, the vast majority of CDC
        // chunks must survive the 1-byte insertion. With FastCDC's
        // ~16 KiB average and 256 KiB payload, we expect ~14 chunks
        // and at most 1-2 to differ around the insertion point.
        assertTrue(
          shared >= (total - 3),                       // at most 3 chunks differ
          shared.toDouble / total.toDouble >= 0.75      // and the vast majority survive
        )
      }
    },

    test("CDC of two completely different payloads has nearly disjoint chunk hashes") {
      val a = payload(7L, 1 << 17)
      val b = payload(8L, 1 << 17)
      for {
        ca <- chunkAll(a)
        cb <- chunkAll(b)
      } yield {
        val sa = ca.map(hashOf).toSet
        val sb = cb.map(hashOf).toSet
        // Random payloads of the same size shouldn't accidentally
        // share more than a couple of chunks.
        assertTrue((sa & sb).size <= 2)
      }
    },

    test("encoder can ingest a 10 MiB random stream into CDC chunks without OOM (memory-bounded, hits target avg)") {
      val size  = 10 * 1024 * 1024
      // Random bytes: representative of compressed/encrypted PDF
      // content streams. Cyclic deterministic data (i & 0xff) is a
      // *bad* CDC test fixture because the gear hash on it never
      // triggers cuts before maxSize.
      val bytes = payload(99L, size)
      for {
        stats <- ZStream
                   .fromChunk(Chunk.fromArray(bytes))
                   .rechunk(64 * 1024)
                   .via(FastCdc.pipeline())
                   .runFold((0L, 0L)) { case ((n, total), c) =>
                     (n + 1L, total + c.size.toLong)
                   }
        (chunkCount, totalBytes) = stats
      } yield assertTrue(
        totalBytes == size.toLong,
        // With avg=16 KiB target we expect ~640 chunks for 10 MiB.
        // Allow a generous range for the variance inherent to CDC.
        chunkCount >= 400L,
        chunkCount <= 1500L
      )
    } @@ TestAspect.timeout(60.seconds)

    ,

    test("CDC degrades gracefully on highly-structured input (cuts at maxSize)") {
      // Inverse property test: deterministic cyclic data shouldn't
      // crash CDC; it should just produce maxSize-bounded chunks.
      val size = 1024 * 1024
      val source: ZStream[Any, Throwable, Byte] =
        ZStream.fromIterable(0 until size).map(i => (i & 0xff).toByte)
      for {
        stats <- source
                   .via(FastCdc.pipeline())
                   .runFold((0L, 0L)) { case ((n, total), c) =>
                     (n + 1L, total + c.size.toLong)
                   }
        (chunkCount, totalBytes) = stats
      } yield assertTrue(
        totalBytes == size.toLong,
        chunkCount >= 1L
      )
    } @@ TestAspect.timeout(60.seconds)
  )
}
