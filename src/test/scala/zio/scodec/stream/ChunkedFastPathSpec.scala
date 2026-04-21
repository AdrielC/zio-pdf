/*
 * Tests for the byte-aligned batched fast path
 * (PureDecoder.manyChunked + StreamDecoder.fromPureChunked).
 */

package zio.scodec.stream

import _root_.scodec.bits.*
import _root_.scodec.codecs.uint8
import zio.*
import zio.stream.*
import zio.test.*

import zio.scodec.stream.syntax.*

object ChunkedFastPathSpec extends ZIOSpecDefault {

  /** uint8 fast path - reads each byte directly into an `Int`,
    * skipping the per-element scodec decoder entirely. */
  inline def uint8Batch: PureDecoder[Chunk[Int]] =
    PureDecoder.manyChunked[Int] { arr =>
      val out = new Array[Int](arr.length)
      var i   = 0
      while (i < arr.length) { out(i) = arr(i) & 0xff; i += 1 }
      Chunk.fromArray(out)
    }

  def spec: Spec[Any, Throwable] = suite("byte-aligned batched fast path")(

    test("PureDecoder.manyChunked emits one batch per runAll") {
      val bits          = BitVector.view(Array.tabulate[Byte](16)(i => i.toByte))
      val (log, result) = uint8Batch.run.runAll(bits)
      assertTrue(
        log.size == 1,
        log(0) == Chunk.fromArray((0 until 16).toArray),
        result.map(_._1) == Right(BitVector.empty),
        result.map(_._2) == Right(PureDecoder.Status.NeedMore)
      )
    },

    test("StreamDecoder.fromPureChunked flattens batches into the downstream stream") {
      val bytes  = Chunk.fromArray(Array.tabulate[Byte](64)(i => i.toByte))
      val source = ZStream.fromChunk(bytes).grouped(16).map(c => BitVector.view(c.toArray))
      for {
        out <- source.viaDecoder(uint8Batch.toStreamChunked).runCollect
      } yield assertTrue(out.toList == (0 until 64).toList)
    },

    test("the chunked fast path is observably equivalent to many(uint8)") {
      val bytes  = Array.tabulate[Byte](256)(i => i.toByte)
      val source = ZStream.fromChunk(Chunk.fromArray(bytes)).grouped(32).map(c => BitVector.view(c.toArray))
      for {
        fast <- source.viaDecoder(uint8Batch.toStreamChunked).runCollect
        slow <- source.decodeManyBits(uint8).runCollect
      } yield assertTrue(fast == slow, fast.size == 256)
    },

    test("strict decoding via the chunked path returns one big chunk") {
      val bits = BitVector.view(Array.tabulate[Byte](100)(i => (i * 3).toByte))
      val r    = bits.decodeStrict(StreamDecoder.fromPureChunked(uint8Batch))
      assertTrue(
        r match {
          case Right(dr) => dr.value.size == 100 && dr.remainder.isEmpty
          case _         => false
        }
      )
    }
  )
}
