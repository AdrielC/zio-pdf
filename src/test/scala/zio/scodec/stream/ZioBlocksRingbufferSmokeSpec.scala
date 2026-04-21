/*
 * Smoke test demonstrating that zio-blocks-ringbuffer is wired in,
 * resolvable, and works as a producer/consumer between a streaming
 * decoder and a downstream consumer.
 *
 * The ring buffer doesn't help when there is no thread boundary -
 * the cost of `offer`/`take` is comparable to a `Chunk` allocation.
 * It earns its keep when the producer and the consumer are on
 * different threads (e.g. an I/O thread feeding a CPU-bound parser).
 * This test just proves the API is reachable.
 */

package zio.scodec.stream

import _root_.scodec.bits.*
import _root_.scodec.codecs.uint8
import zio.*
import zio.blocks.ringbuffer.SpscRingBuffer
import zio.stream.*
import zio.test.*

import zio.scodec.stream.syntax.*

object ZioBlocksRingbufferSmokeSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("zio-blocks-ringbuffer smoke")(

    test("offer/take round-trip on SpscRingBuffer") {
      val rb = SpscRingBuffer.apply[Integer](16)
      val offered = (0 until 8).forall(i => rb.offer(Integer.valueOf(i)))
      val drained = ZIO.succeed {
        val out = scala.collection.mutable.ArrayBuffer.empty[Int]
        var v   = rb.take()
        while (v != null) { out += v.intValue; v = rb.take() }
        out.toList
      }
      drained.map(out => assertTrue(offered, out == (0 until 8).toList))
    },

    test("a streaming uint8 decoder hands off through an SpscRingBuffer to a consumer fiber") {
      val payload   = Array.tabulate[Byte](1024)(i => (i & 0xff).toByte)
      val source    = ZStream.fromChunk(Chunk.fromArray(payload))
      val rb        = SpscRingBuffer.apply[Integer](1024)
      val producer  = source
        .decodeMany(uint8)
        .runForeach { v =>
          // Spin until the slot opens; this is a smoke test, not
          // a perf demo, so a tight retry is fine.
          ZIO.succeed {
            while (!rb.offer(Integer.valueOf(v))) {}
          }
        }
        .fork
      val consumer  = ZIO.succeed {
        val out = new Array[Int](1024)
        var i   = 0
        while (i < 1024) {
          val v = rb.take()
          if (v != null) {
            out(i) = v.intValue
            i += 1
          }
        }
        out.toList
      }
      for {
        prod <- producer
        out  <- consumer
        _    <- prod.join
      } yield assertTrue(out == (0 until 1024).map(_ & 0xff).toList)
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(30.seconds)
  )
}
