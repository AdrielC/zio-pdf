/*
 * A non-JMH microbench that runs inside the test suite. It compares
 * three ways to decode a large in-memory `BitVector` of `uint8`
 * values:
 *
 *   1. raw `scodec.codecs.vector(uint8).decode(bits)` (the baseline -
 *      a single big strict decode, no streaming overhead at all)
 *   2. the new `StreamDecoder.many(uint8)` (ZChannel-backed)
 *   3. the new `PureDecoder.many(uint8)` (ZPure-backed) routed
 *      through `StreamDecoder.fromPure`
 *
 * The output is written to stdout when the test runs; we don't make
 * the test fail on perf, only on correctness, so this stays useful
 * locally and in CI without becoming flaky.
 */

package zio.scodec.stream

import _root_.scodec.bits.*
import _root_.scodec.codecs.*
import zio.*
import zio.stream.*
import zio.test.*

object PerfBench extends ZIOSpecDefault {

  /** ~4 MiB of uint8 values, fed as a single chunk to keep the
    * comparison apples-to-apples with the baseline strict decode. */
  private val N: Int             = 4 * 1024 * 1024
  private val payload: BitVector = BitVector.view(Array.tabulate[Byte](N)(i => (i & 0xff).toByte))

  /** Same payload, fed in fixed-size 64 KiB chunks - this is the
    * realistic streaming case (and where StreamDecoder/PureDecoder
    * earn their keep over the strict baseline). */
  private val chunkSize: Int                      = 64 * 1024
  private val chunked: ZStream[Any, Nothing, BitVector] =
    ZStream.fromChunk(Chunk.fromArray(payload.toByteArray))
      .grouped(chunkSize)
      .map(c => BitVector.view(c.toArray))

  private def timeMillis(label: String, repeats: Int)(thunk: => Unit): Long = {
    var i = 0
    while (i < 2) { thunk; i += 1 } // warm-up
    val t0 = java.lang.System.nanoTime()
    var j = 0
    while (j < repeats) { thunk; j += 1 }
    val ms = (java.lang.System.nanoTime() - t0) / 1_000_000L / repeats
    println(f"  [$label%-42s] ${ms}%5d ms / iter (avg of $repeats, $N bytes)")
    ms
  }

  private def timeMillisZIO(label: String, repeats: Int)(thunk: => ZIO[Any, Throwable, Any]): ZIO[Any, Throwable, Long] =
    for {
      _  <- ZIO.foreachDiscard(0 until 2)(_ => thunk)
      t0 <- ZIO.succeed(java.lang.System.nanoTime())
      _  <- ZIO.foreachDiscard(0 until repeats)(_ => thunk)
      ms  = (java.lang.System.nanoTime() - t0) / 1_000_000L / repeats.toLong
      _  <- ZIO.succeed(println(f"  [$label%-42s] ${ms}%5d ms / iter (avg of $repeats, $N bytes)"))
    } yield ms

  def spec: Spec[Any, Throwable] = suite("PerfBench")(

    test("decoding ~4 MiB of uint8 (single in-memory decode)") {
      println(s"\n=== single-shot decode of $N bytes (lower is better) ===")

      val baselineMs = timeMillis("scodec.codecs.vector(uint8).decode", 5) {
        val r = vector(uint8).decode(payload).require
        require(r.value.size == N)
      }

      val pureMs = timeMillis("PureDecoder.many(uint8).run.runAll", 5) {
        val (log, _) = PureDecoder.many(uint8).run.runAll(payload)
        require(log.size == N)
      }

      val pureChunkMs = timeMillis("PureDecoder.manyUInt8Chunked (1 log / pass)", 5) {
        val (log, _) = PureDecoder.manyUInt8Chunked.run.runAll(payload)
        require(log.size == 1 && log(0).size == N)
      }

      val sdStrictMs = timeMillis("StreamDecoder.many(uint8).strict.decode", 5) {
        val r = StreamDecoder.many(uint8).strict.decode(payload).require
        require(r.value.size == N)
      }

      println(s"  baseline=$baselineMs ms  pure=$pureMs ms  pureChunk=$pureChunkMs ms  strict-stream=$sdStrictMs ms")
      assertCompletes
    },

    test("decoding ~4 MiB of uint8 (streaming, 64 KiB chunks)") {
      println(s"\n=== streaming decode of $N bytes in $chunkSize-byte chunks (lower is better) ===")

      for {
        sdMs <- timeMillisZIO("StreamDecoder.many(uint8).decode", 5) {
                  StreamDecoder.many(uint8).decode(chunked).runCount
                }
        pureSdMs <- timeMillisZIO("StreamDecoder.fromPure(many(uint8))", 5) {
                      StreamDecoder.fromPure(PureDecoder.many(uint8)).decode(chunked).runCount
                    }
        _ <- ZIO.succeed(println(s"  channel=$sdMs ms  pure-via-channel=$pureSdMs ms"))
      } yield assertCompletes
    } @@ TestAspect.withLiveClock @@ TestAspect.timeout(2.minutes)
  ) @@ TestAspect.sequential
}
