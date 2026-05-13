/*
 * JMH benchmarks for the streaming decoder layers.
 *
 * Run from sbt:
 *
 *   sbt "bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 .*StreamDecoderBench.*"
 *
 * Or for a quick smoke run:
 *
 *   sbt "bench/Jmh/run -i 2 -wi 1 -f 1 -t 1 .*StreamDecoderBench.*"
 *
 * The benchmarks decode a fixed in-memory `BitVector` (or stream
 * thereof) of N `uint8` values, comparing every layer of the API:
 *
 *   - scodec.codecs.vector(uint8).decode  (strict baseline)
 *   - PureDecoder.many(uint8).runAllNormalized  (ZPure layer, flat log)
 *   - StreamDecoder.many(uint8).strict    (pure interpreter over Step)
 *   - StreamDecoder.many(uint8) over a chunked ZStream (ZChannel)
 *   - StreamDecoder.fromPure(many(uint8)) over a chunked ZStream (hybrid)
 */

package zio.scodec.stream.bench

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.{uint8, vector}

import zio.{Chunk, Runtime, Unsafe}
import zio.stream.ZStream

import zio.scodec.stream.{PureDecoder, StreamDecoder}
import zio.scodec.stream.syntax.*

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime, Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class StreamDecoderBench {

  /** ~1 MiB of uint8 values. Big enough to amortise streaming
    * overhead, small enough to keep iteration time low. */
  @Param(Array("1048576"))
  var n: Int = uninitialized

  /** Chunk size used for the streaming variants. */
  @Param(Array("65536"))
  var chunkSize: Int = uninitialized

  private var bits: BitVector                                 = uninitialized
  private var byteStream: ZStream[Any, Throwable, BitVector]   = uninitialized

  // Pre-built decoders; constructing them is not part of any
  // benchmark's hot loop.
  private val baseline    = vector(uint8)
  private val pureMany    = PureDecoder.many(uint8)
  private val streamMany  = StreamDecoder.many(uint8)
  private val hybridMany  = StreamDecoder.fromPure(PureDecoder.many(uint8))
  private val streamMany2 = uint8.streamMany           // via syntax, identity-equal at runtime

  // The byte-aligned, fixed-width fast path: read the whole batch
  // of bytes into an Int chunk per ZPure step, then ship one log
  // entry per upstream chunk. This is what `StreamDecoder.fromPureChunked`
  // uses to amortise per-element ZPure / ZChannel overhead.
  private val uint8BatchPure: PureDecoder[Chunk[Int]] =
    PureDecoder.manyChunked[Int] { arr =>
      val out = new Array[Int](arr.length)
      var i   = 0
      while (i < arr.length) { out(i) = arr(i) & 0xff; i += 1 }
      Chunk.fromArray(out)
    }
  private val streamMany3 = StreamDecoder.fromPureChunked(uint8BatchPure)

  private val runtime = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    val arr = Array.tabulate[Byte](n)(i => (i & 0xff).toByte)
    bits = BitVector.view(arr)

    val chunk = Chunk.fromArray(arr)
    byteStream = ZStream
      .fromChunk(chunk)
      .grouped(chunkSize)
      .map(c => BitVector.view(c.toArray))
  }

  // -------------------------------------------------------------------
  // strict (no streaming)
  // -------------------------------------------------------------------

  @Benchmark
  def scodecVectorBaseline: Vector[Int] =
    baseline.decode(bits).require.value

  @Benchmark
  def pureDecoderRunAll: Chunk[Int] =
    pureMany.runAllNormalized(bits)._1

  @Benchmark
  def streamDecoderStrict: Chunk[Int] =
    streamMany.strict.decode(bits).require.value

  @Benchmark
  def syntaxStreamDecoderStrict: Chunk[Int] =
    streamMany2.strict.decode(bits).require.value

  // -------------------------------------------------------------------
  // streaming via ZChannel (full pipeline)
  // -------------------------------------------------------------------

  @Benchmark
  def streamDecoderChannel: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(byteStream.viaDecoder(streamMany).runCount).getOrThrow()
    }

  @Benchmark
  def streamDecoderHybrid: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(byteStream.viaDecoder(hybridMany).runCount).getOrThrow()
    }

  // -------------------------------------------------------------------
  // The byte-aligned batched fast path - the actual point of this
  // exercise. Should beat scodec.codecs.vector both strictly and
  // streaming, because it skips per-element decoder dispatch *and*
  // per-element ZPure.log overhead.
  // -------------------------------------------------------------------

  @Benchmark
  def chunkedFastPathStrict: Chunk[Int] = {
    val (log, _) = uint8BatchPure.run.runAll(bits)
    if (log.size == 1) log(0) else log.flatten
  }

  @Benchmark
  def chunkedFastPathChannel: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(byteStream.viaDecoder(streamMany3).runCount).getOrThrow()
    }
}
