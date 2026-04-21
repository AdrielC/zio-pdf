/*
 * Head-to-head: zio-pdf's StreamDecoder + chunked fast path
 * vs fs2 + the (folded-into-fs2) scodec-stream interop.
 *
 * Same scodec.Decoder. Same in-memory byte input. Same chunk size
 * for the streaming path. The only thing that differs is the
 * library doing the streaming.
 *
 * Run with:
 *
 *   sbt 'benchFs2/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu ms .*HeadToHeadBench.*'
 */

package zio.pdf.bench.fs2

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import _root_.scodec.codecs.uint8

import _root_.cats.effect.IO
import _root_.cats.effect.unsafe.implicits.global as ceGlobal
import _root_.fs2.{Chunk as FsChunk, Stream as FsStream}
import _root_.fs2.interop.scodec.{StreamDecoder as FsStreamDecoder}

import _root_.zio.{Chunk, Runtime, Unsafe}
import _root_.zio.stream.ZStream
import _root_.zio.scodec.stream.{PureDecoder, StreamDecoder => ZStreamDecoder}
import _root_.zio.scodec.stream.syntax.*

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class HeadToHeadBench {

  /** ~4 MiB of bytes - big enough to amortise pipeline setup
    * cost, small enough to keep iteration time around a second. */
  @Param(Array("4194304"))
  var n: Int = uninitialized

  /** Upstream chunk size (both libraries get the same). */
  @Param(Array("65536"))
  var chunkSize: Int = uninitialized

  private var bytes: Array[Byte]                 = uninitialized

  // Chunked input as the two libraries' native chunk types.
  private var zioChunked: ZStream[Any, Throwable, _root_.scodec.bits.BitVector] = uninitialized
  private var fs2Chunked: FsStream[IO, _root_.scodec.bits.BitVector]            = uninitialized

  // Pre-built decoders.
  private val zioMany   = ZStreamDecoder.many(uint8)
  private val zioFast   = ZStreamDecoder.fromPureChunked(
    PureDecoder.manyChunked[Int] { arr =>
      val out = new Array[Int](arr.length)
      var i   = 0
      while (i < arr.length) { out(i) = arr(i) & 0xff; i += 1 }
      Chunk.fromArray(out)
    }
  )
  private val fsMany    = FsStreamDecoder.many(uint8)

  private val zioRuntime = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    bytes = Array.tabulate(n)(i => (i & 0xff).toByte)

    val zioBytes = Chunk.fromArray(bytes)
    zioChunked = ZStream
      .fromChunk(zioBytes)
      .grouped(chunkSize)
      .map(c => _root_.scodec.bits.BitVector.view(c.toArray))

    val fsBytes = FsChunk.array(bytes)
    fs2Chunked = FsStream
      .chunk(fsBytes)
      .covary[IO]
      .chunkN(chunkSize)
      .map(c => _root_.scodec.bits.BitVector.view(c.toArray))
  }

  // --------------------------------------------------------------
  // Streaming uint8 decode - apples to apples
  // --------------------------------------------------------------

  @Benchmark
  def fs2_StreamDecoder_many: Long =
    fs2Chunked
      .through(fsMany.toPipe)
      .compile
      .count
      .unsafeRunSync()

  @Benchmark
  def zio_StreamDecoder_many: Long =
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(zioChunked.viaDecoder(zioMany).runCount).getOrThrow()
    }

  @Benchmark
  def zio_StreamDecoder_fromPureChunked: Long =
    Unsafe.unsafe { implicit u =>
      zioRuntime.unsafe.run(zioChunked.viaDecoder(zioFast).runCount).getOrThrow()
    }

  // --------------------------------------------------------------
  // Reference baselines: a single-shot strict decode (no streaming
  // overhead, no IO runtime) - the ceiling either streaming
  // library could possibly approach.
  // --------------------------------------------------------------

  @Benchmark
  def baseline_scodec_vector: Int =
    _root_.scodec.codecs
      .vector(uint8)
      .decode(_root_.scodec.bits.BitVector.view(bytes))
      .require
      .value
      .size

  @Benchmark
  def baseline_zio_PureDecoder_runAll: Int =
    PureDecoder
      .many(uint8)
      .run
      .runAll(_root_.scodec.bits.BitVector.view(bytes))
      ._1
      .size
}
