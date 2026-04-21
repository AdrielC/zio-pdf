/*
 * JMH throughput benchmark for FastCDC. Measures chunks-per-MB
 * and MB/s for chunking 32 MiB of random bytes through the
 * `FastCdc.pipeline` ZPipeline.
 *
 * Run with:
 *
 *   sbt 'bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm thrpt -tu s .*FastCdcBench.*'
 */

package zio.pdf.cdc.bench

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import kyo.{AllowUnsafe, Emit, Sync}
import zio.{Chunk, Runtime, Unsafe}
import zio.stream.ZStream

import zio.pdf.cdc.{FastCdc, FastCdcKyo}

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class FastCdcBench {

  /** 32 MiB of pseudo-random bytes - big enough to amortise
    * pipeline setup cost, small enough to keep iteration time
    * around a second. */
  @Param(Array("33554432"))
  var size: Int = uninitialized

  /** Upstream chunk size before CDC. */
  @Param(Array("65536"))
  var rechunk: Int = uninitialized

  private var bytes: Chunk[Byte] = uninitialized

  /** Same bytes as `bytes`; JMH iteration uses this for Kyo to avoid `Chunk.toArray` per run. */
  private var rawBytes: Array[Byte] = uninitialized

  private val runtime = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    val arr = new Array[Byte](size)
    new java.util.Random(0xCAFEBABEL).nextBytes(arr)
    rawBytes = arr
    bytes = Chunk.fromArray(arr)
  }

  @Benchmark
  def cdcThroughput: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          ZStream
            .fromChunk(bytes)
            .rechunk(rechunk)
            .via(FastCdc.pipeline())
            .runCount
        )
        .getOrThrow()
    }

  @Benchmark
  def cdcThroughputKyoEmit: Long = {
    given AllowUnsafe = AllowUnsafe.embrace.danger
    Sync.Unsafe.evalOrThrow {
      Emit
        .runFold(0L)((n: Long, _: Array[Byte]) => n + 1L)(
          FastCdcKyo.emitChunkedFromArray(rawBytes, rechunk, FastCdc.defaultConfig)
        )
        .map(_._1)
    }
  }

  @Benchmark
  def cdcThroughputCount: Long = {
    // Same as above but count both chunks and total bytes.
    Unsafe.unsafe { implicit u =>
      val r = runtime.unsafe
        .run(
          ZStream
            .fromChunk(bytes)
            .rechunk(rechunk)
            .via(FastCdc.pipeline())
            .runFold((0L, 0L)) { case ((n, t), c) => (n + 1L, t + c.size.toLong) }
        )
        .getOrThrow()
      r._1 + r._2
    }
  }
}
