/*
 * JMH benchmarks for handing decoded values across a thread
 * boundary. Compares:
 *
 *   - zio.blocks.ringbuffer.SpscRingBuffer (offer / take)
 *   - java.util.concurrent.ArrayBlockingQueue (offer / poll)
 *
 * The point isn't "ringbuffer is faster" in isolation - the point
 * is that when a decoder's output has to cross a thread boundary
 * the SPSC ringbuffer skips both the BlockingQueue's internal
 * locking *and* its array-bounds-check on every put.
 */

package zio.scodec.stream.bench

import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import org.openjdk.jmh.annotations.*

import zio.blocks.ringbuffer.SpscRingBuffer

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class RingBufferBench {

  /** Number of items per benchmark iteration. Powers of two are
    * required by SpscRingBuffer; ArrayBlockingQueue accepts any. */
  @Param(Array("16384"))
  var n: Int = uninitialized

  private var rb: SpscRingBuffer[Integer]    = uninitialized
  private var abq: ArrayBlockingQueue[Integer] = uninitialized
  private var values: Array[Integer]         = uninitialized

  @Setup(Level.Trial)
  def setup(): Unit = {
    // capacity at least n so we can fill-then-drain in one round
    rb     = SpscRingBuffer.apply[Integer](n * 2)
    abq    = new ArrayBlockingQueue[Integer](n * 2)
    values = Array.tabulate[Integer](n)(i => Integer.valueOf(i))
  }

  /**
   * SPSC: producer thread fills the ring, consumer thread drains
   * it. Modeled as fill-then-drain on a single thread to keep the
   * bench focused on the per-element put/take cost (the actual
   * cross-thread visibility cost is measured under MultipleProducer
   * / Group benches, which JMH doesn't easily support without more
   * setup).
   */
  @Benchmark
  def spscRingBufferFillDrain(): Int = {
    var i = 0
    while (i < n) {
      while (!rb.offer(values(i))) {}
      i += 1
    }
    var sum = 0
    var v   = rb.take()
    while (v != null) {
      sum += v.intValue
      v   = rb.take()
    }
    sum
  }

  @Benchmark
  def arrayBlockingQueueFillDrain(): Int = {
    var i = 0
    while (i < n) {
      abq.offer(values(i))
      i += 1
    }
    var sum = 0
    var v   = abq.poll()
    while (v != null) {
      sum += v.intValue
      v   = abq.poll()
    }
    sum
  }
}
