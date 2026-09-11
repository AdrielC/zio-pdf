package zio.pdf.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.*
import _root_.scodec.bits.BitVector
import zio.pdf.{Prim, XrefStream}

/** Same valid input for before/after comparisons; includes table construction. */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
class XrefStreamBench:
  @Param(Array("100", "10000"))
  var entries: Int = 0
  private var data: Prim.Dict = scala.compiletime.uninitialized
  private var bytes: BitVector = scala.compiletime.uninitialized

  @Setup(Level.Trial)
  def setup(): Unit =
    data = Prim.dict("Size" -> Prim.Number(BigDecimal(entries)), "W" -> Prim.Array.nums(1, 4, 2))
    val payload = new Array[Byte](entries * 7)
    var i = 0
    while i < entries do
      payload(i * 7) = 1
      val offset = i * 100
      var b = 0
      while b < 4 do
        payload(i * 7 + 1 + b) = (offset >>> ((3 - b) * 8)).toByte
        b += 1
      i += 1
    bytes = BitVector(payload)

  @Benchmark
  def decodeTable: Int = XrefStream(data)(bytes).require.tables.head.entries.size
