package zio.pdf.bench

import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations.*
import scala.compiletime.uninitialized
import com.tybera.kyopdf.{ByteLimit as KyoByteLimit, PdfDocument, PdfError}
import kyo.{Abort, Result}
import zio.{Chunk, Runtime, Unsafe}
import zio.pdf.{ByteLimit, PdfEngine}
import zio.stream.ZStream

/** Decoded-graph parity benchmark over the same in-memory court PDF.
  *
  * Run with:
  *   sbt 'bench/Jmh/run .*KyoPdfParityBench.*'
  */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class KyoPdfParityBench:
  @Param(Array(
    "court-corpus/scotus-order-list-2025-05-19.pdf",
    "court-corpus/govinfo-district-court-order.pdf"
  ))
  var fixture: String = uninitialized

  private var bytes: Array[Byte] = uninitialized
  private var zioLimit: ByteLimit = uninitialized
  private var kyoLimit: KyoByteLimit = uninitialized
  private val runtime = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit =
    val input = getClass.getResourceAsStream(s"/$fixture")
    require(input != null, s"$fixture not on classpath")
    try bytes = input.readAllBytes()
    finally input.close()
    zioLimit = ByteLimit.mebibytes(20)
    kyoLimit = Abort.run[PdfError](KyoByteLimit.mebibytes(20)).eval match
      case Result.Success(value) => value
      case result => throw IllegalStateException(s"Invalid benchmark limit: $result")

  @Benchmark
  def kyoDecodedGraph(): Int =
    Abort.run[PdfError](PdfDocument.decode(bytes, kyoLimit)).eval match
      case Result.Success(document) => document.objects.length
      case result => throw IllegalStateException(s"Kyo decode failed: $result")

  @Benchmark
  def zioDecodedGraph(): Int =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe.run(
        PdfEngine.decode(
          ZStream.fromChunk(Chunk.fromArray(bytes)),
          PdfEngine.Options(maxInputBytes = zioLimit.toLong, maxMaterializedDocumentBytes = zioLimit)
        ).runCollect.provide(PdfEngine.live)
      ).getOrThrow().length
    }
