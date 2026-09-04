/*
 * Tight Blocks Reader scan vs the ZIO-per-window / ZIO-per-byte lifts.
 *
 *   sbt "bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu us .*ScannerLiftBench.*"
 */

package zio.pdf.bench

import java.io.ByteArrayInputStream
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import zio.blocks.chunk.{Chunk as BlocksChunk}
import zio.blocks.streams.Stream as BlocksStream
import zio.blocks.streams.io.Reader
import zio.stream.ZStream
import zio.{Chunk, Runtime, Unsafe}
import zio.pdf.{BlocksLift, PdfObjectScanner}

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
class ScannerLiftBench {

  private var bytes: Chunk[Byte] = uninitialized
  private var raw: Array[Byte]   = uninitialized
  private val runtime            = Runtime.default
  private val window             = 64 * 1024

  @Setup(Level.Trial)
  def setup(): Unit = {
    val is = getClass.getResourceAsStream("/court-corpus/oknd-general-order-2024-09.pdf")
    require(is != null, "oknd-general-order-2024-09.pdf not on classpath")
    raw = is.readAllBytes()
    is.close()
    bytes = Chunk.fromArray(raw)
  }

  private def asError(error: Throwable): PdfObjectScanner.Error =
    error match {
      case err: PdfObjectScanner.Error => err
      case other =>
        val detail = Option(other.getMessage).filter(_.nonEmpty).getOrElse(other.getClass.getSimpleName)
        PdfObjectScanner.Error.Malformed(detail, other)
    }

  private def run[A](effect: zio.ZIO[Any, Any, A]): A =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(effect).getOrThrowFiberFailure()
    }

  @Benchmark
  def stepWholeChunk: Int =
    PdfObjectScanner.step(PdfObjectScanner.Config.default, PdfObjectScanner.initial, bytes) match {
      case Right((_, found)) => found.length
      case Left(err)         => throw err
    }

  @Benchmark
  def scanInputStream: Int = {
    val reader = Reader.fromInputStream(new ByteArrayInputStream(raw))
    PdfObjectScanner.scan(reader) match {
      case Right(found) => found.length
      case Left(err)    => throw err
    }
  }

  @Benchmark
  def scanReader: Int = {
    val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
    PdfObjectScanner.scan(reader) match {
      case Right(found) => found.length
      case Left(err)    => throw err
    }
  }

  @Benchmark
  def scanSink: Int = {
    val source = BlocksStream.fromChunk(BlocksLift.toBlocksChunk(bytes))
    PdfObjectScanner.scan(source) match {
      case Right(found) => found.length
      case Left(err)    => throw err
    }
  }

  @Benchmark
  def streamWindows: Int = {
    val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
    run(PdfObjectScanner.streamWindows(reader).runCount).toInt
  }

  @Benchmark
  def streamFlatten: Int = {
    val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
    run(PdfObjectScanner.stream(reader).runCollect).length
  }

  @Benchmark
  def oldWindowLift: Int = {
    val windows = BlocksChunk(BlocksLift.toBlocksChunk(bytes))
    val lifted =
      BlocksLift
        .fromReader(Reader.fromChunk(windows), null)
        .mapError(asError)
        .map(BlocksLift.toZioChunk)
    run(PdfObjectScanner.stream(lifted).runCollect).length
  }

  @Benchmark
  def perByteLift: Int = {
    val reader = Reader.fromChunk(BlocksLift.toBlocksChunk(bytes))
    run(
      ZStream
        .repeatZIOOption {
          zio.ZIO
            .attempt {
              val n = reader.readByte()
              if n < 0 then None else Some(n.toByte)
            }
            .foldZIO(
              error => zio.ZIO.fail(Some(error)),
              {
                case None        => zio.ZIO.fail(None)
                case Some(value) => zio.ZIO.succeed(value)
              }
            )
        }
        .runCollect
    ).length
  }
}
