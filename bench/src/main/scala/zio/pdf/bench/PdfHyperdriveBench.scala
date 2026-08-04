/*
 * PdfEngine fused path vs streaming PdfStream on xref-stream.pdf.
 *
 *   sbt "bench/Jmh/run -i 10 -wi 5 .*PdfHyperdriveBench.*"
 */

package zio.pdf.bench

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import zio.{Runtime, Unsafe}
import zio.pdf.{PdfEngine, PdfHyperdrive, PdfStream}

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
class PdfHyperdriveBench {

  private var bytes: Array[Byte] = uninitialized
  private var pdfPath: Path      = uninitialized
  private val runtime            = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    val is = getClass.getResourceAsStream("/xref-stream.pdf")
    require(is != null, "xref-stream.pdf not on classpath")
    bytes = is.readAllBytes()
    is.close()
    pdfPath = Files.createTempFile("hyperdrive-bench-", ".pdf")
    Files.write(pdfPath, bytes)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    val _ = Files.deleteIfExists(pdfPath)
  }

  @Benchmark
  def hyperdriveDecodeSync: Int =
    PdfHyperdrive.decodeSync(bytes).size

  @Benchmark
  def pdfEngineDecode: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfEngine.decode(pdfPath).provide(PdfEngine.live)).getOrThrow().size
    }

  @Benchmark
  def zioStreamDecode: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          zio.stream.ZStream
            .fromChunk(zio.Chunk.fromArray(bytes))
            .via(PdfStream.decode())
            .runCount
        )
        .getOrThrow()
        .toInt
    }
}
