/*
 * PdfEngine / PdfIO entry-point benchmarks on the real `xref-stream.pdf` fixture.
 *
 * Run from sbt:
 *
 *   sbt "bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 .*PdfIOBench.*"
 *
 * Quick smoke:
 *
 *   sbt "bench/Jmh/run -i 2 -wi 1 -f 1 -t 1 .*PdfIOBench.*"
 *
 * File I/O benches are noisy (OS cache, SSD). Use fixed warmup, treat
 * results as trends not gospel.
 */

package zio.pdf.io.bench

import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import zio.{Runtime, Unsafe}
import zio.pdf.{PdfEngine, PdfStream}
import zio.pdf.io.PdfIO

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class PdfIOBench {

  @Param(Array("65536"))
  var chunkSize: Int = uninitialized

  @Param(Array("false"))
  var enableDiagnostics: Boolean = uninitialized

  private var pdfPath: Path = uninitialized
  private val runtime       = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    val is = getClass.getResourceAsStream("/xref-stream.pdf")
    require(is != null, "xref-stream.pdf not on classpath (bench/src/main/resources)")
    pdfPath = Files.createTempFile("zio-pdf-io-bench-", ".pdf")
    Files.copy(is, pdfPath, StandardCopyOption.REPLACE_EXISTING)
    is.close()
    // Prime OS page cache so the first measured iteration isn't cold-start I/O.
    Unsafe.unsafe { implicit u =>
      val _ = runtime.unsafe.run(PdfIO.readAll(pdfPath, chunkSize)).getOrThrow()
    }
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    val _ = Files.deleteIfExists(pdfPath)
  }

  // -------------------------------------------------------------------
  // Full decode pipeline (the shape production code uses)
  // -------------------------------------------------------------------

  @Benchmark
  def decodeDecoded: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          PdfEngine
            .decode(pdfPath, PdfEngine.Options(enableDiagnostics = enableDiagnostics))
            .provide(PdfEngine.live)
        )
        .getOrThrow()
        .size
    }

  @Benchmark
  def decodeCount: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          PdfIO
            .reader(pdfPath, chunkSize)
            .via(PdfStream.decode(enableDiagnostics))
            .runCount
        )
        .getOrThrow()
    }

  // -------------------------------------------------------------------
  // Validate on top of decode
  // -------------------------------------------------------------------

  @Benchmark
  def validate: Boolean =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          PdfEngine
            .validate(pdfPath, PdfEngine.Options(enableDiagnostics = enableDiagnostics))
            .provide(PdfEngine.live)
        )
        .getOrThrow()
        .isSuccess
    }

  // -------------------------------------------------------------------
  // Raw bytes only (isolates I/O + ZStream overhead)
  // -------------------------------------------------------------------

  @Benchmark
  def readAll: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfIO.readAll(pdfPath, chunkSize)).getOrThrow().size
    }
}
