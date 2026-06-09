/*
 * PdfIO entry-point benchmarks: scoped (zio-blocks-scope) vs ZIO reader
 * on the real `xref-stream.pdf` fixture.
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
 * results as trends not gospel. Decode dominates on typical PDFs; these
 * measure whether scoped incremental read pays for itself vs ZStream.
 */

package zio.pdf.io.bench

import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import zio.{Runtime, Unsafe}
import zio.pdf.{PdfStream, ValidatePdf}
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

  private var pdfPath: Path       = uninitialized
  private val runtime             = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    val is = getClass.getResourceAsStream("/xref-stream.pdf")
    require(is != null, "xref-stream.pdf not on classpath (bench/src/main/resources)")
    pdfPath = Files.createTempFile("zio-pdf-io-bench-", ".pdf")
    Files.copy(is, pdfPath, StandardCopyOption.REPLACE_EXISTING)
    is.close()
    // Prime OS page cache so the first measured iteration isn't cold-start I/O.
    PdfIO.scoped.readAll(pdfPath, chunkSize)
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfIO.zio.readAll(pdfPath, chunkSize)).getOrThrow()
    }
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    Files.deleteIfExists(pdfPath)

  // -------------------------------------------------------------------
  // Full decode pipeline (the shape production code uses)
  // -------------------------------------------------------------------

  @Benchmark
  def scopedDecodeDecoded: Int =
    PdfIO.scoped.decodeDecoded(pdfPath, chunkSize, enableDiagnostics) match {
      case Right(chunk) => chunk.size
      case Left(err)    => throw err
    }

  @Benchmark
  def zioDecodeDecoded: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          PdfIO.zio
            .reader(pdfPath, chunkSize)
            .via(PdfStream.decode(enableDiagnostics))
            .runCount
        )
        .getOrThrow()
        .toInt
    }

  // -------------------------------------------------------------------
  // Validate on top of decode (scoped sync vs ZIO effect)
  // -------------------------------------------------------------------

  @Benchmark
  def scopedValidate: Boolean =
    PdfIO.scoped.validate(pdfPath, chunkSize, enableDiagnostics) match {
      case Right(v) => v.isSuccess
      case Left(e)  => throw e
    }

  @Benchmark
  def zioValidate: Boolean =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          PdfIO.zio
            .reader(pdfPath, chunkSize)
            .via(PdfStream.decode(enableDiagnostics))
            .runCollect
            .map(ValidatePdf.fromChunk)
            .map(_.isSuccess)
        )
        .getOrThrow()
    }

  // -------------------------------------------------------------------
  // Raw bytes only (isolates I/O + Scope vs ZStream overhead)
  // -------------------------------------------------------------------

  @Benchmark
  def scopedReadAll: Int =
    PdfIO.scoped.readAll(pdfPath, chunkSize).size

  @Benchmark
  def zioReadAll: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfIO.zio.readAll(pdfPath, chunkSize)).getOrThrow().size
    }
}
