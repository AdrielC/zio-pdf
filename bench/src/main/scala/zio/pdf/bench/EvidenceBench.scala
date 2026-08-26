/*
 * Focused evidence benchmarks.
 *
 *   sbt "bench/Jmh/run -prof gc -i 10 -wi 5 .*EvidenceBench.*"
 *
 * Keep early evidence separate from the public-corpus matrix: it measures a
 * valid first-object signal, while PublicPdfCorpusBench measures complete
 * bundles over larger real PDFs.
 */

package zio.pdf.bench

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import _root_.scodec.bits.BitVector
import org.openjdk.jmh.annotations.*
import zio.{Chunk, Runtime, Unsafe}
import zio.pdf.{IndirectObj, PdfEngine, PdfInspection, Prim, Trailer, WritePdf}
import zio.stream.ZStream

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
class EvidenceBench:

  private var linearizedPath: Path = scala.compiletime.uninitialized
  private val runtime              = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit =
    linearizedPath = Files.createTempFile("linearized-evidence-bench-", ".pdf")
    Files.write(linearizedPath, linearizedFixture()): Unit

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    val _ = Files.deleteIfExists(linearizedPath)

  /**
   * Time to the first positive structural fact on a valid marker PDF.
   * `SatisfiedEarly` proves that this composable preflight leaf stops after
   * its first `/Linearized` object. Use JMH's GC profiler to compare
   * allocation/op; that is reproducible, unlike a process-wide peak claim.
   */
  @Benchmark
  def pathFirstLinearizationEvidence: Long =
    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe
        .run(PdfEngine.inspect(linearizedPath, PdfInspection.linearized).provide(PdfEngine.live))
        .getOrThrow() match
        case PdfInspection.Outcome.Accepted(report)    => report.elementsRead
        case PdfInspection.Outcome.Rejected(report, _) => report.elementsRead
    }

  private def linearizedFixture(): Array[Byte] =
    val marker = IndirectObj.nostream(1, Prim.dict("Linearized" -> Prim.Number(BigDecimal(1))))
    val catalog = IndirectObj.nostream(
      2,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(3, 0))
    )
    val pages = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type" -> Prim.Name("Pages"),
        "Kids" -> Prim.Array(Prim.Ref(4, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      4,
      Prim.dict(
        "Type" -> Prim.Name("Page"),
        "Parent" -> Prim.Ref(3, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
        "Contents" -> Prim.Ref(5, 0)
      )
    )
    val content = IndirectObj.stream(5, Prim.Dict.empty, BitVector("BT (linearized) Tj ET\\n".getBytes))
    val trailer = Trailer(BigDecimal(6), Prim.dict("Root" -> Prim.Ref(2, 0)), Some(Prim.Ref(2, 0)))

    Unsafe.unsafe { implicit unsafe =>
      runtime.unsafe
        .run(
          ZStream(marker, catalog, pages, page, content)
            .via(WritePdf.objects(trailer))
            .runFold(Chunk.empty[Byte])((all, value) => all ++ Chunk.fromArray(value.toArray))
        )
        .getOrThrow()
        .toArray
    }
