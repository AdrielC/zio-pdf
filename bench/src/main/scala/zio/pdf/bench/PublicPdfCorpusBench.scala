/*
 * Benchmarks over the vendored public-PDF corpus.
 *
 *   sbt "bench/Jmh/run -i 10 -wi 5 .*PublicPdfCorpusBench.*"
 *
 * Kept separate from RealPdfBench so its historical fixture matrix remains a
 * stable comparison surface. The parser has no corpus-specific branch: these
 * are ordinary resource paths exercised through the same public APIs.
 */

package zio.pdf.bench

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import zio.{Chunk, Runtime, Unsafe}
import zio.pdf.{PdfEngine, PdfEvidence, PdfHyperdrive, PdfInspection, PdfPolicy}

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(1)
class PublicPdfCorpusBench {

  @Param(
    Array(
      "court-corpus/scotus-atlantic-richfield-slip-opinion.pdf",
      "court-corpus/scotus-order-list-2025-05-19.pdf",
      "court-corpus/ca4-bayramov-v-american-credit-acceptance.pdf",
      "court-corpus/cafc-janich-v-collins.pdf",
      "court-corpus/govinfo-district-court-order.pdf",
      "court-corpus/oknd-general-order-2024-09.pdf"
    )
  )
  var fixture: String = uninitialized

  private var bytes: Array[Byte] = uninitialized
  private var chunk: Chunk[Byte] = uninitialized
  private var pdfPath: Path      = uninitialized
  private val runtime            = Runtime.default

  @Setup(Level.Trial)
  def setup(): Unit = {
    val is = getClass.getResourceAsStream(s"/$fixture")
    require(is != null, s"$fixture not on classpath")
    bytes = is.readAllBytes()
    is.close()
    chunk = Chunk.fromArray(bytes)
    pdfPath = Files.createTempFile("public-pdf-corpus-bench-", ".pdf")
    Files.write(pdfPath, bytes): Unit
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit = {
    val _ = Files.deleteIfExists(pdfPath)
  }

  @Benchmark
  def pathDecode: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfEngine.decode(pdfPath).provide(PdfEngine.live)).getOrThrow().size
    }

  @Benchmark
  def pathSink: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfEngine.sink(pdfPath)(_ => ()).provide(PdfEngine.live)).getOrThrow()
    }

  @Benchmark
  def pathStream: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfEngine.stream(pdfPath).runCount.provide(PdfEngine.live)).getOrThrow().toInt
    }

  /**
   * The composable inspection-plan path over the same decoded element stream.
   * It is intentionally a full-drain policy benchmark: proving that a document
   * contains no JavaScript requires reading all available elements.
   */
  @Benchmark
  def pathInspectForJavaScript: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(PdfEngine.inspect(pdfPath, PdfInspection.forbidJavaScript).provide(PdfEngine.live))
        .getOrThrow() match
        case PdfInspection.Outcome.Accepted(report)       => report.elementsRead
        case PdfInspection.Outcome.Rejected(report, _)    => report.elementsRead
    }

  /**
   * Compatibility baseline for a consumer that invokes every convenience verb
   * separately. It deliberately avoids retaining page text, so the comparison
   * isolates repeated reads / decodes rather than a result-size difference.
   */
  @Benchmark
  def pathFivePassReviewBaseline: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          (for
            inspection <- PdfEngine.inspect(pdfPath, PdfInspection.documentProfile)
            textChars  <- PdfEngine.extractText(pdfPath).runFold(0L)((size, page) => size + page.text.length.toLong)
            validation <- PdfEngine.validate(pdfPath)
            policy     <- PdfEngine.policy(pdfPath, PdfPolicy.strict)
            digest     <- PdfEngine.digest(pdfPath)
          yield
            val elements = inspection match
              case PdfInspection.Outcome.Accepted(report)    => report.elementsRead
              case PdfInspection.Outcome.Rejected(report, _) => report.elementsRead
            elements + textChars + digest.size.toLong + (if validation.isSuccess then 1L else 0L) +
              (if policy.isSuccess then 1L else 0L)).provide(PdfEngine.live)
        )
        .getOrThrow()
    }

  /** One path read: decode, inspect, text summary, validate, policy, and SHA-256 together. */
  @Benchmark
  def pathEvidenceBundle: Long =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(PdfEngine.evidence(pdfPath, PdfEvidence.Plan.browser).provide(PdfEngine.live))
        .getOrThrow()
        .decodedEvents
    }

  @Benchmark
  def callerOwnedByteSource: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe
        .run(
          PdfEngine
            .decode(zio.stream.ZStream.fromChunk(zio.Chunk.fromArray(bytes)))
            .runCount
            .provide(PdfEngine.live)
        )
        .getOrThrow()
        .toInt
    }

  @Benchmark
  def chunkDecodeSequential: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfEngine.decode(chunk).withParallelism(1).provide(PdfEngine.live)).getOrThrow().size
    }

  @Benchmark
  def boundaryScanChunk: Int =
    zio.pdf.PdfObjectScanner.scan(chunk) match {
      case Right(found) => found.length
      case Left(err)    => throw err
    }

  @Benchmark
  def chunkDecodeFusedBaseline: Int =
    PdfHyperdrive.decodeSync(bytes).size

  @Benchmark
  def chunkDecodeParallel: Int =
    Unsafe.unsafe { implicit u =>
      runtime.unsafe.run(PdfEngine.decode(chunk).withParallelism(4).provide(PdfEngine.live)).getOrThrow().size
    }
}
