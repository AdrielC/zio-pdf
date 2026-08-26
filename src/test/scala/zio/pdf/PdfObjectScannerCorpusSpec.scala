package zio.pdf

import java.nio.file.Path

import zio.*
import zio.stream.ZStream
import zio.test.*

object PdfObjectScannerCorpusSpec extends ZIOSpecDefault:

  private val fixtures = Chunk(
    "scotus-atlantic-richfield-slip-opinion.pdf",
    "scotus-order-list-2025-05-19.pdf",
    "ca4-bayramov-v-american-credit-acceptance.pdf",
    "cafc-janich-v-collins.pdf",
    "govinfo-district-court-order.pdf",
    "oknd-general-order-2024-09.pdf"
  )

  private def scan(path: Path): IO[PdfObjectScanner.Error, Long] =
    ZStream
      .fromFile(path.toFile, chunkSize = 257)
      .chunks
      .mapError(error => PdfObjectScanner.Error.Malformed(error.getMessage, error))
      .runFoldZIO((PdfObjectScanner.initial, 0L)) { case ((cursor, count), bytes) =>
        ZIO.fromEither(PdfObjectScanner.step(PdfObjectScanner.Config.default, cursor, bytes)).map {
          case (next, boundaries) => (next, count + boundaries.size.toLong)
        }
      }
      .flatMap { case (cursor, count) =>
        ZIO.fromEither(PdfObjectScanner.finish(cursor)).as(count)
      }

  def spec: Spec[Any, Any] = suite("PdfObjectScanner public corpus")(
    test("scans real federal-court PDFs through 257-byte upload chunks") {
      ZIO.foreach(fixtures) { name =>
        val path = Path.of("src", "test", "resources", "court-corpus", name)
        scan(path).map(count => name -> count)
      }.map { counts =>
        assertTrue(counts.forall(_._2 > 10L))
      }
    } @@ TestAspect.timeout(90.seconds)
  )
