package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.bits.BitVector
import zio.*
import zio.stream.ZStream
import zio.test.*

/**
 * Compact, deterministic evidence fixtures that cover parser shapes not
 * supplied by the public court corpus. Each fixture exercises PdfEngine's
 * public one-pass evidence entrypoint rather than calling an inspector
 * accumulator directly.
 */
object EvidenceCorpusSpec extends ZIOSpecDefault:

  private def write(objects: List[IndirectObj], root: Long): ZIO[Any, Throwable, Chunk[Byte]] =
    val trailer = Trailer(BigDecimal(objects.map(_.obj.index.number).max + 1L), Prim.dict("Root" -> Prim.Ref(root, 0)), Some(Prim.Ref(root, 0)))
    ZStream
      .fromIterable(objects)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((all, bytes) => all ++ Chunk.fromArray(bytes.toArray))

  private def document(
    catalogNumber: Long,
    pagesNumber: Long,
    pageNumber: Long,
    contentNumber: Long,
    content: BitVector,
    extraCatalog: Prim.Dict = Prim.Dict.empty,
    prefix: List[IndirectObj] = Nil,
    suffix: List[IndirectObj] = Nil
  ): ZIO[Any, Throwable, Chunk[Byte]] =
    val catalog = IndirectObj.nostream(
      catalogNumber,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(pagesNumber, 0)) ++ extraCatalog
    )
    val pages = IndirectObj.nostream(
      pagesNumber,
      Prim.dict(
        "Type" -> Prim.Name("Pages"),
        "Kids" -> Prim.Array(Prim.Ref(pageNumber, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      pageNumber,
      Prim.dict(
        "Type" -> Prim.Name("Page"),
        "Parent" -> Prim.Ref(pagesNumber, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
        "Contents" -> Prim.Ref(contentNumber, 0)
      )
    )
    val pageContent = IndirectObj.stream(contentNumber, Prim.Dict.empty, content)
    write(prefix ++ List(catalog, pages, page, pageContent) ++ suffix, catalogNumber)

  private def nativeTextFixture: ZIO[Any, Throwable, Chunk[Byte]] =
    document(
      catalogNumber = 1,
      pagesNumber = 2,
      pageNumber = 3,
      contentNumber = 4,
      content = BitVector("BT (native evidence) Tj ET\\n".getBytes(StandardCharsets.US_ASCII))
    )

  private def visualOnlyFixture: ZIO[Any, Throwable, Chunk[Byte]] =
    document(
      catalogNumber = 1,
      pagesNumber = 2,
      pageNumber = 3,
      contentNumber = 4,
      content = BitVector("q 0 0 612 792 re f Q\\n".getBytes(StandardCharsets.US_ASCII))
    )

  private def pdfA3bFixture: ZIO[Any, Throwable, Chunk[Byte]] =
    val metadata = IndirectObj.stream(
      5,
      Prim.dict("Type" -> Prim.Name("Metadata"), "Subtype" -> Prim.Name("XML")),
      BitVector(
        "<x:xmpmeta><pdfaid:part>3</pdfaid:part><pdfaid:conformance>B</pdfaid:conformance></x:xmpmeta>"
          .getBytes(StandardCharsets.UTF_8)
      )
    )
    document(
      catalogNumber = 1,
      pagesNumber = 2,
      pageNumber = 3,
      contentNumber = 4,
      content = BitVector("BT (pdfa) Tj ET\\n".getBytes(StandardCharsets.US_ASCII)),
      extraCatalog = Prim.dict("Metadata" -> Prim.Ref(5, 0)),
      suffix = List(metadata)
    )

  private def linearizedFixture: ZIO[Any, Throwable, Chunk[Byte]] =
    val marker = IndirectObj.nostream(1, Prim.dict("Linearized" -> Prim.Number(BigDecimal(1))))
    document(
      catalogNumber = 2,
      pagesNumber = 3,
      pageNumber = 4,
      contentNumber = 5,
      content = BitVector("BT (linearized) Tj ET\\n".getBytes(StandardCharsets.US_ASCII)),
      prefix = List(marker)
    )

  private def fixture(name: String): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.attemptBlocking {
      val resource = getClass.getResourceAsStream(s"/$name")
      require(resource != null, s"missing fixture: $name")
      try Chunk.fromArray(resource.readAllBytes())
      finally resource.close()
    }

  private def browserEvidence(bytes: Chunk[Byte]): ZIO[PdfEngine, Throwable, PdfEvidence.Bundle] =
    PdfEngine.evidence(bytes, PdfEvidence.Plan.browser)

  def spec: Spec[Any, Throwable] = suite("EvidenceCorpus")(
    test("retains native citations and makes visual-only recovery explicit") {
      for
        native <- nativeTextFixture
        visual <- visualOnlyFixture
        nativeBundle <- browserEvidence(native)
        visualBundle <- browserEvidence(visual)
      yield assertTrue(
        nativeBundle.citations.size == 1,
        nativeBundle.citations.head.location.pageNumber == 1L,
        nativeBundle.citations.head.location.pageObjectNumber == 3L,
        nativeBundle.textRecoveryRequests.isEmpty,
        visualBundle.nativeText.pages == 1L,
        visualBundle.nativeText.textPages == 0L,
        visualBundle.citations.isEmpty,
        visualBundle.textRecoveryRequests == Chunk(
          PdfEvidence.TextRecovery.Request(
            PdfEvidence.PageLocation(1L, 3L, Chunk(4L)),
            PdfEvidence.TextRecovery.Reason.NoUsableNativeText
          )
        )
      )
    }.provide(PdfEngine.live),
    test("records PDF/A declarations and linearization markers from actual decoded PDFs") {
      for
        pdfA <- pdfA3bFixture
        linearized <- linearizedFixture
        pdfABundle <- browserEvidence(pdfA)
        linearizedBundle <- browserEvidence(linearized)
        early <- PdfEngine.inspect(linearized, PdfInspection.linearized)
      yield
        val pdfAReport = pdfABundle.inspection match
          case PdfInspection.Outcome.Accepted(report)    => report
          case PdfInspection.Outcome.Rejected(report, _) => report
        val linearizedReport = linearizedBundle.inspection match
          case PdfInspection.Outcome.Accepted(report)    => report
          case PdfInspection.Outcome.Rejected(report, _) => report
        val earlyReport = early match
          case PdfInspection.Outcome.Accepted(report)    => report
          case PdfInspection.Outcome.Rejected(report, _) => report
        assertTrue(
          pdfAReport.pdfA.exists(_.declaresA3b),
          pdfAReport.pdfAEvidence.exists(_.confidence == PdfInspection.Confidence.Structural),
          linearizedReport.linearization.contains(PdfInspection.Linearization(1L)),
          linearizedReport.linearizationEvidence.exists(_.confidence == PdfInspection.Confidence.Structural),
          earlyReport.linearization.contains(PdfInspection.Linearization(1L)),
          earlyReport.elementsRead == 1L,
          earlyReport.completion == PdfInspection.Completion.SatisfiedEarly
        )
    }.provide(PdfEngine.live),
    test("counts real image XObjects without turning the fixture into a special case") {
      fixture("test-image.pdf").flatMap(browserEvidence).map { bundle =>
        val report = bundle.inspection match
          case PdfInspection.Outcome.Accepted(value)    => value
          case PdfInspection.Outcome.Rejected(value, _) => value
        assertTrue(
          report.imageCount > 0L,
          report.imageCountEvidence.confidence == PdfInspection.Confidence.Structural,
          bundle.nativeText.pages == 4L
        )
      }.provide(PdfEngine.live)
    },
    test("rejects malformed input or returns a failed validation record") {
      browserEvidence(Chunk.fromArray("not a PDF".getBytes(StandardCharsets.US_ASCII))).either.map {
        case Left(_)       => assertTrue(true)
        case Right(bundle) => assertTrue(!bundle.validation.isSuccess)
      }.provide(PdfEngine.live)
    }
  )
