package zio.pdf

import java.nio.file.Files
import java.security.MessageDigest

import zio.*
import zio.stream.ZStream
import zio.test.*
import zio.test.Assertion.*

/**
 * Integration contracts over real, public federal-court PDFs.
 *
 * The fixtures are vendored so CI never relies on court-site availability. See
 * `src/test/resources/court-corpus/README.md` for source URLs and provenance.
 */
object CourtCorpusSpec extends ZIOSpecDefault:

  private final case class Fixture(
    name: String,
    bytes: Int,
    sha256: String,
    version: Version,
    pages: Int,
    dataObjects: Int,
    contentObjects: Int,
    annotationArrays: Int,
    acroForms: Int,
    widgets: Int
  )

  private final case class DecodeSummary(
    dataObjects: Int,
    contentObjects: Int,
    metas: Int,
    version: Option[Version],
    annotationArrays: Int,
    acroForms: Int,
    widgets: Int
  ):
    def objectCount: Int = dataObjects + contentObjects

  private val scotusAtlanticRichfield =
    Fixture(
      "scotus-atlantic-richfield-slip-opinion.pdf",
      251066,
      "00d21eed19fa61629ef0ff0cc3837cf529c0f26fb3501be8b4afca3d4aa7b3b7",
      Version(1, 6, None),
      46,
      1665,
      76,
      0,
      0,
      0
    )

  private val scotusOrderList =
    Fixture(
      "scotus-order-list-2025-05-19.pdf",
      75365,
      "2fd20eee0560d55307e3e96343dd737affd4c3b64ffaf526654ad2ad48c1633b",
      Version(1, 6, None),
      8,
      940,
      16,
      0,
      0,
      0
    )

  private val fourthCircuitBayramov =
    Fixture(
      "ca4-bayramov-v-american-credit-acceptance.pdf",
      280214,
      "2a3238423df474c05a4ad0f4c978548545e9cbe0fac201c62689fce7a6cc4def",
      Version(1, 6, None),
      25,
      332,
      38,
      0,
      0,
      0
    )

  private val federalCircuitJanich =
    Fixture(
      "cafc-janich-v-collins.pdf",
      124254,
      "66b0ebaad01de04bc5a7c331e4e851d6e300a29af197745a1002a4fbca794c24",
      Version(1, 6, None),
      11,
      120,
      49,
      0,
      0,
      0
    )

  private val govInfoDistrictOrder =
    Fixture(
      "govinfo-district-court-order.pdf",
      199530,
      "1a9d7090501e370779069a85bad351b7c933b5f22a105373e63abb4bef071090",
      Version(1, 5, None),
      11,
      82,
      49,
      1,
      1,
      1
    )

  private val okndGeneralOrder =
    Fixture(
      "oknd-general-order-2024-09.pdf",
      1384975,
      "f14ebb8d301dd5f5e4e0d589bb172bc36e6861a17bc7ebf1f1bbbeafbe04e3a8",
      Version(1, 6, None),
      92,
      2173,
      337,
      0,
      0,
      0
    )

  private val fixtures = Vector(
    scotusAtlanticRichfield,
    scotusOrderList,
    fourthCircuitBayramov,
    federalCircuitJanich,
    govInfoDistrictOrder,
    okndGeneralOrder
  )

  private def read(fixture: Fixture): Task[Chunk[Byte]] =
    ZIO.attemptBlocking {
      val resource = getClass.getResourceAsStream(s"/court-corpus/${fixture.name}")
      require(resource != null, s"Missing court corpus fixture: ${fixture.name}")
      try Chunk.fromArray(resource.readAllBytes())
      finally resource.close()
    }

  private def hex(bytes: Chunk[Byte]): String =
    bytes.iterator.map(byte => f"${byte & 0xff}%02x").mkString

  private def sha256(bytes: Chunk[Byte]): String =
    val digest = MessageDigest.getInstance("SHA-256")
    bytes.foreach(digest.update)
    digest.digest().iterator.map(byte => f"${byte & 0xff}%02x").mkString

  private def dictionaries(decoded: Chunk[Decoded]): Iterator[Prim.Dict] =
    decoded.iterator.flatMap {
      case Decoded.DataObj(Obj(_, dictionary: Prim.Dict))          => Iterator.single(dictionary)
      case Decoded.ContentObj(Obj(_, dictionary: Prim.Dict), _, _) => Iterator.single(dictionary)
      case _                                                        => Iterator.empty
    }

  private def summary(decoded: Chunk[Decoded]): DecodeSummary =
    DecodeSummary(
      dataObjects = decoded.count {
        case _: Decoded.DataObj => true
        case _                  => false
      },
      contentObjects = decoded.count {
        case _: Decoded.ContentObj => true
        case _                     => false
      },
      metas = decoded.count {
        case _: Decoded.Meta => true
        case _               => false
      },
      version = decoded.collectFirst { case Decoded.Meta(_, _, version) => version }.flatten,
      annotationArrays = dictionaries(decoded).count(_.apply("Annots").exists(_.isInstanceOf[Prim.Array])),
      acroForms = dictionaries(decoded).count(_.apply("AcroForm").isDefined),
      widgets = dictionaries(decoded).count(_.apply("Subtype").contains(Prim.Name("Widget")))
    )

  private def sameDecoded(left: Decoded, right: Decoded): Boolean =
    (left, right) match {
      case (a: Decoded.ContentObj, b: Decoded.ContentObj) =>
        a.obj == b.obj && a.rawStream == b.rawStream
      case (a, b) => a == b
    }

  private def sameTimeline(left: Chunk[Decoded], right: Chunk[Decoded]): Boolean =
    left.size == right.size && left.zip(right).forall(sameDecoded)

  private def withTempPdf[R, A](fixture: Fixture, bytes: Chunk[Byte])(
    use: java.nio.file.Path => ZIO[R, Throwable, A]
  ): ZIO[R, Throwable, A] =
    ZIO.acquireReleaseWith(
      ZIO.attemptBlocking {
        val path = Files.createTempFile(s"court-corpus-${fixture.name.stripSuffix(".pdf")}-", ".pdf")
        Files.write(path, bytes.toArray)
        path
      }
    )(path => ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)(use)

  private def verify(fixture: Fixture): ZIO[PdfEngine, Throwable, TestResult] =
    for
      bytes <- read(fixture)
      streamed <- ZStream
                    .fromChunk(bytes)
                    .rechunk(257)
                    .via(PdfStream.decode())
                    .runCollect
      fused <- withTempPdf(fixture, bytes)(PdfEngine.decode(_))
      digest <- PdfEngine.digest(ZStream.fromChunk(bytes))
      streamedSummary = summary(streamed)
      fusedSummary    = summary(fused)
    yield assertTrue(
      bytes.size == fixture.bytes,
      sha256(bytes) == fixture.sha256,
      hex(digest) == fixture.sha256,
      streamedSummary == fusedSummary,
      fusedSummary.dataObjects == fixture.dataObjects,
      fusedSummary.contentObjects == fixture.contentObjects,
      fusedSummary.metas == 1,
      fusedSummary.version.exists(version => version.major == fixture.version.major && version.minor == fixture.version.minor),
      fusedSummary.annotationArrays == fixture.annotationArrays,
      fusedSummary.acroForms == fixture.acroForms,
      fusedSummary.widgets == fixture.widgets
    )

  private def verifyNoCollection(fixture: Fixture): ZIO[PdfEngine, Throwable, TestResult] =
    for
      bytes <- read(fixture)
      result <- withTempPdf(fixture, bytes) { path =>
                  for
                    streamCount <- PdfEngine.stream(path).runCount
                    sinkCount   <- PdfEngine.sink(path)(_ => ())
                  yield assertTrue(
                    streamCount == fixture.dataObjects + fixture.contentObjects + 1L,
                    sinkCount == streamCount
                  )
                }
    yield result

  private def verifyNativeText(fixture: Fixture): ZIO[PdfEngine, Throwable, TestResult] =
    for
      bytes <- read(fixture)
      rechunked <- PdfEngine
                     .extractText(ZStream.fromChunk(bytes).rechunk(257))
                     .runCollect
      fused <- withTempPdf(fixture, bytes)(path => PdfEngine.extractText(path).runCollect)
    yield assertTrue(
      fused.size == fixture.pages,
      rechunked.size == fixture.pages,
      fused == rechunked,
      fused.forall(_.text.trim.nonEmpty)
    )

  private def verifyEvidenceSummary(fixture: Fixture): ZIO[PdfEngine, Throwable, TestResult] =
    for
      bytes <- read(fixture)
      bundle <- withTempPdf(fixture, bytes)(path => PdfEngine.evidence(path, PdfEvidence.Plan.browser))
    yield assertTrue(
      bundle.sha256Hex == fixture.sha256,
      bundle.decodedEvents == fixture.dataObjects + fixture.contentObjects + 1L,
      bundle.validation.isSuccess,
      bundle.nativeText.pages == fixture.pages,
      bundle.nativeText.textPages == fixture.pages,
      bundle.nativeText.characters > 0L,
      bundle.nativeText.retainsPages,
      bundle.nativeText.retainedPages.size == fixture.pages,
      bundle.nativeText.retainedPages.forall(page => page.text.length <= 360),
      bundle.nativeText.retainedPages.zipWithIndex.forall { case (page, index) =>
        page.pageNumber == index.toLong + 1L && page.hasNativeText
      },
      bundle.citations.size == fixture.pages,
      bundle.citations.forall(citation => citation.id.startsWith(s"pdf:${fixture.sha256}:page:")),
      bundle.textRecoveryRequests.isEmpty,
      bundle.nativeText.preview.nonEmpty
    )

  private def fixtureTest(fixture: Fixture) =
    test(s"preserves and decodes ${fixture.name} across fused and 257-byte streaming paths") {
      verify(fixture).provide(PdfEngine.live)
    } @@ TestAspect.timeout(90.seconds)

  def spec: Spec[TestEnvironment & Scope, Any] =
    suite("CourtCorpus")(
      fixtureTest(scotusAtlanticRichfield),
      fixtureTest(scotusOrderList),
      fixtureTest(fourthCircuitBayramov),
      fixtureTest(federalCircuitJanich),
      fixtureTest(govInfoDistrictOrder),
      fixtureTest(okndGeneralOrder),
      test("every accepted xref index preserves its court-corpus decode timeline") {
        ZIO
          .foreach(fixtures) { fixture =>
            read(fixture).flatMap { bytes =>
              val array = bytes.toArray
              StructuralIndex.index(array) match {
                case Some(indexed) =>
                  indexed.decode(array).withParallelism(4).map { parallel =>
                    sameTimeline(parallel, PdfHyperdrive.decodeSync(array))
                  }
                case None => ZIO.succeed(true)
              }
            }
          }
          .map(results => assertTrue(results.forall(identity)))
      },
      test("incremental textual xrefs deliberately retain the fused decoder") {
        read(govInfoDistrictOrder).map { bytes =>
          assertTrue(StructuralIndex.index(bytes.toArray).isEmpty)
        }
      },
      test("processes the largest corpus fixture through stream and sink without collecting decoded objects") {
        verifyNoCollection(fixtures.maxBy(_.bytes)).provide(PdfEngine.live)
      } @@ TestAspect.timeout(90.seconds),
      test("preserves native text across fused and 257-byte streaming paths for every public fixture") {
        ZIO
          .foreach(fixtures)(verifyNativeText)
          .map(results => results.reduce(_ && _))
          .provide(PdfEngine.live)
      } @@ TestAspect.timeout(90.seconds),
      test("runs the largest court fixture through the bounded citation evidence bundle") {
        verifyEvidenceSummary(fixtures.maxBy(_.bytes)).provide(PdfEngine.live)
      } @@ TestAspect.timeout(90.seconds),
      test("follows the OKND first-page Contents references into native text") {
        read(okndGeneralOrder).flatMap { bytes =>
          withTempPdf(okndGeneralOrder, bytes) { path =>
            PdfEngine.elements(path).map { elements =>
              val firstPageAndContents = elements.filter {
                case Element.Data(obj, Element.DataKind.Page(_)) => obj.index.number == 2601L
                case Element.Data(obj, _)                         => obj.index.number == 2619L
                case Element.Content(obj, _, _, _)                => obj.index.number >= 2602L && obj.index.number <= 2607L
                case _                                             => false
              }
              val contents = firstPageAndContents.collectFirst {
                case Element.Data(_, Element.DataKind.Page(page)) => page.data.data.get("Contents")
              }.flatten
              val text = TextExtract.fromElements(firstPageAndContents)

              assert(contents)(isSome(equalTo(Prim.Ref(2619, 0)))) &&
                assertTrue(text.exists(_.text.contains("IN THE UNITED ST")))
            }
          }
        }.provide(PdfEngine.live)
      } @@ TestAspect.timeout(90.seconds)
    )
