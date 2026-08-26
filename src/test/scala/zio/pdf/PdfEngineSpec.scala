/*
 * PdfEngine façade — path decode / stream / policy / text / compare.
 */

package zio.pdf

import java.nio.file.Files

import _root_.scodec.bits.BitVector
import zio.pdf.io.PdfIO
import zio.*
import zio.stream.ZStream
import zio.test.*

object PdfEngineSpec extends ZIOSpecDefault {

  private def sameDecoded(left: Decoded, right: Decoded): Boolean =
    (left, right) match {
      case (a: Decoded.ContentObj, b: Decoded.ContentObj) =>
        a.obj == b.obj && a.rawStream == b.rawStream
      case (a, b) => a == b
    }

  private def sameTimeline(left: Chunk[Decoded], right: Chunk[Decoded]): Boolean =
    left.size == right.size && left.zip(right).forall(sameDecoded)

  private val contentPayload: BitVector =
    BitVector("BT /F1 24 Tf 100 700 Td (hi) Tj ET\n".getBytes)

  private def minimalPdfBytes: ZIO[Any, Throwable, Array[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
        "Contents" -> Prim.Ref(4, 0),
        "Resources" -> Prim.dict("Font" -> Prim.dict("F1" -> Prim.Ref(5, 0)))
      )
    )
    val content = IndirectObj.stream(4, Prim.dict(), contentPayload)
    val font = IndirectObj.nostream(
      5,
      Prim.dict(
        "Type" -> Prim.Name("Font"),
        "Subtype" -> Prim.Name("Type1"),
        "BaseFont" -> Prim.Name("Courier")
      )
    )
    val trailer =
      Trailer(BigDecimal(6), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content, font)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
      .map(_.toArray)
  }

  private def jsPdfBytes: ZIO[Any, Throwable, Array[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict(
        "Type"       -> Prim.Name("Catalog"),
        "Pages"      -> Prim.Ref(2, 0),
        "OpenAction" -> Prim.dict(
          "S"  -> Prim.Name("JavaScript"),
          "JS" -> Prim.str("app.alert('x');")
        )
      )
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 1, 1),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.dict(), BitVector("BT (x) Tj ET\n".getBytes))
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
      .map(_.toArray)
  }

  private def withTempBytes(bytes: Array[Byte])(use: java.nio.file.Path => ZIO[Any, Throwable, TestResult]) =
    for {
      path   <- ZIO.attemptBlocking(Files.createTempFile("pdf-engine-", ".pdf"))
      _      <- ZIO.attemptBlocking(Files.write(path, bytes))
      result <- use(path).ensuring(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
    } yield result

  private def loadFixture(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing")
      val b = is.readAllBytes()
      is.close()
      b
    }

  def spec: Spec[Any, Throwable] = suite("PdfEngine")(
    test("default input limit permits multi-gigabyte incremental paths") {
      assertTrue(
        PdfEngine.Options.default.maxInputBytes == Long.MaxValue,
        PdfEngine.Options.default.maxMaterializedDocumentBytes == ByteLimit.mebibytes(256)
      )
    },
    test("decode path matches Hyperdrive sync on xref-stream.pdf") {
      loadFixture("xref-stream.pdf").flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            engine <- PdfEngine.decode(path).provide(PdfEngine.live)
            direct  = PdfHyperdrive.decodeSync(bytes)
          } yield assertTrue(engine == direct)
        }
      }
    },
    test("sinkZIO counts match decode size") {
      loadFixture("xref-stream.pdf").flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            n   <- PdfEngine.sinkZIO(path)(_ => ZIO.unit).provide(PdfEngine.live)
            all <- PdfEngine.decode(path).provide(PdfEngine.live)
          } yield assertTrue(n == all.size.toLong)
        }
      }
    },
    test("policy.strict flags OpenAction JavaScript") {
      jsPdfBytes.flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            result <- PdfEngine.policy(path, PdfPolicy.strict).provide(PdfEngine.live)
          } yield assertTrue(!result.isSuccess)
        }
      }
    },
    test("inspect runs a typed preflight plan through the incremental engine stream") {
      jsPdfBytes.flatMap { bytes =>
        PdfEngine
          .inspect(ZStream.fromChunk(Chunk.fromArray(bytes)), PdfInspection.forbidJavaScript)
          .provide(PdfEngine.live)
          .map {
            case PdfInspection.Outcome.Rejected(report, PdfInspection.Violation.JavaScript(found)) =>
              assertTrue(found.objectNumber == 1L, report.elementsRead >= 1L)
            case _ => assertTrue(false)
          }
      }
    },
    test("extractText pulls literal Tj string") {
      minimalPdfBytes.flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            pages <- PdfEngine.extractText(path).runCollect.provide(PdfEngine.live)
          } yield assertTrue(pages.exists(_.text.contains("hi")))
        }
      }
    },
    test("evidence merges the JVM path observers into one immutable bundle") {
      minimalPdfBytes.flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            bundle <- PdfEngine.evidence(path).provide(PdfEngine.live)
            digest <- PdfEngine.digest(path).provide(PdfEngine.live)
          } yield assertTrue(
            bundle.sha256 == digest,
            bundle.decodedEvents > 0L,
            bundle.validation.isSuccess,
            bundle.policy.isSuccess,
            bundle.nativeText.pages == 1L,
            bundle.nativeText.textPages == 1L,
            bundle.nativeText.characters == 2L,
            bundle.nativeText.retainsPages,
            bundle.nativeText.retainedPages.size == 1,
            bundle.nativeText.retainedPages.head.contentObjectNumbers == Chunk(4L),
            bundle.nativeText.retainedPages.head.text == "hi",
            bundle.inspection match
              case PdfInspection.Outcome.Accepted(report) =>
                report.fonts == Chunk(PdfInspection.Font(5L, "Courier", Some("Type1")))
              case _ => false,
            bundle.canonicalJson.contains("\"pageObjectNumber\":3"),
            bundle.canonicalJson.contains("\"contentObjectNumbers\":[4]"),
            bundle.canonicalJson.contains("\"baseFont\":\"Courier\"")
          )
        }
      }
    },
    test("citation evidence keeps page provenance while bounding each retained excerpt") {
      minimalPdfBytes.flatMap { bytes =>
        val plan = PdfEvidence.Plan(text = PdfEvidence.TextMode.Citations(previewCharacters = 8, pageExcerptCharacters = 1))
        PdfEngine
          .evidence(ZStream.fromChunk(Chunk.fromArray(bytes)), plan)
          .provide(PdfEngine.live)
          .map { bundle =>
            val citation = bundle.nativeText.retainedPages.head
            assertTrue(
              bundle.nativeText.retainsPages,
              bundle.nativeText.characters == 2L,
              citation.pageObjectNumber == 3L,
          citation.contentObjectNumbers == Chunk(4L),
          citation.text == "h",
          citation.truncated,
          citation.pageNumber == 1L,
          bundle.canonicalJson.contains("\"truncated\":true")
            )
          }
      }
    },
    test("page evidence exposes digest-scoped citations and explicit recovery work") {
      val native = PdfEvidence.NativeText(
        pages = 2L,
        textPages = 1L,
        characters = 5L,
        preview = "hello",
        retainedPages = Chunk(
          PdfEvidence.Page(
            pageObjectNumber = 11L,
            contentObjectNumbers = Chunk(21L),
            text = "",
            pageNumber = 1L,
            nativeTextRecovered = false
          ),
          PdfEvidence.Page(
            pageObjectNumber = 12L,
            contentObjectNumbers = Chunk(22L, 23L),
            text = "hello",
            pageNumber = 2L,
            nativeTextRecovered = true
          )
        ),
        retainsPages = true
      )

      for {
        recovered <- native.recoverMissing(request => ZIO.succeed(s"ocr:${request.location.pageNumber}"))
      } yield {
        val citation = native.citations(Chunk(0.toByte, 15.toByte)).head
        val request = native.textRecoveryRequests.head
        val boundedNative = native.copy(
          retainedPages = Chunk(
            PdfEvidence.Page(
              pageObjectNumber = 13L,
              contentObjectNumbers = Chunk(24L),
              text = "",
              truncated = true,
              pageNumber = 1L,
              nativeTextRecovered = true
            )
          )
        )
        assertTrue(
          citation.id == "pdf:000f:page:2",
          citation.location.pageNumber == 2L,
          citation.location.pageObjectNumber == 12L,
          citation.location.contentObjectNumbers == Chunk(22L, 23L),
          citation.excerpt == "hello",
          request.location.pageNumber == 1L,
          request.location.pageObjectNumber == 11L,
          request.reason == PdfEvidence.TextRecovery.Reason.NoUsableNativeText,
          recovered == Chunk(PdfEvidence.TextRecovery.Recovered(request, "ocr:1")),
          boundedNative.citations(Chunk.empty).isEmpty,
          boundedNative.textRecoveryRequests.isEmpty
        )
      }
    },
    test("evidence keeps inspecting after a policy violation so the bundle remains complete") {
      jsPdfBytes.flatMap { bytes =>
        PdfEngine
          .evidence(ZStream.fromChunk(Chunk.fromArray(bytes)).rechunk(23))
          .provide(PdfEngine.live)
          .map { bundle =>
            bundle.inspection match
              case PdfInspection.Outcome.Rejected(report, PdfInspection.Violation.JavaScript(found)) =>
                assertTrue(
                  found.objectNumber == 1L,
                  report.completion == PdfInspection.Completion.RejectedAfterFullScan,
                  bundle.validation.isSuccess,
                  !bundle.policy.isSuccess,
                  bundle.nativeText.textPages == 1L,
                  bundle.sha256.nonEmpty
                )
              case _ => assertTrue(false)
          }
      }
    },
    test("decode caller-owned byte source matches path decode count") {
      loadFixture("xref-stream.pdf").flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            sourceCount <- PdfEngine.decode(PdfIO.reader(path)).runCount.provide(PdfEngine.live)
            pathCount   <- PdfEngine.decode(path).map(_.size.toLong).provide(PdfEngine.live)
          } yield assertTrue(sourceCount == pathCount)
        }
      }
    },
    test("streaming decode rejects an oversized input before parsing") {
      val opts = PdfEngine.Options(maxInputBytes = 8L)
      ZStream
        .fromChunks(Chunk.fromArray("%PDF".getBytes), Chunk.fromArray("-1.7\n".getBytes))
        .via(PdfEngine.decoded(opts))
        .runDrain
        .either
        .provide(PdfEngine.live)
        .map {
          case Left(PdfEngine.InputTooLarge(limit, observed)) => assertTrue(limit == 8L, observed == 9L)
          case _                                             => assertTrue(false)
        }
    },
    test("fused path decode rejects an oversized input before mapping it") {
      val opts = PdfEngine.Options(maxInputBytes = 8L)
      withTempBytes(Array.fill[Byte](9)(0)) { path =>
        PdfEngine
          .decode(path, opts)
          .either
          .provide(PdfEngine.live)
          .map {
            case Left(PdfEngine.InputTooLarge(limit, observed)) => assertTrue(limit == 8L, observed == 9L)
            case _                                             => assertTrue(false)
          }
      }
    },
    test("collected path decode has an independent typed materialization limit") {
      val limit = ByteLimit.fromBytes(8L).toOption.get
      val opts  = PdfEngine.Options(maxMaterializedDocumentBytes = limit)
      withTempBytes(Array.fill[Byte](9)(0)) { path =>
        PdfEngine
          .decode(path, opts)
          .either
          .provide(PdfEngine.live)
          .map {
            case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
              assertTrue(observed == 9L)
            case _ => assertTrue(false)
          }
      }
    },
    test("stream-returning decode is not constrained by the collected-document limit") {
      val limit = ByteLimit.fromBytes(8L).toOption.get
      val opts  = PdfEngine.Options(maxMaterializedDocumentBytes = limit)
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .decode(ZStream.fromChunk(Chunk.fromArray(bytes)), opts)
          .runDrain
          .as(assertTrue(bytes.length > limit.bytes))
          .provide(PdfEngine.live)
      }
    },
    test("chunk decode rejects an oversized input before allocating a fused buffer") {
      val opts = PdfEngine.Options(maxInputBytes = 8L)
      PdfEngine
        .decode(Chunk.fill(9)(0.toByte), opts)
        .either
        .provide(PdfEngine.live)
        .map {
          case Left(PdfEngine.InputTooLarge(limit, observed)) => assertTrue(limit == 8L, observed == 9L)
          case _                                             => assertTrue(false)
        }
    },
    test("xref-indexed chunk decoding matches the sequential fused timeline") {
      loadFixture("court-corpus/oknd-general-order-2024-09.pdf").flatMap { bytes =>
        val chunk = Chunk.fromArray(bytes)
        for {
          sequential <- ZIO.succeed(PdfHyperdrive.decodeSync(bytes))
          parallel   <- PdfEngine.decode(chunk).withParallelism(4)
        } yield {
          assertTrue(
            StructuralIndex.index(bytes).nonEmpty,
            sameTimeline(parallel, sequential)
          )
        }
      }.provide(PdfEngine.live)
    },
    test("a bounded decode pipeline can be reused without carrying prior byte counts") {
      minimalPdfBytes.flatMap { bytes =>
        val opts = PdfEngine.Options(maxInputBytes = bytes.length.toLong)
        for {
          engine <- ZIO.service[PdfEngine]
          pipe    = engine.decoded(opts)
          first  <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(pipe).runCollect
          second <- ZStream.fromChunk(Chunk.fromArray(bytes)).via(pipe).runCollect
        } yield assertTrue(first.nonEmpty, second.size == first.size)
      }.provide(PdfEngine.live)
    },
    test("compare identical paths succeeds") {
      minimalPdfBytes.flatMap { bytes =>
        for {
          a <- ZIO.attemptBlocking(Files.createTempFile("pdf-engine-a-", ".pdf"))
          b <- ZIO.attemptBlocking(Files.createTempFile("pdf-engine-b-", ".pdf"))
          _ <- ZIO.attemptBlocking(Files.write(a, bytes))
          _ <- ZIO.attemptBlocking(Files.write(b, bytes))
          result <- PdfEngine
                      .compare(a, b)
                      .provide(PdfEngine.live)
                      .ensuring(ZIO.attemptBlocking {
                        Files.deleteIfExists(a)
                        Files.deleteIfExists(b)
                      }.ignore)
        } yield assertTrue(result.isSuccess)
      }
    }
  )
}
