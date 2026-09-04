package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector
import org.scalajs.dom
import scala.scalajs.js
import scala.scalajs.js.typedarray.Uint8Array
import zio.*
import zio.stream.ZStream
import zio.test.*

object ScalaJsPdfEngineSpec extends ZIOSpecDefault:

  private def attemptZIO[A](attempt: Attempt[A]): IO[Err, A] =
    attempt match
      case Attempt.Successful(value) => ZIO.succeed(value)
      case Attempt.Failure(error)    => ZIO.fail(error)

  private def minimalPdfBytes: Task[Chunk[Byte]] =
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
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792)
      )
    )
    val trailer = Trailer(BigDecimal(4), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bytes) => acc ++ Chunk.fromArray(bytes.toArray))

  private def transformPdfBytes: Task[Chunk[Byte]] =
    def font(number: Long, baseFont: String): IndirectObj =
      IndirectObj.nostream(
        number,
        Prim.dict(
          "Type"      -> Prim.Name("Font"),
          "Subtype"   -> Prim.Name("Type1"),
          "BaseFont"  -> Prim.Name(baseFont),
          "Encoding"  -> Prim.Name("WinAnsiEncoding"),
          "FirstChar" -> Prim.Number(BigDecimal(32)),
          "LastChar"  -> Prim.Number(BigDecimal(33)),
          "Widths"    -> Prim.Array(Prim.Number(BigDecimal(500)), Prim.Number(BigDecimal(500)))
        )
      )

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
        "Resources" -> Prim.dict(
          "Font" -> Prim.dict("F1" -> Prim.Ref(5, 0), "F2" -> Prim.Ref(6, 0))
        ),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(
      4,
      Prim.Dict.empty,
      BitVector("BT /F1 12 Tf (JS) Tj ET\n".getBytes)
    )
    val trailer = Trailer(BigDecimal(7), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

    ZStream(catalog, pages, page, content, font(5, "SourceFace"), font(6, "TargetFace"))
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bytes) => acc ++ Chunk.fromArray(bytes.toArray))

  def spec: Spec[Any, Throwable] = suite("Scala.js PdfEngine")(
    test("PdfSource streams a Uint8Array without a platform file API") {
      val input = new Uint8Array(3)
      input(0) = 1
      input(1) = 2
      input(2) = 3
      PdfSource
        .fromUint8Array(input)
        .bytes
        .runCollect
        .map(result => assertTrue(result == Chunk[Byte](1, 2, 3)))
    },
    test("PdfSource reopens a browser Blob for independent consumers") {
      val input = new Uint8Array(3)
      input(0) = 7
      input(1) = 8
      input(2) = 9
      val source = PdfSource.fromBlob(new dom.Blob(js.Array(input)))
      for
        first <- source.bytes.runCollect
        second <- source.bytes.runCollect
      yield assertTrue(
        first == Chunk[Byte](7, 8, 9),
        second == Chunk[Byte](7, 8, 9)
      )
    },
    test("incremental SHA-256 matches the canonical digest") {
      PdfEngine
        .digest(ZStream.fromChunk(Chunk.fromArray(Array[Byte]('a', 'b', 'c'))))
        .provide(PdfEngine.live)
        .map(result => assertTrue(result.toArray.map(byte => f"${byte & 0xff}%02x").mkString == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"))
    },
    test("Flate encode and decode round-trip through pako") {
      val input = BitVector("scala.js PDF filter support".getBytes)
      (for
        encoded <- attemptZIO(FlateEncode(input))
        decoded <- attemptZIO(FlateDecode(encoded, Prim.dict("DecodeParms" -> Prim.Dict.empty)))
      yield assertTrue(decoded == input)).mapError(error => RuntimeException(error.messageWithContext))
    },
    test("browser Flate decode enforces the same typed output bound") {
      val input = BitVector(Array.fill[Byte](256 * 1024)(0))
      val limit = ByteLimit.fromBytes(32L * 1024L).toOption.get
      attemptZIO(FlateEncode(input)).map { encoded =>
        assertTrue(
          FlateDecode(encoded, Prim.dict("DecodeParms" -> Prim.Dict.empty), limit) match
            case Attempt.Failure(error) => error.messageWithContext.contains("configured 32768-byte limit")
            case _                      => false
        )
      }.mapError(error => RuntimeException(error.messageWithContext))
    },
    test("engine decodes a generated PDF through the shared stream pipeline") {
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .decode(PdfSource.fromChunk(bytes))
          .provide(PdfEngine.live)
          .map(decoded => assertTrue(decoded.nonEmpty))
      }
    },
    test("caller-owned byte streams use the same decode verb") {
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .decode(ZStream.fromChunk(bytes))
          .runCollect
          .provide(PdfEngine.live)
          .map(decoded => assertTrue(decoded.nonEmpty))
      }
    },
    test("PdfTransform runs against a browser PdfSource and streams its render") {
      transformPdfBytes.flatMap { bytes =>
        val source = PdfSource.fromChunk(bytes)
        val program =
          PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace") >>>
            PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.characters)
        for
          output <- program.run(source.bytes).provide(PdfEngine.live)
          rendered <- output.bytes.runCollect
          pages <- PdfEngine.extractText(PdfSource.fromChunk(rendered)).runCollect.provide(PdfEngine.live)
        yield assertTrue(
          output.value == Chunk(PdfTransform.text.PageTokens(3L, Chunk('J', 'S'))),
          pages == Chunk(PageText(3L, "JS"))
        )
      }
    },
    test("browser transforms reject input above their typed materialization bound") {
      val limit   = ByteLimit.fromBytes(8L).toOption.get
      val options = PdfEngine.Options(maxMaterializedDocumentBytes = limit)
      PdfTransform.text
        .tokenize(PdfTransform.text.Tokenizer.characters)
        .run(ZStream.fromChunk(Chunk.fill(9)(0.toByte)), options)
        .either
        .provide(PdfEngine.live)
        .map {
          case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
            assertTrue(observed == 9L)
          case _ => assertTrue(false)
        }
    },
    test("shared typed inspection plans run in Scala.js") {
      val linearized = Element.Data(
        Obj(Obj.Index(1L, 0), Prim.dict("Linearized" -> Prim.Number(BigDecimal(1)))),
        Element.DataKind.General
      )
      PdfInspection
        .run(ZStream(linearized), PdfInspection.linearized)
        .map {
          case PdfInspection.Outcome.Accepted(report) =>
            assertTrue(
              report.linearization.contains(PdfInspection.Linearization(1L)),
              report.completion == PdfInspection.Completion.SatisfiedEarly
            )
          case _ => assertTrue(false)
        }
    },
    test("shared text extraction follows a page tree and indirect Contents array") {
      val catalogIndex  = Obj.Index(1L, 0)
      val pagesIndex    = Obj.Index(2L, 0)
      val pageIndex     = Obj.Index(3L, 0)
      val contentsIndex = Obj.Index(4L, 0)
      val streamIndex   = Obj.Index(5L, 0)
      val catalog = Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(pagesIndex.number, pagesIndex.generation))
      val pages = Prim.dict(
        "Type" -> Prim.Name("Pages"),
        "Kids" -> Prim.Array(Prim.Ref(pageIndex.number, pageIndex.generation)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
      val pageData = Prim.dict(
        "Type" -> Prim.Name("Page"),
        "Parent" -> Prim.Ref(pagesIndex.number, pagesIndex.generation),
        "MediaBox" -> Prim.Array.nums(0, 0, 1, 1),
        "Contents" -> Prim.Ref(contentsIndex.number, contentsIndex.generation)
      )
      val page = Page(pageIndex, pageData, MediaBox(0, 0, 1, 1))
      val elements = Chunk(
        Element.Data(Obj(catalogIndex, catalog), Element.DataKind.General),
        Element.Data(Obj(pagesIndex, pages), Element.DataKind.Pages(Pages(pagesIndex, pages, List(Prim.Ref(pageIndex.number, pageIndex.generation)), root = true))),
        Element.Data(Obj(pageIndex, pageData), Element.DataKind.Page(page)),
        Element.Data(
          Obj(contentsIndex, Prim.Array(Prim.Ref(streamIndex.number, streamIndex.generation))),
          Element.DataKind.Array(Prim.Array(Prim.Ref(streamIndex.number, streamIndex.generation)))
        ),
        Element.Content(
          Obj(streamIndex, Prim.Dict.empty),
          BitVector.empty,
          Uncompressed.now(BitVector("BT (browser text) Tj ET".getBytes)),
          Element.ContentKind.General
        ),
        Element.Meta(Some(Trailer(BigDecimal(6), Prim.dict("Root" -> Prim.Ref(catalogIndex.number, catalogIndex.generation)), Some(Prim.Ref(catalogIndex.number, catalogIndex.generation)))), None)
      )

      assertTrue(TextExtract.fromElements(elements) == Chunk(PageText(pageIndex.number, "browser text")))
    },
    test("PdfEngine.inspect accepts a browser PdfSource with the shared plan") {
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .inspect(PdfSource.fromChunk(bytes), PdfInspection.forbidJavaScript)
          .provide(PdfEngine.live)
          .map {
            case PdfInspection.Outcome.Accepted(report) =>
              assertTrue(report.elementsRead > 0L, report.completion == PdfInspection.Completion.EndOfInput)
            case _ => assertTrue(false)
        }
      }
    },
    test("PdfEngine.evidence merges browser source observers through one public API") {
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .evidence(PdfSource.fromChunk(bytes), PdfEvidence.Plan.browser)
          .provide(PdfEngine.live)
          .map { bundle =>
            assertTrue(
              bundle.decodedEvents > 0L,
              bundle.validation.isSuccess,
              bundle.policy.isSuccess,
              bundle.nativeText.pages == 1L,
              bundle.nativeText.retainsPages,
              bundle.nativeText.retainedPages.size == 1,
              bundle.nativeText.retainedPages.head.pageObjectNumber == 3L,
              bundle.sha256.nonEmpty
            )
          }
      }
    },
    test("browser sources and owned bytes share the decode, text, validate, policy, digest, and compare verbs") {
      minimalPdfBytes.flatMap { bytes =>
        val source = PdfSource.fromChunk(bytes)
        for
          decoded  <- PdfEngine.decode(bytes).provide(PdfEngine.live)
          text     <- PdfEngine.extractText(source).runCollect.provide(PdfEngine.live)
          validated <- PdfEngine.validate(source).provide(PdfEngine.live)
          policy   <- PdfEngine.policy(source, PdfPolicy.permissive).provide(PdfEngine.live)
          digest   <- PdfEngine.digest(source).provide(PdfEngine.live)
          compared <- PdfEngine.compare(source, source).provide(PdfEngine.live)
        yield assertTrue(
          decoded.nonEmpty,
          text.size == 1,
          text.forall(_.text.isEmpty),
          validated.isSuccess,
          policy.isSuccess,
          digest.nonEmpty,
          compared.isSuccess
        )
      }
    },
    test("browser byte streams expose bounded schema-aware PDF diffs") {
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .diff(ZStream.fromChunk(bytes), ZStream.fromChunk(bytes), PdfDiff.Config(windowSize = 2, maximumCells = 9))
          .runCollect
          .provide(PdfEngine.live)
          .map(windows => assertTrue(windows.nonEmpty, windows.forall(_.exactWithinWindow)))
      }
    },
    test("engine applies the configured input limit before parsing") {
      PdfEngine
        .decode(PdfSource.fromChunk(Chunk.fill(9)(0.toByte)), PdfEngine.Options(maxInputBytes = 8L))
        .either
        .provide(PdfEngine.live)
        .map {
          case Left(PdfEngine.InputTooLarge(limit, observed)) => assertTrue(limit == 8L, observed == 9L)
          case _                                             => assertTrue(false)
        }
    },
    test("collected browser decode enforces its independent materialization limit") {
      val limit = ByteLimit.fromBytes(8L).toOption.get
      PdfEngine
        .decode(
          PdfSource.fromChunk(Chunk.fill(9)(0.toByte)),
          PdfEngine.Options(maxMaterializedDocumentBytes = limit)
        )
        .either
        .provide(PdfEngine.live)
        .map {
          case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
            assertTrue(observed == 9L)
          case _ => assertTrue(false)
        }
    },
    test("Scala.js write helpers linearize and merge caller-owned bytes") {
      minimalPdfBytes.flatMap { bytes =>
        for
          linearized <- PdfEngine.linearize(bytes)
          merged     <- PdfEngine.mergeBytes(NonEmptyChunk(bytes, bytes)).provide(PdfEngine.live)
          trailer    <- ZIO.fromEither(PdfAppend.trailerFromTail(bytes))
        yield assertTrue(
          linearized.nonEmpty,
          merged.size > bytes.size,
          trailer.size.toLong > 0L
        )
      }
    },
    test("Scala.js append fails before rewrite when the base exceeds ByteLimit") {
      minimalPdfBytes.flatMap { bytes =>
        val limit = ByteLimit.fromBytes(8L).toOption.get
        PdfEngine
          .appendRevision(
            bytes,
            Chunk(Part.Meta(Trailer(BigDecimal(6), Prim.dict(), None))),
            PdfEngine.Options(maxMaterializedDocumentBytes = limit)
          )
          .either
          .map {
            case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
              assertTrue(observed == bytes.size.toLong)
            case _ =>
              assertTrue(false)
          }
      }
    },
    test("Scala.js linearize fails before decode when the document exceeds ByteLimit") {
      minimalPdfBytes.flatMap { bytes =>
        val limit = ByteLimit.fromBytes(8L).toOption.get
        PdfEngine
          .linearize(bytes, PdfEngine.Options(maxMaterializedDocumentBytes = limit))
          .either
          .map {
            case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
              assertTrue(observed == bytes.size.toLong)
            case _ =>
              assertTrue(false)
          }
      }
    },
    test("Scala.js watermark stamps Helvetica text onto caller-owned bytes") {
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .watermark(bytes, PdfWatermark.Text("FILED"))
          .provide(PdfEngine.live)
          .map { stamped =>
            val text = new String(stamped.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
            assertTrue(stamped.nonEmpty, text.contains("(FILED)"), text.contains("/Helv"))
          }
      }
    },
    test("Scala.js applyPrep round-trips a schema program and stamps a date") {
      val program = PdfPrep.Program.of(
        PdfPrep.Op.DateStamp(PdfPrep.StampDate(PdfPrep.DateSource.Fixed("2026-09-04"), pattern = "yyyy-MM-dd"))
      )
      val json = PdfPrep.toJson(program)
      minimalPdfBytes.flatMap { bytes =>
        ZIO.fromEither(PdfPrep.fromJson(json)).flatMap { decoded =>
          PdfEngine
            .applyPrep(bytes, decoded)
            .provide(PdfEngine.live)
            .map { stamped =>
              val text = new String(stamped.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
              assertTrue(decoded == program, text.contains("(2026-09-04)"))
            }
        }
      }
    },
    test("Scala.js image watermark embeds an Image XObject") {
      val pixels = Chunk.fromArray(Array.fill[Byte](64)(0x70.toByte))
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .watermark(bytes, PdfWatermark.GrayImage(width = 8, height = 8, pixels = pixels))
          .provide(PdfEngine.live)
          .map { stamped =>
            val text = new String(stamped.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
            assertTrue(stamped.nonEmpty, text.contains("/WmImg Do"), text.contains("/DeviceGray"))
          }
      }
    },
    test("Scala.js extract, split, and rotate page helpers") {
      minimalPdfBytes.flatMap { bytes =>
        for
          merged   <- PdfEngine.mergeBytes(NonEmptyChunk(bytes, bytes)).provide(PdfEngine.live)
          extracted <- PdfEngine.extractPages(merged, 1, 1).provide(PdfEngine.live)
          split    <- PdfEngine.splitPages(merged).provide(PdfEngine.live)
          rotated  <- PdfEngine.rotatePages(merged, 180, 1, 2).provide(PdfEngine.live)
        yield assertTrue(
          extracted.nonEmpty,
          split.size == 2,
          rotated.nonEmpty
        )
      }
    },
    test("Scala.js BlocksLift and MpscMailbox stay on the published JS path") {
      val mailbox = BlocksLift.MpscMailbox[String](8)
      val reader  = zio.blocks.streams.io.Reader.fromChunk(zio.blocks.chunk.Chunk(1, 2, 3))
      for
        pulled <- BlocksLift.fromReader(reader, -1).runCollect
        _      <- mailbox.offerZIO("ok")
        got    <- mailbox.pollZIO
      yield assertTrue(pulled == Chunk(1, 2, 3), got.contains("ok"))
    },
    test("browser byte-stream decode is independent of collected-document limits") {
      val limit = ByteLimit.fromBytes(8L).toOption.get
      minimalPdfBytes.flatMap { bytes =>
        PdfEngine
          .decode(
            ZStream.fromChunk(bytes),
            PdfEngine.Options(maxMaterializedDocumentBytes = limit)
          )
          .runDrain
          .as(assertTrue(bytes.length > limit.bytes))
          .provide(PdfEngine.live)
      }
    }
  )
