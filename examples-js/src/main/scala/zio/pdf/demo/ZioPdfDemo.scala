package zio.pdf.demo

import scala.concurrent.ExecutionContext.Implicits.global
import scala.scalajs.js
import scala.scalajs.js.JSConverters.*
import scala.scalajs.js.annotation.{JSExport, JSExportTopLevel}
import scala.scalajs.js.typedarray.Uint8Array
import org.scalajs.dom
import zio.*
import zio.pdf.*

/** Minimal JavaScript boundary for the Vite example. The PDF work remains in Scala.js. */
@JSExportTopLevel("ZioPdfDemo")
object ZioPdfDemo:

  private val browserPlan = PdfEvidence.Plan.browser
  private val browserTransformLimit = ByteLimit.mebibytes(64)
  private val browserTransformOptions = PdfEngine.Options(
    maxInputBytes = browserTransformLimit.toLong,
    maxMaterializedDocumentBytes = browserTransformLimit
  )
  private val outputChunkBytes = 64 * 1024
  /**
   * A plan snapshot for the browser inspector. The values come from the same
   * public immutable plan that a JVM consumer can analyze or reinterpret.
   */
  @JSExport
  def inspectTransformPlan(
    remapExistingFonts: Boolean,
    sourceFont: String,
    targetFont: String,
    tokenize: Boolean,
    tokenizer: String
  ): js.Dictionary[js.Any] =
    val remap = PdfTransform.fonts.replaceExisting(sourceFont, targetFont).program
    val tokens =
      if tokenizer == "words" then PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.words).program
      else PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.characters).program
    val plan: PdfTransform.Plan =
      (remapExistingFonts, tokenize) match
        case (true, true)   => remap.andThen(tokens)
        case (true, false)  => remap
        case (false, true)  => tokens
        case (false, false) => PdfTransform.Plan.empty
    val profile = PdfTransform.profile(plan)
    js.Dictionary(
      "operations" -> profile.operations.toSeq.toJSArray,
      "requiresMaterializedDocument" -> profile.requiresMaterializedDocument,
      "readsContentStreams" -> profile.readsContentStreams,
      "code" -> source(remapExistingFonts, sourceFont, targetFont, tokenize, tokenizer)
    )

  /**
   * Execute the same typed plan shown by the browser builder and return
   * bounded output chunks for a Blob download. Document transforms retain the
   * decoded graph, so the browser boundary is explicitly capped at 64 MiB.
   */
  @JSExport
  def executeTransformBlob(
    input: dom.Blob,
    remapExistingFonts: Boolean,
    sourceFont: String,
    targetFont: String,
    tokenize: Boolean,
    tokenizer: String
  ): js.Promise[js.Dictionary[js.Any]] =
    val tokenProgram =
      if tokenizer == "words" then
        PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.words).map(tokenStats)
      else PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.characters).map(tokenStats)
    val remapProgram = PdfTransform.fonts.replaceExisting(sourceFont, targetFont)
    val sourceBytes  = PdfSource.fromBlob(input).bytes

    val prepared =
      if input.size > browserTransformLimit.toLong then
        ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(browserTransformLimit, input.size.toLong))
      else
        (remapExistingFonts, tokenize) match
          case (true, true) =>
            remapProgram.andThen(tokenProgram).run(sourceBytes, browserTransformOptions).map { output =>
              val (replacement, (tokenPages, tokenCount)) = output.value
              (transformSummary(Some(replacement), tokenPages, tokenCount), output.bytes)
            }
          case (true, false) =>
            remapProgram.run(sourceBytes, browserTransformOptions).map { output =>
              (transformSummary(Some(output.value), 0, 0L), output.bytes)
            }
          case (false, true) =>
            tokenProgram.run(sourceBytes, browserTransformOptions).map { output =>
              val (tokenPages, tokenCount) = output.value
              (transformSummary(None, tokenPages, tokenCount), output.bytes)
            }
          case (false, false) =>
            ZIO.fail(new IllegalArgumentException("select at least one transform operation"))

    val effect =
      prepared.flatMap { case (summary, output) =>
        output
          .rechunk(outputChunkBytes)
          .chunks
          .mapAccumZIO(0L) { (seen, chunk) =>
            val next = seen + chunk.length.toLong
            if next > browserTransformLimit.toLong then
              ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(browserTransformLimit, next))
            else ZIO.succeed((next, JsBinary.uint8(chunk, 0, chunk.length)))
          }
          .runCollect
          .map { chunks =>
            val outputBytes = chunks.foldLeft(0L)((total, chunk) => total + chunk.length.toLong)
            summary("chunks") = chunks.toSeq.toJSArray
            summary("outputBytes") = outputBytes.toDouble
            summary("maxMaterializedBytes") = browserTransformLimit.bytes.toDouble
            summary
          }
      }

    val browserEffect =
      effect.mapError(browserTransformError)

    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe
        .runToFuture(browserEffect.provide(PdfEngine.live))
        .toJSPromise
    }

  private def browserTransformError(error: Throwable): Throwable =
    val message = error match
      case PdfTransform.Error.IncompatibleFont(_, _, field) =>
        val mismatch = field match
          case "DW" | "W" | "DW2" | "W2" | "Widths" => "different glyph widths"
          case "Encoding"                                 => "different character encodings"
          case "ToUnicode"                                => "different Unicode mappings"
          case "CIDToGIDMap"                              => "different character-to-glyph mappings"
          case "CIDSystemInfo"                            => "different CID character collections"
          case "Subtype" | "DescendantFonts/Subtype"    => "different font formats"
          case _                                          => s"incompatible /$field font data"
        s"These fonts use $mismatch, so swapping them could change the document's text or layout. " +
          "Choose another replacement font or turn off font replacement. No output PDF was created."
      case PdfTransform.Error.MetricsUnavailable(_, _, _) =>
        "This PDF does not expose enough font metrics to prove a safe replacement. " +
          "Choose another font pair or turn off font replacement. No output PDF was created."
      case PdfTransform.Error.UnsupportedFontSubtype(_, _, _, _) =>
        "This font requires glyph re-encoding and embedding, which Replace existing fonts does not perform. " +
          "Choose another font pair or turn off font replacement. No output PDF was created."
      case PdfTransform.Error.InvalidToUnicode(_, _) =>
        "This font's Unicode map could not be verified, so zio-pdf refused to change the document. " +
          "Choose another font pair or turn off font replacement. No output PDF was created."
      case PdfTransform.Error.CompositeFontDataUnavailable(_, _) =>
        "This composite font does not contain enough linked font data to prove a safe replacement. " +
          "Choose another font pair or turn off font replacement. No output PDF was created."
      case PdfTransform.Error.AmbiguousTargetFont(_, _) =>
        "This font name points to more than one replacement resource. Choose an unambiguous font. " +
          "No output PDF was created."
      case PdfTransform.Error.SourceFontNotFound(_) | PdfTransform.Error.TargetFontNotFound(_) =>
        "The selected font resource is no longer available in this document. Run inspection again and choose another font."
      case _ => Option(error.getMessage).getOrElse("The PDF pipeline could not run.")

    new IllegalArgumentException(message, error)

  private def tokenStats[A](pages: Chunk[PdfTransform.text.PageTokens[A]]): (Int, Long) =
    (pages.length, pages.foldLeft(0L)((total, page) => total + page.tokens.length.toLong))

  private def transformSummary(
    replacement: Option[PdfTransform.fonts.Replacement],
    tokenPages: Int,
    tokenCount: Long
  ): js.Dictionary[js.Any] =
    js.Dictionary(
      "sourceFont" -> replacement.fold[js.Any](js.undefined)(_.sourceBaseFont),
      "targetFont" -> replacement.fold[js.Any](js.undefined)(_.targetBaseFont),
      "sourceObjectNumbers" -> replacement
        .fold(js.Array[Double]())(_.sourceObjectNumbers.map(_.toDouble).toSeq.toJSArray),
      "targetObjectNumber" -> replacement.fold[js.Any](js.undefined)(_.targetObjectNumber.toDouble),
      "resourceBindingsRewritten" -> replacement.fold(0d)(_.resourceBindingsRewritten.toDouble),
      "tokenPages" -> tokenPages.toDouble,
      "tokenCount" -> tokenCount.toDouble
    )

  private def source(
    remapExistingFonts: Boolean,
    sourceFont: String,
    targetFont: String,
    tokenize: Boolean,
    tokenizer: String
  ): String =
    val operations = List(
      Option.when(remapExistingFonts)(s"PdfTransform.fonts.replaceExisting(${literal(sourceFont)}, ${literal(targetFont)})"),
      Option.when(tokenize)(s"PdfTransform.text.tokenize(Tokenizer.$tokenizer)")
    ).flatten
    operations match
      case Nil                => "PdfTransform.Plan.empty"
      case remap :: token :: Nil => s"$remap\n  .andThen($token)"
      case head :: Nil        => head
      case head :: tail       => (head :: tail.map(operation => s">>> $operation")).mkString("\n  ")

  private def literal(value: String): String =
    s"\"${value.replace("\\", "\\\\").replace("\"", "\\\"")}\""

  @JSExport
  def analyze(input: Uint8Array): js.Promise[js.Dictionary[js.Any]] =
    analyzeSource(PdfSource.fromUint8Array(input), input.length.toLong, (_, _, _) => ())

  /**
   * Streams a browser `File` / `Blob` through `PdfSource` without an
   * `arrayBuffer()` copy at the JavaScript boundary.
   */
  @JSExport
  def analyzeBlob(input: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    analyzeSource(PdfSource.fromBlob(input), input.size.toLong, (_, _, _) => ())

  /**
   * Attach a first-page `/Thumb` to PDF bytes.
   *
   * - omit `grayPixels` for deterministic placeholders (Scala.js-safe)
   * - pass PDF.js canvas grayscale bytes for a rendered preview tile
   */
  @JSExport
  def attachFirstPageThumbnail(
    input: Uint8Array,
    grayPixels: js.UndefOr[Uint8Array],
    width: Int,
    height: Int
  ): js.Promise[Uint8Array] =
    val source = Chunk.fromArray(JsBinary.bytes(input))
    val options =
      if grayPixels.isDefined then
        val gray = JsBinary.bytes(grayPixels.get)
        PdfThumbnail.renderedOptions(
          (_, w, h) =>
            val need = w * h
            if gray.length >= need then Right(gray.take(need))
            else Left(s"thumbnail pixels: expected $need bytes, got ${gray.length}"),
          width = width,
          height = height
        )
      else
        PdfThumbnail.placeholderOptions(width = width, height = height)

    val effect =
      PdfThumbnail.enrichBytes(source, options).map { output =>
        JsBinary.uint8(output, 0, output.length)
      }

    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe
        .runToFuture(effect)
        .toJSPromise
    }

  /** Emit actual phase and byte boundaries while a browser source is consumed. */
  @JSExport
  def analyzeBlobWithProgress(
    input: dom.Blob,
    progress: js.Function3[String, Double, Double, Unit]
  ): js.Promise[js.Dictionary[js.Any]] =
    analyzeSource(
      PdfSource.fromBlob(input),
      input.size.toLong,
      (phase, loaded, total) => progress(phase, loaded.toDouble, total.toDouble)
    )

  private def analyzeSource(
    source: PdfSource,
    totalBytes: Long,
    progress: (String, Long, Long) => Unit
  ): js.Promise[js.Dictionary[js.Any]] =
    var loadedBytes = 0L
    val observedSource = new PdfSource:
      def bytes =
        source.bytes.mapChunksZIO { chunk =>
          loadedBytes += chunk.length.toLong
          ZIO.succeed(progress("evidence", loadedBytes, totalBytes)).as(chunk)
        }

    def phase(name: String): UIO[Unit] = ZIO.succeed(progress(name, loadedBytes, totalBytes))

    val startedAt = js.Date.now()
    val effect =
      for
        _      <- phase("evidence")
        bundle <- PdfEngine.evidence(observedSource, browserPlan)
        _      <- phase("complete")
      yield js.Dictionary(
        "inspection" -> inspectionJson(bundle.inspection).asInstanceOf[js.Any],
        "content" -> contentJson(bundle).asInstanceOf[js.Any],
        "valid" -> bundle.validation.isSuccess.asInstanceOf[js.Any],
        "strictPolicyPassed" -> bundle.policy.isSuccess.asInstanceOf[js.Any],
        "cannotProcess" -> bundle.cannotProcess.asInstanceOf[js.Any],
        "processingBlockers" -> bundle.processingBlockers.map { blocker =>
          val reference = blocker match
            case PdfEvidence.ProcessingBlocker.Encrypted(value) => value
          js.Dictionary[js.Any](
            "kind" -> "Encrypted",
            "objectNumber" -> reference.fold[js.Any](js.undefined)(_.number.toDouble),
            "reason" -> blocker.reason
          )
        }.toSeq.toJSArray.asInstanceOf[js.Any],
        "sha256" -> bundle.sha256Hex.asInstanceOf[js.Any],
        "decodedEvents" -> bundle.decodedEvents.toDouble.asInstanceOf[js.Any],
        "elapsedMs" -> (js.Date.now() - startedAt).asInstanceOf[js.Any]
      )

    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe
        .runToFuture(effect.provide(PdfEngine.live))
        .toJSPromise
    }

  @JSExport
  def linearizeBlob(input: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    runWorkflow(input) { bytes =>
      PdfEngine.linearize(bytes, browserTransformOptions).map { output =>
        val prefix = PdfLinearize.firstPageByteLength(output).toOption
        workflowSummary("linearize", output, prefix)
      }
    }

  @JSExport
  def mergeBlobs(primary: dom.Blob, secondary: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    val effect =
      admitBlob(primary).zip(admitBlob(secondary)).flatMap { (left, right) =>
        PdfEngine
          .mergeBytes(NonEmptyChunk(left, right), browserTransformOptions)
          .flatMap(output => boundedWorkflow("merge", output))
      }
    runBrowser(effect)

  @JSExport
  def appendRevisionBlob(input: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    runWorkflow(input) { bytes =>
      val revision = Chunk(
        Part.Obj(IndirectObj.nostream(99L, Prim.dict("Producer" -> Prim.Name("zio-pdf")))),
        Part.Meta(Trailer(BigDecimal(100), Prim.dict("Info" -> Prim.Ref(99L, 0)), None))
      )
      PdfEngine.appendRevision(bytes, revision).map(output => workflowSummary("append", output, None))
    }

  @JSExport
  def flattenFormsBlob(input: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    runWorkflow(input) { bytes =>
      PdfEngine.flattenForms(bytes, browserTransformOptions).map(output => workflowSummary("flatten", output, None))
    }

  @JSExport
  def extractPagesBlob(input: dom.Blob, fromPage: Int, toPage: Int): js.Promise[js.Dictionary[js.Any]] =
    runWorkflow(input) { bytes =>
      PdfEngine.extractPages(bytes, fromPage, toPage, browserTransformOptions).map { output =>
        workflowSummary("extract", output, None)
      }
    }

  @JSExport
  def rotatePagesBlob(input: dom.Blob, degrees: Int, fromPage: Int, toPage: Int): js.Promise[js.Dictionary[js.Any]] =
    runWorkflow(input) { bytes =>
      PdfEngine.rotatePages(bytes, degrees, fromPage, toPage, browserTransformOptions).map { output =>
        workflowSummary("rotate", output, None)
      }
    }

  @JSExport
  def splitPagesBlob(input: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    runWorkflow(input) { bytes =>
      PdfEngine.splitPages(bytes, browserTransformOptions).map { pdfs =>
        val documents = pdfs.zipWithIndex.map { (pdf, index) =>
          js.Dictionary[js.Any](
            "name" -> s"page-${index + 1}.pdf",
            "chunks" -> chunked(pdf),
            "outputBytes" -> pdf.size.toDouble
          )
        }
        val first = pdfs.head
        val summary = workflowSummary("split", first, None)
        summary("documents") = documents.toSeq.toJSArray
        summary("pageCount") = pdfs.size.toDouble
        summary
      }
    }

  private def admitBlob(input: dom.Blob): Task[Chunk[Byte]] =
    if input.size > browserTransformLimit.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(browserTransformLimit, input.size.toLong))
    else
      PdfSource.fromBlob(input).bytes.runCollect.flatMap { bytes =>
        if bytes.size.toLong > browserTransformLimit.toLong then
          ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(browserTransformLimit, bytes.size.toLong))
        else ZIO.succeed(bytes)
      }

  private def runWorkflow(input: dom.Blob)(
    run: Chunk[Byte] => ZIO[PdfEngine, Throwable, js.Dictionary[js.Any]]
  ): js.Promise[js.Dictionary[js.Any]] =
    runBrowser(
      admitBlob(input).flatMap(run).flatMap { summary =>
        val outputBytes = summary.get("outputBytes").fold(0L)(_.asInstanceOf[Double].toLong)
        if outputBytes > browserTransformLimit.toLong then
          ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(browserTransformLimit, outputBytes))
        else ZIO.succeed(summary)
      }
    )

  private def chunked(output: Chunk[Byte]): js.Array[Uint8Array] =
    output
      .grouped(outputChunkBytes)
      .map(chunk => JsBinary.uint8(chunk, 0, chunk.length))
      .toSeq
      .toJSArray

  private def workflowSummary(
    kind: String,
    output: Chunk[Byte],
    firstPagePrefix: Option[Long]
  ): js.Dictionary[js.Any] =
    val chunks = chunked(output)
    val summary = js.Dictionary[js.Any](
      "kind" -> kind,
      "chunks" -> chunks,
      "outputBytes" -> output.size.toDouble,
      "maxMaterializedBytes" -> browserTransformLimit.bytes.toDouble
    )
    firstPagePrefix.foreach(value => summary("firstPagePrefixBytes") = value.toDouble)
    summary

  private def boundedWorkflow(kind: String, output: Chunk[Byte]): Task[js.Dictionary[js.Any]] =
    if output.size.toLong > browserTransformLimit.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(browserTransformLimit, output.size.toLong))
    else ZIO.succeed(workflowSummary(kind, output, None))

  private def runBrowser(effect: ZIO[PdfEngine, Throwable, js.Dictionary[js.Any]]): js.Promise[js.Dictionary[js.Any]] =
    val browserEffect = effect.mapError(browserTransformError)
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe
        .runToFuture(browserEffect.provide(PdfEngine.live))
        .toJSPromise
    }

  private def inspectionJson(outcome: PdfInspection.Outcome): js.Dictionary[js.Any] =
    val (report, status, violation) = outcome match
      case PdfInspection.Outcome.Accepted(value) => (value, "accepted", js.undefined)
      case PdfInspection.Outcome.Rejected(value, PdfInspection.Violation.JavaScript(found)) =>
        (value, "rejected", s"JavaScript in object ${found.objectNumber}")
      case PdfInspection.Outcome.Rejected(value, PdfInspection.Violation.Encrypted(found)) =>
        (
          value,
          "rejected",
          found.reference.fold("encrypted trailer")(ref => s"encrypted object ${ref.number}")
        )

    js.Dictionary(
      "status" -> status,
      "violation" -> violation,
      "completion" -> report.completion.toString,
      "elementsRead" -> report.elementsRead.toDouble,
      "linearizedObject" -> report.linearization.fold[js.Any](js.undefined)(_.objectNumber.toDouble),
      "pdfAObject" -> report.pdfA.fold[js.Any](js.undefined)(_.metadataObjectNumber.toDouble),
      "pdfAPart" -> report.pdfA.flatMap(_.part).fold[js.Any](js.undefined)(identity),
      "pdfAConformance" -> report.pdfA.flatMap(_.conformance).fold[js.Any](js.undefined)(identity),
      "pdfA3bDeclared" -> report.pdfA.exists(_.declaresA3b),
      "thumbnailPageObject" -> report.thumbnail.fold[js.Any](js.undefined)(_.pageObjectNumber.toDouble),
      "thumbnailImageObject" -> report.thumbnail.fold[js.Any](js.undefined)(_.image.number.toDouble),
      "encrypted" -> report.encryption.nonEmpty,
      "encryptionObject" -> report.encryption.flatMap(_.reference).fold[js.Any](js.undefined)(_.number.toDouble),
      "javaScriptObject" -> report.javaScript.fold[js.Any](js.undefined)(_.objectNumber.toDouble),
      "acroFormObject" -> report.acroForm.fold[js.Any](js.undefined)(_.objectNumber.toDouble),
      "acroFormFields" -> report.acroForm.fold(0d)(_.fieldCount.toDouble),
      "acroFormNeedAppearances" -> report.acroForm.exists(_.needAppearances),
      "fonts" -> report.fonts.map { font =>
        js.Dictionary(
          "objectNumber" -> font.objectNumber.toDouble,
          "baseFont" -> font.baseFont,
          "subtype" -> font.subtype.fold[js.Any](js.undefined)(identity),
          "existingResourceRemapCandidate" -> font.isExistingResourceRemapCandidate
        )
      }.toSeq.toJSArray
    )

  private def contentJson(bundle: PdfEvidence.Bundle): js.Dictionary[js.Any] =
    val report = bundle.inspection match
      case PdfInspection.Outcome.Accepted(value)     => value
      case PdfInspection.Outcome.Rejected(value, _)  => value
    val text = bundle.nativeText

    js.Dictionary(
      "images" -> report.imageCount.toDouble,
      "attachments" -> report.attachmentCount.toDouble,
      "tableCandidates" -> report.tableCandidateCount.toDouble,
      "imageEvidence" -> report.imageCountEvidence.reason,
      "attachmentEvidence" -> report.attachmentCountEvidence.reason,
      "tableCandidateEvidence" -> report.tableCandidateCountEvidence.reason,
      "pages" -> text.pages.toDouble,
      "textPages" -> text.textPages.toDouble,
      "textCharacters" -> text.characters.toDouble,
      "textPreview" -> text.preview,
      "citations" -> bundle.citations.iterator.map { citation =>
        js.Dictionary(
          "id" -> citation.id,
          "page" -> citation.location.pageNumber.toDouble,
          "pageObjectNumber" -> citation.location.pageObjectNumber.toDouble,
          "contentObjectNumbers" -> citation.location.contentObjectNumbers.map(_.toDouble).toSeq.toJSArray,
          "excerpt" -> citation.excerpt,
          "truncated" -> citation.truncated
        )
      }.toSeq.toJSArray,
      "textRecoveryRequests" -> bundle.textRecoveryRequests.iterator.map { request =>
        js.Dictionary(
          "page" -> request.location.pageNumber.toDouble,
          "pageObjectNumber" -> request.location.pageObjectNumber.toDouble,
          "contentObjectNumbers" -> request.location.contentObjectNumbers.map(_.toDouble).toSeq.toJSArray,
          "reason" -> request.reason.toString
        )
      }.toSeq.toJSArray
    )
