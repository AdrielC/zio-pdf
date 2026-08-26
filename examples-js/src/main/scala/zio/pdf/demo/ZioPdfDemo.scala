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
      case Nil          => "PdfTransform.Plan.empty"
      case head :: tail => (head :: tail.map(operation => s">>> $operation")).mkString("\n  ")

  private def literal(value: String): String =
    s"\"${value.replace("\\", "\\\\").replace("\"", "\\\"")}\""

  @JSExport
  def analyze(input: Uint8Array): js.Promise[js.Dictionary[js.Any]] =
    analyzeSource(PdfSource.fromUint8Array(input), _ => ())

  /**
   * Streams a browser `File` / `Blob` through `PdfSource` without an
   * `arrayBuffer()` copy at the JavaScript boundary.
   */
  @JSExport
  def analyzeBlob(input: dom.Blob): js.Promise[js.Dictionary[js.Any]] =
    analyzeSource(PdfSource.fromBlob(input), _ => ())

  /** Emit actual phase boundaries while a browser source is being consumed. */
  @JSExport
  def analyzeBlobWithProgress(
    input: dom.Blob,
    progress: js.Function1[String, Unit]
  ): js.Promise[js.Dictionary[js.Any]] =
    analyzeSource(PdfSource.fromBlob(input), phase => progress(phase))

  private def analyzeSource(
    source: PdfSource,
    progress: String => Unit
  ): js.Promise[js.Dictionary[js.Any]] =
    def phase(name: String): UIO[Unit] = ZIO.succeed(progress(name))

    val startedAt = js.Date.now()
    val effect =
      for
        _      <- phase("evidence")
        bundle <- PdfEngine.evidence(source, browserPlan)
        _      <- phase("complete")
      yield js.Dictionary(
        "inspection" -> inspectionJson(bundle.inspection).asInstanceOf[js.Any],
        "content" -> contentJson(bundle).asInstanceOf[js.Any],
        "valid" -> bundle.validation.isSuccess.asInstanceOf[js.Any],
        "strictPolicyPassed" -> bundle.policy.isSuccess.asInstanceOf[js.Any],
        "sha256" -> bundle.sha256Hex.asInstanceOf[js.Any],
        "decodedEvents" -> bundle.decodedEvents.toDouble.asInstanceOf[js.Any],
        "elapsedMs" -> (js.Date.now() - startedAt).asInstanceOf[js.Any]
      )

    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe
        .runToFuture(effect.provide(PdfEngine.live))
        .toJSPromise
    }

  private def inspectionJson(outcome: PdfInspection.Outcome): js.Dictionary[js.Any] =
    val (report, status, violation) = outcome match
      case PdfInspection.Outcome.Accepted(value) => (value, "accepted", js.undefined)
      case PdfInspection.Outcome.Rejected(value, PdfInspection.Violation.JavaScript(found)) =>
        (value, "rejected", s"JavaScript in object ${found.objectNumber}")

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
