package zio.pdf

import zio.{Chunk, ZIO}
import zio.prelude.Validation
import zio.stream.ZStream

/**
 * A provenance-first result assembled from one decoded PDF traversal.
 *
 * The plan is deliberately a product of independent observations: the typed
 * inspection arrow, native text accumulator, structural validator, policy,
 * and raw-byte digest all see the same input. `PdfEngine.evidence` is the
 * fused interpreter for that product; calling the individual convenience
 * verbs remains useful when a caller intentionally needs only one result.
 */
object PdfEvidence:

  /** Controls how much native text the result retains after counting it. */
  enum TextMode:
    /** Skip native text recovery entirely. */
    case None
    /** Retain totals and a bounded preview, but not every page string. */
    case Summary(previewCharacters: Int)
    /** Retain page-provenanced text as evidence plus a bounded preview. */
    case Pages(previewCharacters: Int)
    /**
     * Retain a bounded native-text excerpt for every logical page.
     *
     * This is useful for interactive review surfaces that need citations but
     * must not retain a second complete text-layer copy of a large PDF.
     */
    case Citations(previewCharacters: Int, pageExcerptCharacters: Int)

  object TextMode:
    val browser: TextMode = Citations(previewCharacters = 480, pageExcerptCharacters = 360)
    val full: TextMode    = Pages(previewCharacters = 480)

  /**
   * A complete evidence request. Inspection remains an ordinary composable
   * inspection plan; this only chooses which other independent facts to retain.
   */
  final case class Plan(
    inspection: PdfInspection.Plan = PdfInspection.documentProfile,
    policy: Policy = PdfPolicy.strict,
    text: TextMode = TextMode.full
  ):
    text match
      case TextMode.Summary(limit) => require(limit >= 0, "text preview length must be non-negative")
      case TextMode.Pages(limit)   => require(limit >= 0, "text preview length must be non-negative")
      case TextMode.Citations(preview, excerpt) =>
        require(preview >= 0, "text preview length must be non-negative")
        require(excerpt >= 0, "page citation excerpt length must be non-negative")
      case TextMode.None => ()

  object Plan:
    /** Court/DMS form: retain every page's native text and source objects. */
    val complete: Plan = Plan()
    /** Browser form: the same scan, without retaining a second full text copy. */
    val browser: Plan = Plan(text = TextMode.browser)

  /**
   * A stable, document-local location that another interpreter can use without
   * parsing the PDF again. The page number is logical page order, while the
   * object references make the claim auditable against the decoded document.
   */
  final case class PageLocation(
    pageNumber: Long,
    pageObjectNumber: Long,
    contentObjectNumbers: Chunk[Long]
  )

  /** Native text tied to the logical page and the content streams that produced it. */
  final case class Page(
    pageObjectNumber: Long,
    contentObjectNumbers: Chunk[Long],
    text: String,
    truncated: Boolean = false,
    pageNumber: Long = 0L,
    nativeTextRecovered: Boolean = false
  ):
    def hasNativeText: Boolean = nativeTextRecovered || text.nonEmpty

    def location: PageLocation =
      PageLocation(pageNumber, pageObjectNumber, contentObjectNumbers)

  /** A native-text citation with a deterministic identifier scoped to its document digest. */
  final case class Citation(
    id: String,
    location: PageLocation,
    excerpt: String,
    truncated: Boolean
  )

  /**
   * Explicit handoff point for a caller-owned recovery interpreter such as
   * OCR, a layout model, or a human-review queue. Core intentionally carries
   * no renderer or OCR dependency and never silently consumes the PDF again.
   */
  object TextRecovery:
    enum Reason:
      case NoUsableNativeText

    final case class Request(location: PageLocation, reason: Reason)
    final case class Recovered[+A](request: Request, value: A)

  /**
   * Text totals are available for every mode. `retainedPages` is populated
   * only by [[TextMode.Pages]] and [[TextMode.Citations]]. Citation mode keeps
   * browser review bounded with a short excerpt per page rather than an
   * additional full text-layer copy.
   */
  final case class NativeText(
    pages: Long,
    textPages: Long,
    characters: Long,
    preview: String,
    retainedPages: Chunk[Page],
    retainsPages: Boolean
  ):
    def isEmpty: Boolean = textPages == 0L

    /** Page-scoped native citations in logical document order. */
    def citations(documentDigest: Chunk[Byte]): Chunk[Citation] =
      val digest = hex(documentDigest)
      retainedPages.collect {
        case page if page.hasNativeText && page.text.nonEmpty =>
          Citation(
            id = s"pdf:$digest:page:${page.pageNumber}",
            location = page.location,
            excerpt = page.text,
            truncated = page.truncated
          )
      }

    /**
     * Page-scoped recovery work, available when the plan retained pages.
     * An empty result means either every retained page has native text or the
     * selected text mode intentionally did not retain page-level provenance.
     */
    def textRecoveryRequests: Chunk[TextRecovery.Request] =
      retainedPages.collect {
        case page if !page.hasNativeText =>
          TextRecovery.Request(page.location, TextRecovery.Reason.NoUsableNativeText)
      }

    /**
     * Runs a caller-owned recovery action only for pages whose native text was
     * absent. This never re-reads the PDF; the request identifies the already
     * decoded page for the caller's renderer, OCR service, or review queue.
     */
    def recoverMissing[R, E, A](
      recover: TextRecovery.Request => ZIO[R, E, A]
    ): ZIO[R, E, Chunk[TextRecovery.Recovered[A]]] =
      ZIO.foreach(textRecoveryRequests) { request =>
        recover(request).map(TextRecovery.Recovered(request, _))
      }

  /** The deterministic, serializable result of an evidence run. */
  final case class Bundle(
    sha256: Chunk[Byte],
    inspection: PdfInspection.Outcome,
    validation: Validation[PdfError, Unit],
    policy: Validation[PolicyViolation, Unit],
    nativeText: NativeText,
    decodedEvents: Long
  ):
    def sha256Hex: String = hex(sha256)
    def citations: Chunk[Citation] = nativeText.citations(sha256)
    def textRecoveryRequests: Chunk[TextRecovery.Request] = nativeText.textRecoveryRequests

    def recoverMissingText[R, E, A](
      recover: TextRecovery.Request => ZIO[R, E, A]
    ): ZIO[R, E, Chunk[TextRecovery.Recovered[A]]] =
      nativeText.recoverMissing(recover)

    /**
     * Stable JSON for evidence storage and reproducible review artifacts.
     * It preserves page/object provenance and never relies on map iteration
     * order or `toString` of internal PDF model values.
     */
    def canonicalJson: String = CanonicalJson.bundle(this)

  /**
   * Mutable only at the fused interpreter boundary. No state escapes this
   * object: callers receive an immutable [[Bundle]].
   */
  private[pdf] final class Accumulator private[pdf] (plan: Plan):
    private val inspection = PdfInspection.accumulator(plan.inspection, drainOnViolation = true)
    private val assembled  = AssemblePdf.accumulator()
    private var text       = TextExtract.Acc()
    private var decoded    = 0L

    def add(value: Decoded): Unit =
      assembled.add(value)
      decoded += 1L
      Elements.classifyOne(value) match
        case Left(error) => throw error
        case Right(element) =>
          inspection.add(element)
          plan.text match
            case TextMode.None => ()
            case _             => text = TextExtract.fold(text, element)

    def result(digest: Chunk[Byte]): Bundle =
      val assembledResult = assembled.result
      Bundle(
        sha256 = digest,
        inspection = inspection.result,
        validation = ValidatePdf.fromAssembly(assembledResult),
        policy = PdfPolicy.fromAssembly(plan.policy)(assembledResult),
        nativeText = collectNativeText(text, plan.text),
        decodedEvents = decoded
      )

  private[pdf] def accumulator(plan: Plan): Accumulator =
    new Accumulator(plan)

  /** Fold a decoded stream once when a platform has already fused raw-byte digesting. */
  private[pdf] def fromDecoded[R, E](
    decoded: ZStream[R, E, Decoded],
    plan: Plan,
    digest: => Chunk[Byte]
  ): zio.ZIO[R, E, Bundle] =
    val acc = accumulator(plan)
    decoded.runForeach(value => zio.ZIO.succeed(acc.add(value))).map(_ => acc.result(digest))

  private def collectNativeText(acc: TextExtract.Acc, mode: TextMode): NativeText =
    mode match
      case TextMode.None =>
        NativeText(0L, 0L, 0L, "", Chunk.empty, retainsPages = false)
      case TextMode.Summary(limit) =>
        collectPages(acc, limit, retainPages = false)
      case TextMode.Pages(limit) =>
        collectPages(acc, limit, retainPages = true)
      case TextMode.Citations(preview, excerpt) =>
        collectPages(acc, preview, retainPages = true, pageExcerptCharacters = Some(excerpt))

  private def collectPages(
    acc: TextExtract.Acc,
    previewLimit: Int,
    retainPages: Boolean,
    pageExcerptCharacters: Option[Int] = None
  ): NativeText =
    val pages = Chunk.newBuilder[Page]
    var pageCount = 0L
    var textPages = 0L
    var characters = 0L
    val preview = new StringBuilder

    TextExtract.foreachPage(acc) { (pageObjectNumber, contentObjectNumbers, rawText) =>
      pageCount += 1L
      val text = rawText.trim
      val hasNativeText = text.nonEmpty
      if text.nonEmpty then
        textPages += 1L
        characters += text.length.toLong
        if preview.length < previewLimit then
          if preview.nonEmpty then preview.append("\n\n")
          val remaining = previewLimit - preview.length
          if remaining > 0 then preview.append(text.take(remaining))
      if retainPages then
        val retained = pageExcerptCharacters.fold(text)(limit => text.take(limit))
        pages += Page(
          pageObjectNumber = pageObjectNumber,
          contentObjectNumbers = contentObjectNumbers,
          text = retained,
          truncated = retained.length < text.length,
          pageNumber = pageCount,
          nativeTextRecovered = hasNativeText
        )
    }

    NativeText(
      pages = pageCount,
      textPages = textPages,
      characters = characters,
      preview = preview.toString,
      retainedPages = pages.result(),
      retainsPages = retainPages
    )

  private def hex(bytes: Chunk[Byte]): String =
    bytes.iterator.map(byte => f"${byte & 0xff}%02x").mkString

  private object CanonicalJson:
    def bundle(value: Bundle): String =
      val inspection = value.inspection match
        case PdfInspection.Outcome.Accepted(report) => reportJson(report, None)
        case PdfInspection.Outcome.Rejected(report, violation) => reportJson(report, Some(violation))
      s"{" +
        field("sha256", quote(value.sha256Hex)) + "," +
        field("decodedEvents", value.decodedEvents.toString) + "," +
        field("inspection", inspection) + "," +
        field("validation", status(value.validation.isSuccess)) + "," +
        field("policy", status(value.policy.isSuccess)) + "," +
        field("nativeText", textJson(value.nativeText, value.sha256)) +
        "}"

    private def reportJson(report: PdfInspection.Report, violation: Option[PdfInspection.Violation]): String =
      s"{" +
        field("completion", quote(report.completion.toString)) + "," +
        field("elementsRead", report.elementsRead.toString) + "," +
        field("linearization", report.linearization.fold("null")(value => number(value.objectNumber))) + "," +
        field("pdfA", report.pdfA.fold("null")(pdfAJson)) + "," +
        field("thumbnail", report.thumbnail.fold("null")(thumbnailJson)) + "," +
        field("encryption", report.encryption.fold("null")(encryptionJson)) + "," +
        field("javaScript", report.javaScript.fold("null")(value => number(value.objectNumber))) + "," +
        field("fonts", array(report.fonts.iterator.map(fontJson))) + "," +
        field("imageCount", report.imageCount.toString) + "," +
        field("attachmentCount", report.attachmentCount.toString) + "," +
        field("tableCandidateCount", report.tableCandidateCount.toString) + "," +
        field("violation", violation.fold("null")(violationJson)) +
        "}"

    private def pdfAJson(value: PdfInspection.PdfA): String =
      s"{" +
        field("metadataObjectNumber", number(value.metadataObjectNumber)) + "," +
        field("part", optional(value.part)) + "," +
        field("conformance", optional(value.conformance)) +
        "}"

    private def thumbnailJson(value: PdfInspection.Thumbnail): String =
      s"{" +
        field("pageObjectNumber", number(value.pageObjectNumber)) + "," +
        field("imageObjectNumber", number(value.image.number)) +
        "}"

    private def encryptionJson(value: PdfInspection.Encryption): String =
      s"{" + field("objectNumber", value.reference.fold("null")(ref => number(ref.number))) + "}"

    private def fontJson(value: PdfInspection.Font): String =
      s"{" +
        field("objectNumber", number(value.objectNumber)) + "," +
        field("baseFont", quote(value.baseFont)) + "," +
        field("subtype", optional(value.subtype)) + "," +
        field("existingResourceRemapCandidate", value.isExistingResourceRemapCandidate.toString) +
        "}"

    private def violationJson(value: PdfInspection.Violation): String =
      value match
        case PdfInspection.Violation.JavaScript(found) =>
          s"{" + field("kind", quote("JavaScript")) + "," + field("objectNumber", number(found.objectNumber)) + "}"

    private def textJson(value: NativeText, digest: Chunk[Byte]): String =
      s"{" +
        field("pages", value.pages.toString) + "," +
        field("textPages", value.textPages.toString) + "," +
        field("characters", value.characters.toString) + "," +
        field("preview", quote(value.preview)) + "," +
        field("retainsPages", value.retainsPages.toString) + "," +
        field("pageEvidence", array(value.retainedPages.iterator.map(pageJson))) + "," +
        field("citations", array(value.citations(digest).iterator.map(citationJson))) + "," +
        field("textRecoveryRequests", array(value.textRecoveryRequests.iterator.map(textRecoveryJson))) +
        "}"

    private def pageJson(value: Page): String =
      s"{" +
        field("pageNumber", number(value.pageNumber)) + "," +
        field("pageObjectNumber", number(value.pageObjectNumber)) + "," +
        field("contentObjectNumbers", array(value.contentObjectNumbers.iterator.map(number))) + "," +
        field("text", quote(value.text)) + "," +
        field("truncated", value.truncated.toString) + "," +
        field("hasNativeText", value.hasNativeText.toString) +
        "}"

    private def citationJson(value: Citation): String =
      s"{" +
        field("id", quote(value.id)) + "," +
        field("pageNumber", number(value.location.pageNumber)) + "," +
        field("pageObjectNumber", number(value.location.pageObjectNumber)) + "," +
        field("contentObjectNumbers", array(value.location.contentObjectNumbers.iterator.map(number))) + "," +
        field("excerpt", quote(value.excerpt)) + "," +
        field("truncated", value.truncated.toString) +
        "}"

    private def textRecoveryJson(value: TextRecovery.Request): String =
      s"{" +
        field("pageNumber", number(value.location.pageNumber)) + "," +
        field("pageObjectNumber", number(value.location.pageObjectNumber)) + "," +
        field("contentObjectNumbers", array(value.location.contentObjectNumbers.iterator.map(number))) + "," +
        field("reason", quote(value.reason.toString)) +
        "}"

    private def status(success: Boolean): String =
      s"{" + field("status", quote(if success then "passed" else "failed")) + "}"

    private def field(name: String, value: String): String = quote(name) + ":" + value
    private def number(value: Long): String = value.toString
    private def optional(value: Option[String]): String = value.fold("null")(quote)
    private def array(values: Iterator[String]): String = values.mkString("[", ",", "]")

    private def quote(value: String): String =
      val out = new StringBuilder(value.length + 2)
      out.append('"')
      value.foreach {
        case '"'  => out.append("\\\"")
        case '\\' => out.append("\\\\")
        case '\b' => out.append("\\b")
        case '\f' => out.append("\\f")
        case '\n' => out.append("\\n")
        case '\r' => out.append("\\r")
        case '\t' => out.append("\\t")
        case char if char < ' ' => out.append(f"\\u${char.toInt}%04x")
        case char => out.append(char)
      }
      out.append('"')
      out.toString
