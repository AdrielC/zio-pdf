package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import io.github.adrielc.scan.Scan
import java.security.MessageDigest
import kyo.*
import zio.blocks.schema.{Schema as BSchema}
import StepRegistry.*
import PdfPipeline.{Outcome, Issue, Code}

/** Portable settings and intermediate values for registered PDF operations. */
object PdfSteps:
  final case class Empty()
  final case class Count(value: Long)
  final case class Page(number: Int, textCharacters: Option[Long] = None)
  final case class Facts(sha256: String, bytes: Long, pages: Vector[Page])
  final case class PageCollection(sha256: String, pages: Vector[Page])
  /** Coordinates are PDF points, top-left origin, on an unrotated page. */
  final case class Region(page: Int, x: Double, y: Double, width: Double, height: Double)
  final case class Selection(sha256: String, regions: Vector[Region])
  final case class RedactionPlan(sha256: String, regions: Vector[Region], reason: String)
  final case class PolicyResult(sha256: String, outcome: Outcome, evidence: Vector[String])
  final case class Select(regions: Vector[Region] = Vector.empty, terms: Vector[String] = Vector.empty)
  final case class Plan(reason: String = "Remove selected content")
  final case class Edit(dpi: Int = 144)
  final case class Replace(find: String, replacement: String, font: String = "helv", fontSize: Double = 11, dpi: Int = 144)
  final case class Ocr(language: String = "eng", dpi: Int = 144)
  final case class PageFilter(min: Int = 1, max: Int = 64, minTextCharacters: Option[Long] = None,
      unknown: Outcome = Outcome.Unknown)
  final case class Evaluate(profile: Policy.Profile)

  val empty = new DataSchema("empty", 1, BSchema.derived[Empty])
  val count = new DataSchema("count", 1, BSchema.derived[Count])
  val facts = new DataSchema("pdf.facts", 1, BSchema.derived[Facts])
  val pages = new DataSchema("pdf.pages", 1, BSchema.derived[PageCollection])
  val selection = new DataSchema("pdf.selection", 1, BSchema.derived[Selection])
  val redactionPlan = new DataSchema("pdf.redaction-plan", 1, BSchema.derived[RedactionPlan])
  val policyResult = new DataSchema("policy.result", 1, BSchema.derived[PolicyResult])
  given factsPort: PdfArrow.Scalar[Facts] = PdfArrow.dataPort(facts)
  given pagesPort: PdfArrow.Scalar[PageCollection] = PdfArrow.dataPort(pages)
  given selectionPort: PdfArrow.Scalar[Selection] = PdfArrow.dataPort(selection)
  given redactionPort: PdfArrow.Scalar[RedactionPlan] = PdfArrow.dataPort(redactionPlan)
  given policyPort: PdfArrow.Scalar[PolicyResult] = PdfArrow.dataPort(policyResult)
  given countPort: PdfArrow.Scalar[Count] = PdfArrow.dataPort(count)
  private val selectSettings = new DataSchema("pdf.select.settings", 1, BSchema.derived[Select])
  private val planSettings = new DataSchema("pdf.plan.settings", 1, BSchema.derived[Plan])
  private val editSettings = new DataSchema("pdf.edit.settings", 1, BSchema.derived[Edit])
  private val replaceSettings = new DataSchema("pdf.replace.settings", 1, BSchema.derived[Replace])
  private val ocrSettings = new DataSchema("pdf.ocr.settings", 1, BSchema.derived[Ocr])
  private val filterSettings = new DataSchema("pdf.filter.settings", 1, BSchema.derived[PageFilter])
  private val policySettings = new DataSchema("policy.evaluate.settings", 1, BSchema.derived[Evaluate])

  def sha256(file: PdfFile): String =
    val digest = MessageDigest.getInstance("SHA-256")
    // No full-payload array is allocated for hashing.
    file.bytes.iterator.foreach(digest.update)
    digest.digest().iterator.map(byte => f"${byte & 255}%02x").mkString
  private def issue(path: String, test: Boolean, text: String): Vector[Issue] =
    Option.when(!test)(Issue(path, Code.Setting, text)).toVector
  private def dpi(value: Int) = issue("settings.dpi", value >= 72 && value <= 300, "Use 72 to 300 DPI")
  private def valid(region: Region): Boolean =
    region.page >= 1 && region.page <= 64 && Vector(region.x, region.y, region.width, region.height).forall(_.isFinite) &&
      region.x >= 0 && region.y >= 0 && region.width > 0 && region.height > 0 && region.x + region.width <= 14400 && region.y + region.height <= 14400

  val byteCount = new Step("bytes.count", 1, Port.file, Port.data(count), empty, Vector(Empty()), StepRegistry.Access.Stream,
    Set(Capability.Inspect), scan = Some((_: Empty) => Scan.count[Byte].map(Count.apply)),
    description = "Count the exact File bytes; compatible byte scans share one source pass.")((file, _, _) => Count(file.size))
  val highByteCount = new Step("bytes.high-count", 1, Port.file, Port.data(count), empty, Vector(Empty()), StepRegistry.Access.Stream,
    Set(Capability.Inspect), scan = Some((_: Empty) => Scan.foldLong[Byte](0L)((n, b) => n + (if b < 0 then 1 else 0)).map(Count.apply)),
    description = "Count bytes above 127 in the exact File; compatible byte scans share one source pass.")(
    (file, _, _) => Count(file.bytes.count(_ < 0).toLong))

  val inspect = new Step("pdf.inspect", 1, Port.file, Port.data(facts), empty, Vector(Empty()), StepRegistry.Access.RandomAccess,
    Set(Capability.Inspect))((file, _, _) =>
      Abort.run[PdfError](ByteLimit.fromBytes(file.size).map(limit => PdfDocument.decode(file.bytes.toArray, limit).map(doc => PdfPages.pageRefs(doc)))).map {
        case Result.Success(refs) => Facts(sha256(file), file.size, refs.indices.map(i => Page(i + 1)).toVector)
        case _ => Abort.fail(Error(ErrorCode.InvalidInput, "Cannot inspect PDF structure"))
      })
  val pageCollection = new Step("pdf.pages", 1, Port.data(facts), Port.data(pages), empty, Vector(Empty()), StepRegistry.Access.Document,
    Set(Capability.Inspect))((value, _, _) => PageCollection(value.sha256, value.pages))
  val filterPages = new Step("pdf.filter-pages", 1, Port.data(pages), Port.data(pages), filterSettings,
    Vector(PageFilter(2, 4)), StepRegistry.Access.Document, Set(Capability.Inspect), p =>
      issue("settings", p.min >= 1 && p.max >= p.min && p.max <= 64 && p.minTextCharacters.forall(_ >= 0), "Invalid page filter"))((value, p, _) =>
    val candidates = value.pages.filter(page => page.number >= p.min && page.number <= p.max)
    if p.minTextCharacters.nonEmpty && candidates.exists(_.textCharacters.isEmpty) && p.unknown == Outcome.Unknown then
      Abort.fail(Error(ErrorCode.Rejected, "Page filter needs text evidence; unknown pages require review"))
    else PageCollection(value.sha256, candidates.filter(page => p.minTextCharacters.forall(min => page.textCharacters.fold(p.unknown == Outcome.Yes)(_ >= min)))))

  val evaluatePolicy = new Step("policy.evaluate", 1, Port.data(facts), Port.data(policyResult), policySettings,
    Vector(Evaluate(CourtProfile.CaliforniaSecondDistrictPetition.rules)), StepRegistry.Access.Document, Set(Capability.Inspect),
    p => Policy.compile(p.profile).left.toOption.getOrElse(Vector.empty))((value, p, _) =>
      Policy.compile(p.profile) match
        case Left(_) => Abort.fail(Error(ErrorCode.InvalidInput, "Invalid policy"))
        case Right(checked) =>
          val document = Policy.Observation(Policy.Scope.Document, Map(Policy.Metric.FileBytes -> value.bytes, Policy.Metric.PageCount -> value.pages.size.toLong))
          val observations = document +: value.pages.map(page => Policy.Observation(Policy.Scope.Page,
            Map(Policy.Metric.PageNumber -> page.number.toLong) ++ page.textCharacters.map(Policy.Metric.TextCharacters -> _), Some(page.number)))
          val findings = checked.run(observations)
          val outcome = if findings.exists(_.status == "fix-required") then Outcome.No
            else if findings.exists(_.status == "needs-review") then Outcome.Unknown else Outcome.Yes
          PolicyResult(value.sha256, outcome, findings.map(f => s"${f.check}: ${f.status}")))

  val select = new Step("pdf.select-content", 1, Port.file, Port.data(selection), selectSettings,
    Vector(Select(Vector(Region(1, 10, 10, 100, 20)))), StepRegistry.Access.RandomAccess, Set(Capability.Inspect), p =>
      issue("settings.regions", p.regions.size <= 256 && p.regions.forall(valid), "Use at most 256 valid regions") ++
      issue("settings.terms", p.terms.size <= 32 && p.terms.forall(t => t.nonEmpty && t.length <= 256), "Use up to 32 nonempty terms") ++
      issue("settings", p.regions.nonEmpty || p.terms.nonEmpty, "Select regions or search terms"),
    description = "Select regions or locate text. Regions use page, x, y, width and height in top-left PDF points; the result is bound to these exact File bytes.")((file, p, context) =>
      if p.terms.isEmpty then Selection(sha256(file), p.regions)
      else context.services.semantic.locate(file, p).map { found =>
        if found.sha256 != sha256(file) || found.regions.size > 256 || !found.regions.forall(valid) then Abort.fail(Error(ErrorCode.Verification, "Selection is not bound to the input"))
        else found
      })
  val planRedaction = new Step("pdf.plan-redaction", 1, Port.data(selection), Port.data(redactionPlan), planSettings,
    Vector(Plan()), StepRegistry.Access.Document, Set(Capability.Inspect), p => issue("settings.reason", p.reason.nonEmpty && p.reason.length <= 160, "Use a short reason"),
    description = "Turn a Selection into a RedactionPlan, preserving its source hash and regions.")(
    (selected, p, _) => RedactionPlan(selected.sha256, selected.regions, p.reason))
  val redact = new Step("pdf.redact", 1, Port.pair(Port.file, Port.data(redactionPlan)), Port.file, editSettings,
    Vector(Edit()), StepRegistry.Access.RandomAccess, Set(Capability.Rewrite, Capability.Render, Capability.ExternalProcess, Capability.Verify), p => dpi(p.dpi),
    description = "Inputs are File then RedactionPlan. Remove selected content, raster-rebuild and verify the final artifact; output is a File.")((input, p, context) =>
      val (file, plan) = input
      if plan.sha256 != sha256(file) || plan.regions.isEmpty || plan.regions.size > 256 || !plan.regions.forall(valid) then
        Abort.fail(Error(ErrorCode.InvalidInput, "Redaction requires a nonempty plan bound to these exact bytes"))
      else context.services.semantic.redact(file, plan, p).map(output => verified(file, output, "redact")))
  val replaceText = new Step("pdf.replace-text", 1, Port.file, Port.file, replaceSettings,
    Vector(Replace("old", "new")), StepRegistry.Access.RandomAccess, Set(Capability.Rewrite, Capability.Render, Capability.ExternalProcess, Capability.Verify), p =>
      dpi(p.dpi) ++ issue("settings", p.find.nonEmpty && p.find.length <= 256 && p.replacement.length <= 256 &&
        Set("helv", "cour", "tiro")(p.font) && p.fontSize.isFinite && p.fontSize >= 4 && p.fontSize <= 72,
        "Choose a base-14 font, a bounded string, and a 4 to 72 point size"),
    description = "Replace visible find text with replacement using a supported font, then raster-rebuild and verify.")((file, p, context) =>
      context.services.semantic.replace(file, p).map(output => verified(file, output, "replace")))
  val ocr = new Step("pdf.ocr", 1, Port.file, Port.file, ocrSettings, Vector(Ocr()), StepRegistry.Access.RandomAccess,
    Set(Capability.Rewrite, Capability.Render, Capability.Ocr, Capability.ExternalProcess, Capability.Verify), p =>
      dpi(p.dpi) ++ issue("settings.language", p.language.matches("[a-z]{3}(\\+[a-z]{3}){0,3}"), "Choose installed OCR languages"),
    description = "OCR a File using an installed language code (eng for English) and 72 to 300 DPI. Return a fresh searchable File only after verification.")((file, p, context) =>
      context.services.semantic.ocr(file, p).map(output => verified(file, output, "ocr")))
  val cleanup = new Step("pdf.cleanup", 1, Port.file, Port.file, editSettings, Vector(Edit()), StepRegistry.Access.RandomAccess,
    Set(Capability.Rewrite, Capability.Render, Capability.ExternalProcess, Capability.Verify), p => dpi(p.dpi))((file, p, context) =>
      context.services.semantic.cleanup(file, p).map(output => verified(file, output, "cleanup")))

  private def verified(input: PdfFile, output: SemanticBackend.Artifact, operation: String): PdfFile < (Abort[Error] & Sync) =
    if output.inputSha256 != sha256(input) || output.outputSha256 != sha256(output.file) || output.operation != operation || !output.verified then
      Abort.fail(Error(ErrorCode.Verification, "Semantic verification does not match the exact artifact"))
    else output.file

  lazy val registry: Registry = Vector[Entry](byteCount, highByteCount, inspect, pageCollection, filterPages,
    evaluatePolicy, select, planRedaction, redact, replaceText, ocr, cleanup,
    PdfPacketSteps.two, PdfPacketSteps.three, PdfPacketSteps.four).foldLeft(emptyRegistry)((r, e) => r.add(e).toOption.get)
  private def emptyRegistry = StepRegistry.empty

  /** A complete redaction subarrow is reusable inside a larger PDF graph. */
  def redactArrow(settings: Select, edit: Edit = Edit()): PdfArrow[PdfFile, PdfFile] =
    import PdfArrow.*
    val plan = registered(PdfSteps.select, settings) >>> registered(planRedaction, Plan())
    (id[File] &&& plan) >>> registered(redact, edit)
