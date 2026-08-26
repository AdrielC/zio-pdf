package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.{Chunk, ZIO}
import zio.pdf.content.ContentOps
import zio.stream.{ZSink, ZStream}

import scala.collection.mutable.ArrayDeque

/**
 * Composable, streaming PDF preflight plans.
 *
 * A [[Plan]] is a small, immutable, inspectable program. Build a plan from
 * reusable operations, then run it over the incremental [[Element]]
 * stream produced by [[PdfEngine]]. Positive observations stop as soon as all
 * requested signals have appeared. Policies such as [[forbidJavaScript]] read
 * to end when no violation occurs, because absence can only be proven then.
 */
object PdfInspection:

  /** A signal a plan can discover while decoding PDF objects. */
  enum Finding:
    case Linearized, PdfA, Thumbnail, Encrypted, JavaScript

  /** How directly the parser can establish a reported fact. */
  enum Confidence:
    /** A PDF dictionary, stream, or trailer directly establishes the fact. */
    case Structural
    /** A content-stream pattern suggests the fact but cannot prove semantics. */
    case Heuristic

  /** A fact together with the reason the inspector can stand behind it. */
  final case class Evidence[+A](value: A, confidence: Confidence, reason: String)

  /** Why the streaming interpreter stopped consuming elements. */
  enum Completion:
    case EndOfInput, SatisfiedEarly, RejectedEarly, RejectedAfterFullScan

  final case class Linearization(objectNumber: Long)
  /**
   * PDF/A identification declared by the XMP metadata stream.
   *
   * `part` and `conformance` report what the producer declared; they are not a
   * substitute for validating every PDF/A conformance requirement.
   */
  final case class PdfA(
    metadataObjectNumber: Long,
    part: Option[String] = None,
    conformance: Option[String] = None
  ):
    def declaresA3b: Boolean =
      part.contains("3") && conformance.exists(_.equalsIgnoreCase("B"))
  final case class Thumbnail(pageObjectNumber: Long, image: Prim.Ref)
  final case class Encryption(reference: Option[Prim.Ref])
  final case class JavaScript(objectNumber: Long)

  /** A decoded font dictionary available to a document-level transform. */
  final case class Font(
    objectNumber: Long,
    baseFont: String,
    subtype: Option[String]
  ):
    /**
     * `PdfTransform.fonts.replaceExisting` currently preserves existing glyph
     * codes only for simple font dictionaries. Pair compatibility remains a
     * separate check when the transform is executed.
     */
    def isExistingResourceRemapCandidate: Boolean =
      subtype.exists(value => value == "Type1" || value == "TrueType" || value == "Type0")

  /** The stable, partial report accumulated by a plan. */
  final case class Report(
    linearization: Option[Linearization] = None,
    pdfA: Option[PdfA] = None,
    thumbnail: Option[Thumbnail] = None,
    encryption: Option[Encryption] = None,
    javaScript: Option[JavaScript] = None,
    fonts: Chunk[Font] = Chunk.empty,
    imageCount: Long = 0L,
    attachmentCount: Long = 0L,
    tableCandidateCount: Long = 0L,
    elementsRead: Long = 0L,
    completion: Completion = Completion.EndOfInput
  ):
    def has(finding: Finding): Boolean =
      finding match
        case Finding.Linearized => linearization.nonEmpty
        case Finding.PdfA       => pdfA.nonEmpty
        case Finding.Thumbnail  => thumbnail.nonEmpty
        case Finding.Encrypted  => encryption.nonEmpty
        case Finding.JavaScript => javaScript.nonEmpty

    def linearizationEvidence: Option[Evidence[Linearization]] =
      linearization.map(Evidence(_, Confidence.Structural, "The object dictionary declares /Linearized."))

    def pdfAEvidence: Option[Evidence[PdfA]] =
      pdfA.map(Evidence(_, Confidence.Structural, "The XMP metadata stream declares pdfaid:part."))

    def thumbnailEvidence: Option[Evidence[Thumbnail]] =
      thumbnail.map(Evidence(_, Confidence.Structural, "A page dictionary contains a /Thumb image reference."))

    def encryptionEvidence: Option[Evidence[Encryption]] =
      encryption.map(Evidence(_, Confidence.Structural, "The trailer contains an /Encrypt entry."))

    def imageCountEvidence: Evidence[Long] =
      Evidence(imageCount, Confidence.Structural, "Counted decoded XObject streams whose /Subtype is /Image.")

    def attachmentCountEvidence: Evidence[Long] =
      Evidence(attachmentCount, Confidence.Structural, "Counted decoded /Type /EmbeddedFile streams.")

    def tableCandidateCountEvidence: Evidence[Long] =
      Evidence(
        tableCandidateCount,
        Confidence.Heuristic,
        "Counted general content streams containing rectangle and text-show operators."
      )

  /** A policy rejected the document before the decoder read another element. */
  enum Violation:
    case JavaScript(found: PdfInspection.JavaScript)

  /** A completed preflight run, including any partial report on rejection. */
  enum Outcome:
    case Accepted(report: Report)
    case Rejected(report: Report, violation: Violation)

  /**
   * The small inspection algebra. It is public so a caller can inspect a plan
   * or write another analysis without duplicating the composition API.
   */
  enum Op[A, B]:
    case ObserveLinearization extends Op[Element, Element]
    case ObservePdfA extends Op[Element, Element]
    case ObserveThumbnail extends Op[Element, Element]
    case ObserveEncryption extends Op[Element, Element]
    case CountImages extends Op[Element, Element]
    case CountAttachments extends Op[Element, Element]
    case CountTableCandidates extends Op[Element, Element]
    case ObserveJavaScript extends Op[Element, Element]
    case InventoryFonts extends Op[Element, Element]
    case RejectJavaScript extends Op[Element, Element]

  /** A caller-supplied static interpretation of one named plan operation. */
  trait Analyzer[+A]:
    def apply(op: Op[Element, Element]): A

  /** The minimal combination law needed to analyze a composed plan. */
  trait Monoid[A]:
    def empty: A
    def combine(left: A, right: A): A

  /**
   * An immutable, ordered plan whose operations remain visible to consumers.
   * The representation is deliberately domain-specific and dependency-free.
   */
  final case class Plan private (operations: Chunk[Op[Element, Element]]):
    def andThen(right: Plan): Plan = Plan(operations ++ right.operations)
    infix def >>>(right: Plan): Plan = andThen(right)
    def size: Int = operations.size

    def analyze[A](analyzer: Analyzer[A])(using monoid: Monoid[A]): A =
      operations.foldLeft(monoid.empty)((acc, op) => monoid.combine(acc, analyzer(op)))

  object Plan:
    val empty: Plan = Plan(Chunk.empty)
    def single(op: Op[Element, Element]): Plan = Plan(Chunk.single(op))

  /**
   * Execution facts derived from the operations embedded in a plan. This is
   * analysis, not sidecar plan metadata: analyzing the same immutable plan
   * always produces the same profile.
   */
  final case class Profile(required: Set[Finding], requiresFullScan: Boolean):
    def canStop(report: Report): Boolean =
      !requiresFullScan && required.nonEmpty && required.forall(report.has)

  object Profile:
    val empty: Profile = Profile(Set.empty, requiresFullScan = false)

    given Monoid[Profile] with
      def empty: Profile = Profile.empty

      def combine(left: Profile, right: Profile): Profile =
        Profile(
          required = left.required ++ right.required,
          requiresFullScan = left.requiresFullScan || right.requiresFullScan
        )

  private final case class State(report: Report):
    def nextElement: State = copy(report = report.copy(elementsRead = report.elementsRead + 1L))

    def rememberLinearization(found: Option[Linearization]): State =
      copy(report = report.copy(linearization = report.linearization.orElse(found)))

    def rememberPdfA(found: Option[PdfA]): State =
      copy(report = report.copy(pdfA = report.pdfA.orElse(found)))

    def rememberThumbnail(found: Option[Thumbnail]): State =
      copy(report = report.copy(thumbnail = report.thumbnail.orElse(found)))

    def rememberEncryption(found: Option[Encryption]): State =
      copy(report = report.copy(encryption = report.encryption.orElse(found)))

    def rememberJavaScript(found: Option[JavaScript]): State =
      copy(report = report.copy(javaScript = report.javaScript.orElse(found)))

    def addFont(found: Option[Font]): State =
      found match
        case Some(font) if !report.fonts.exists(_.objectNumber == font.objectNumber) =>
          copy(report = report.copy(fonts = report.fonts :+ font))
        case _ => this

    def addImage(element: Element): State =
      if imageOf(element) then copy(report = report.copy(imageCount = report.imageCount + 1L)) else this

    def addAttachment(element: Element): State =
      if attachmentOf(element) then copy(report = report.copy(attachmentCount = report.attachmentCount + 1L)) else this

    def addTableCandidate(element: Element): State =
      if tableCandidateOf(element) then
        copy(report = report.copy(tableCandidateCount = report.tableCandidateCount + 1L))
      else this

  private val profileAnalysis: Analyzer[Profile] = new Analyzer[Profile]:
    def apply(op: Op[Element, Element]): Profile =
      op match
        case Op.ObserveLinearization => Profile(Set(Finding.Linearized), requiresFullScan = false)
        case Op.ObservePdfA          => Profile(Set(Finding.PdfA), requiresFullScan = false)
        case Op.ObserveThumbnail     => Profile(Set(Finding.Thumbnail), requiresFullScan = false)
        case Op.ObserveEncryption    => Profile(Set(Finding.Encrypted), requiresFullScan = false)
        case Op.CountImages           => Profile(Set.empty, requiresFullScan = true)
        case Op.CountAttachments      => Profile(Set.empty, requiresFullScan = true)
        case Op.CountTableCandidates  => Profile(Set.empty, requiresFullScan = true)
        case Op.ObserveJavaScript    => Profile(Set(Finding.JavaScript), requiresFullScan = false)
        case Op.InventoryFonts       => Profile(Set.empty, requiresFullScan = true)
        case Op.RejectJavaScript     => Profile(Set.empty, requiresFullScan = true)

  /** Derive the stream-consumption profile from the public plan structure. */
  def profile(plan: Plan): Profile = plan.analyze(profileAnalysis)

  private def observed(op: Op[Element, Element]): Plan = Plan.single(op)

  /** Detect the `/Linearized` dictionary marker. */
  val linearized: Plan = observed(Op.ObserveLinearization)

  /** Detect XMP PDF/A identification metadata (`pdfaid:part`). Not a conformance claim. */
  val pdfA: Plan = observed(Op.ObservePdfA)

  /** Detect a page `/Thumb` image reference. */
  val thumbnail: Plan = observed(Op.ObserveThumbnail)

  /** Detect a trailer `/Encrypt` entry. */
  val encryption: Plan = observed(Op.ObserveEncryption)

  /** Count XObject streams classified from `/Subtype /Image`. */
  val imageCount: Plan = observed(Op.CountImages)

  /** Count `/Type /EmbeddedFile` streams. */
  val attachmentCount: Plan = observed(Op.CountAttachments)

  /** Count conservative rectangle-plus-text table candidates. */
  val tableCandidates: Plan = observed(Op.CountTableCandidates)

  /** Detect a PDF JavaScript action or payload. */
  val javaScript: Plan = observed(Op.ObserveJavaScript)

  /**
   * Inventory `/BaseFont` dictionaries without inflating content streams.
   *
   * This is deliberately a regular inspection leaf: compose it into a plan
   * whenever a caller wants real document font names to seed a transform.
   */
  val fontInventory: Plan = observed(Op.InventoryFonts)

  /**
   * Reject on the first JavaScript action or payload. The successful path reads
   * the complete input, since no earlier point can prove JavaScript is absent.
   */
  val forbidJavaScript: Plan =
    Plan.single(Op.RejectJavaScript)

  /**
   * A complete document profile expressed entirely as ordinary plan
   * composition. Consumers can remove or replace any component without
   * entering a second, special-case inspection API.
   */
  val documentProfile: Plan =
    linearized >>> pdfA >>> thumbnail >>> encryption >>> fontInventory >>> imageCount >>> attachmentCount >>>
      tableCandidates >>> forbidJavaScript

  /** Run a composed plan over an already-decoded PDF element stream. */
  def run[R, E](source: ZStream[R, E, Element], plan: Plan): ZIO[R, E, Outcome] =
    val executionProfile = profile(plan)
    val initial = (State(Report()), Option.empty[Violation])

    source
      .run(
        ZSink.fold[Element, (State, Option[Violation])](initial) { case (state, violation) =>
          violation.isEmpty && !executionProfile.canStop(state.report)
        } { case ((state, violation), element) =>
          val (next, found) = applyPlan(state.nextElement, plan, element)
          (next, violation.orElse(found))
        }
      )
      .map { case (state, violation) =>
        val completion =
          if violation.nonEmpty then Completion.RejectedEarly
          else if executionProfile.canStop(state.report) then Completion.SatisfiedEarly
          else Completion.EndOfInput
        val report = state.report.copy(completion = completion)
        violation match
          case Some(found) => Outcome.Rejected(report, found)
          case None            => Outcome.Accepted(report)
      }

  private def applyPlan(state: State, plan: Plan, element: Element): (State, Option[Violation]) =
    val iterator = plan.operations.iterator
    var current  = state
    var failure: Option[Violation] = None
    while iterator.hasNext && failure.isEmpty do
      iterator.next() match
        case Op.ObserveLinearization => current = current.rememberLinearization(linearizationOf(element))
        case Op.ObservePdfA          => current = current.rememberPdfA(pdfAOf(element))
        case Op.ObserveThumbnail     => current = current.rememberThumbnail(thumbnailOf(element))
        case Op.ObserveEncryption    => current = current.rememberEncryption(encryptionOf(element))
        case Op.CountImages          => current = current.addImage(element)
        case Op.CountAttachments     => current = current.addAttachment(element)
        case Op.CountTableCandidates => current = current.addTableCandidate(element)
        case Op.ObserveJavaScript    => current = current.rememberJavaScript(javaScriptOf(element))
        case Op.InventoryFonts       => current = current.addFont(fontOf(element))
        case Op.RejectJavaScript =>
          javaScriptOf(element).foreach { found =>
            current = current.rememberJavaScript(Some(found))
            failure = Some(Violation.JavaScript(found))
          }
    (current, failure)

  /**
   * Compiles one inspection plan once, then accepts decoded elements directly.
   *
   * `drainOnViolation` is for a larger evidence bundle which must continue
   * after an inspectable policy violation in order to produce its digest,
   * structural validation, and page-provenanced text. The ordinary
   * [[run]] interpreter keeps its fail-fast behaviour.
   */
  private[pdf] final class Accumulator private[pdf] (
    plan: Plan,
    drainOnViolation: Boolean
  ):
    private val executionProfile = profile(plan)

    private var state: State               = State(Report())
    private var violation: Option[Violation] = None
    private var stopped                    = false

    /** Returns whether the producer should continue feeding elements. */
    def add(element: Element): Boolean =
      if !stopped then
        val (next, result) = applyPlan(state.nextElement, plan, element)
        state = next
        violation = violation.orElse(result)
        stopped =
          !drainOnViolation &&
            (violation.nonEmpty || executionProfile.canStop(state.report))
      !stopped

    def result: Outcome =
      val completion =
        violation match
          case Some(_) if drainOnViolation => Completion.RejectedAfterFullScan
          case Some(_)                     => Completion.RejectedEarly
          case None if stopped              => Completion.SatisfiedEarly
          case None                         => Completion.EndOfInput
      val report = state.report.copy(completion = completion)
      violation match
        case Some(found) => Outcome.Rejected(report, found)
        case None        => Outcome.Accepted(report)

  private[pdf] def accumulator(plan: Plan, drainOnViolation: Boolean = false): Accumulator =
    new Accumulator(plan, drainOnViolation)

  extension [R, E](source: ZStream[R, E, Element])
    /** Ergonomic stream syntax for a composable inspection plan. */
    def inspect(plan: Plan): ZIO[R, E, Outcome] = run(source, plan)

  private def objectOf(element: Element): Option[Obj] =
    element match
      case Element.Data(obj, _)          => Some(obj)
      case Element.Content(obj, _, _, _) => Some(obj)
      case Element.Meta(_, _)            => None

  private def linearizationOf(element: Element): Option[Linearization] =
    objectOf(element).collect {
      case obj if Prim.tryDict("Linearized")(obj.data).nonEmpty => Linearization(obj.index.number)
    }

  private def pdfAOf(element: Element): Option[PdfA] =
    element match
      case Element.Content(obj, _, stream, _)
          if isMetadataXml(obj.data) && stream.exec.toOption.exists(containsPdfAIdentifier) =>
        val declaration = stream.exec.toOption.flatMap(pdfADeclaration)
        Some(
          PdfA(
            obj.index.number,
            declaration.flatMap(_._1),
            declaration.flatMap(_._2)
          )
        )
      case _ => None

  private def thumbnailOf(element: Element): Option[Thumbnail] =
    objectOf(element).flatMap { obj =>
      Prim.tryDict("Thumb")(obj.data).collect { case image: Prim.Ref =>
        Thumbnail(obj.index.number, image)
      }
    }

  private def encryptionOf(element: Element): Option[Encryption] =
    element match
      case Element.Meta(Some(trailer), _) =>
        trailer.data.data.get("Encrypt").map {
          case reference: Prim.Ref => Encryption(Some(reference))
          case _                   => Encryption(None)
        }
      case _ => None

  private def imageOf(element: Element): Boolean =
    element match
      case Element.Content(_, _, _, Element.ContentKind.Image(_)) => true
      case _                                                       => false

  private def attachmentOf(element: Element): Boolean =
    element match
      case Element.Content(_, _, _, Element.ContentKind.EmbeddedFileStream(_)) => true
      case _                                                                     => false

  private def tableCandidateOf(element: Element): Boolean =
    element match
      case Element.Content(_, _, stream, Element.ContentKind.General) =>
        stream.exec.toOption.exists(bits => ContentOps.looksLikeTable(bits.toByteArray))
      case _ => false

  private def fontOf(element: Element): Option[Font] =
    objectOf(element).flatMap { obj =>
      nameAt(obj.data, "BaseFont").map { baseFont =>
        Font(obj.index.number, baseFont, nameAt(obj.data, "Subtype"))
      }
    }

  private def nameAt(data: Prim, key: String): Option[String] =
    Prim.tryDict(key)(data).collect { case Prim.Name(value) => value }

  private def javaScriptOf(element: Element): Option[JavaScript] =
    objectOf(element).collect {
      case obj if containsJavaScript(obj.data) => JavaScript(obj.index.number)
    }

  private val pdfAIdentifier = ByteVector("pdfaid:part".getBytes(StandardCharsets.US_ASCII))

  private def containsPdfAIdentifier(bits: BitVector): Boolean =
    bits.bytes.indexOfSlice(pdfAIdentifier) >= 0L

  private val pdfAPartElement = "(?is)<pdfaid:part\\b[^>]*>\\s*([^<\\s]+)\\s*</pdfaid:part>".r
  private val pdfAConformanceElement = "(?is)<pdfaid:conformance\\b[^>]*>\\s*([^<\\s]+)\\s*</pdfaid:conformance>".r
  private val pdfAPartAttribute = "(?is)\\bpdfaid:part\\s*=\\s*[\"']\\s*([^\"']+)\\s*[\"']".r
  private val pdfAConformanceAttribute = "(?is)\\bpdfaid:conformance\\s*=\\s*[\"']\\s*([^\"']+)\\s*[\"']".r

  private def pdfADeclaration(bits: BitVector): Option[(Option[String], Option[String])] =
    val xml = new String(bits.toByteArray, StandardCharsets.UTF_8)
    val part = xmpValue(xml, pdfAPartElement).orElse(xmpValue(xml, pdfAPartAttribute))
    val conformance =
      xmpValue(xml, pdfAConformanceElement).orElse(xmpValue(xml, pdfAConformanceAttribute))
    Some((part, conformance))

  private def xmpValue(xml: String, pattern: scala.util.matching.Regex): Option[String] =
    pattern
      .findFirstMatchIn(xml)
      .map(_.group(1).trim)
      .filter(_.nonEmpty)

  private def isMetadataXml(data: Prim): Boolean =
    Prim.tryDict("Type")(data).contains(Prim.Name("Metadata")) &&
      Prim.tryDict("Subtype")(data).contains(Prim.Name("XML"))

  private def containsJavaScript(data: Prim): Boolean =
    val pending = ArrayDeque(data)
    var found = false

    while pending.nonEmpty && !found do
      pending.removeLast() match
        case Prim.Dict(values) =>
          found =
            values.get("JS").nonEmpty ||
              values.get("S").contains(Prim.Name("JavaScript"))
          if !found then
            values.toList.foreach { case (_, value) => pending.append(value) }
        case Prim.Array(values) =>
          val iterator = values.iterator
          while iterator.hasNext do pending.append(iterator.next())
        case _ => ()

    found
