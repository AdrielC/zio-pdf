package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*
import zio.blocks.schema.{DynamicOptic, Schema as BlocksSchema, SchemaExpr}

/** A closed, versioned program. Nodes contain settings and references, never code. */
object PdfPipeline:
  enum Kind derives Schema:
    case Document, File, Receipt

  enum Outcome derives Schema:
    case Yes, No, Unknown
  enum Verdict derives Schema:
    case Passed, Failed, Unable
  enum Predicate derives Schema:
    case PageCount(min: Int, max: Int)
    case Declares3b
    case HasPageNumbers
    case Validation(profile: String)
    def input: Kind = this match
      case Validation(_) => Kind.Receipt
      case _ => Kind.Document

  private final case class PageFacts(count: Int)
  private lazy val pageFactsSchema = BlocksSchema.derived[PageFacts]
  private lazy val pageCount = SchemaExpr.optic[PageFacts, Int](DynamicOptic.root.field("count"), pageFactsSchema)

  private def pageRange(min: Int, max: Int): SchemaExpr[PageFacts, Boolean] =
    import SchemaExpr.RelationalOperator.{GreaterThanOrEqual, LessThanOrEqual}
    val lower = SchemaExpr.relational(pageCount, SchemaExpr.literal[PageFacts, Int](min), GreaterThanOrEqual)
    val upper = SchemaExpr.relational(pageCount, SchemaExpr.literal[PageFacts, Int](max), LessThanOrEqual)
    lower.and(upper)

  enum Op derives Schema:
    case Input(slot: String)
    case Select(pages: Vector[Int])
    case Assemble(catalog: Int = 0)
    case Stamp(text: String, placement: String = "top")
    case NumberPages(start: Int = 1, style: String = "page", prefix: String = "")
    case Write
    case ConvertPdfA3b
    case Validate(profiles: Vector[String])
    case Preflight(profile: String)
    case PreflightRules(rules: Policy.Profile)
    case Choose(test: Predicate, kind: Kind = Kind.Document)
    case Stop(message: String, kind: Kind = Kind.Document)
    /** Arrow projections retain evaluation of every input before selecting a value. */
    case Keep(index: Int, kinds: Vector[Kind])
    case StopAfter(message: String, kinds: Vector[Kind], kind: Kind = Kind.Document)

    def output: Kind = this match
      case Write | ConvertPdfA3b => Kind.File
      case Validate(_) | Preflight(_) | PreflightRules(_) => Kind.Receipt
      case Choose(_, kind) => kind
      case Stop(_, kind) => kind
      case Keep(index, kinds) => kinds.lift(index).getOrElse(Kind.Document)
      case StopAfter(_, _, kind) => kind
      case _ => Kind.Document
    def input: Kind = this match
      case Choose(test, _) => test.input
      case Validate(_) | ConvertPdfA3b | Preflight(_) | PreflightRules(_) => Kind.File
      case _ => Kind.Document

  final case class Node(id: String, op: Op, inputs: Vector[String] = Vector.empty) derives Schema
  final case class Recipe(version: Int, name: String, nodes: Vector[Node], outputs: Vector[String]) derives Schema:
    /** Inline a reusable fragment, binding its named document inputs to this graph.
      * IDs are namespaced; no runtime closures or hidden references are introduced.
      */
    def include(prefix: String, fragment: Recipe, bindings: Map[String, String]): Either[Vector[Issue], Recipe] =
      if !Set(1, 2, 3)(fragment.version) then return Left(Vector(Issue("fragment.version", Code.Version, "Use recipe version 1, 2 or 3")))
      check(fragment) match
        case Left(issues) => return Left(issues.map(issue => issue.copy(path = "fragment." + issue.path)))
        case _ => ()
      val slots = fragment.nodes.collect { case Node(_, Op.Input(slot), _) => slot }.toSet
      if !identifier(prefix) || bindings.keySet != slots || !bindings.values.forall(id => nodes.exists(_.id == id)) then
        Left(Vector(Issue("bindings", Code.Binding, "Bind every fragment input to an existing node and use a valid prefix")))
      else
        val aliases = fragment.nodes.map { node => node.id -> (node.op match
          case Op.Input(slot) => bindings(slot)
          case _ => s"$prefix-${node.id}") }.toMap
        val added = fragment.nodes.filterNot(_.op.isInstanceOf[Op.Input]).map { node =>
          node.copy(id = aliases(node.id), inputs = node.inputs.map(id => aliases.getOrElse(id, s"$prefix-$id")))
        }
        val recipe = copy(version = math.max(version, fragment.version), nodes = nodes ++ added, outputs = (outputs ++ fragment.outputs.map(id => aliases.getOrElse(id, s"$prefix-$id"))).distinct)
        check(recipe).map(_.recipe)

  enum Code derives Schema:
    case Structure, Version, Limit, Identifier, Duplicate, Setting, Reference, Arity, Type, Cycle, Output, Binding
  final case class Issue(path: String, code: Code, message: String) derives Schema
  final class Checked private[PdfPipeline] (val recipe: Recipe, val order: Vector[Node])

  val MaxNodes = 24
  val MaxOutputs = 8
  val Profiles = PdfAProfile.values.map(_.code).toSet
  private def identifier(value: String): Boolean = value.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")
  private def line(value: String, max: Int): Boolean = value.length <= max && !value.exists(Character.isISOControl)

  /** Accumulate independent field failures first; graph checks require unique IDs. */
  def check(recipe: Recipe): Either[Vector[Issue], Checked] =
    val issues = Vector.newBuilder[Issue]
    def add(path: String, code: Code, message: String): Unit = issues += Issue(path, code, message)
    if !Set(1, 2, 3)(recipe.version) then add("version", Code.Version, "Use recipe version 1, 2 or 3")
    if recipe.name.trim.isEmpty || !line(recipe.name, 80) then add("name", Code.Setting, "Give the recipe a single-line name up to 80 characters")
    if recipe.nodes.isEmpty || recipe.nodes.length > MaxNodes then add("nodes", Code.Limit, s"Use 1 to $MaxNodes nodes")
    if recipe.outputs.isEmpty || recipe.outputs.length > MaxOutputs || recipe.outputs.distinct != recipe.outputs then
      add("outputs", Code.Output, s"Choose 1 to $MaxOutputs distinct output nodes")
    recipe.nodes.zipWithIndex.foreach { (node, index) =>
      val path = s"nodes[$index]"
      if !identifier(node.id) then add(s"$path.id", Code.Identifier, "Use a letter followed by up to 47 letters, digits, hyphens or underscores")
      if node.inputs.length > 4 then add(s"$path.inputs", Code.Limit, "A node may have at most four inputs")
      node.op match
        case Op.Keep(index, kinds) =>
          if recipe.version < 3 then add(s"$path.op", Code.Version, "Keep requires recipe version 3")
          if kinds.isEmpty || kinds.length > 4 then add(s"$path.kinds", Code.Setting, "Use one to four input kinds")
          if index < 0 || index >= kinds.length then add(s"$path.index", Code.Setting, "Keep an existing zero-based input")
        case Op.StopAfter(message, kinds, _) =>
          if recipe.version < 3 then add(s"$path.op", Code.Version, "Stop after inputs requires recipe version 3")
          if kinds.isEmpty || kinds.length > 4 then add(s"$path.kinds", Code.Setting, "Use one to four input kinds")
          if message.trim.isEmpty || !line(message, 160) then add(s"$path.message", Code.Setting, "Give the stop a single-line reason up to 160 characters")
        case Op.Choose(test, _) =>
          if recipe.version < 2 then add(s"$path.op", Code.Version, "Choose requires recipe version 2")
          test match
            case Predicate.PageCount(min, max) if min < 1 || max < min || max > 64 => add(s"$path.test", Code.Setting, "Use an inclusive page-count range from 1 to 64")
            case Predicate.Validation(profile) if !Profiles(profile) => add(s"$path.test.profile", Code.Setting, "Choose a validation profile from 2b, 3b and 3u")
            case _ => ()
        case Op.Stop(message, _) =>
          if recipe.version < 2 then add(s"$path.op", Code.Version, "Stop requires recipe version 2")
          if message.trim.isEmpty || !line(message, 160) then add(s"$path.message", Code.Setting, "Give the stop a single-line reason up to 160 characters")
        case Op.Input(slot) if !identifier(slot) => add(s"$path.slot", Code.Identifier, "Use a valid input slot name")
        case Op.Select(pages) if pages.isEmpty || pages.length > 64 || pages.exists(_ < 1) || pages.distinct != pages =>
          add(s"$path.pages", Code.Setting, "Choose 1 to 64 distinct positive page numbers")
        case Op.Assemble(catalog) if catalog < 0 || catalog >= node.inputs.length =>
          add(s"$path.catalog", Code.Setting, "Choose the zero-based input whose catalog should be retained")
        case Op.Stamp(text, placement) =>
          if text.trim.isEmpty || !line(text, 80) then add(s"$path.text", Code.Setting, "Use non-empty, single-line stamp text up to 80 characters")
          if !Set("top", "center")(placement) then add(s"$path.placement", Code.Setting, "Choose top or center placement")
        case Op.NumberPages(start, style, prefix) =>
          if start < 1 || start > 999999 then add(s"$path.start", Code.Setting, "Start numbering from 1 to 999999")
          if !Set("page", "number", "bates")(style) then add(s"$path.style", Code.Setting, "Choose page, number, or bates")
          if !line(prefix, 24) then add(s"$path.prefix", Code.Setting, "Use a single-line prefix up to 24 characters")
        case Op.PreflightRules(rules) =>
          Policy.compile(rules).left.foreach(_.foreach { issue =>
            add(s"$path.rules." + issue.path.stripPrefix("profile."), issue.code, issue.message)
          })
        case Op.Preflight(profile) if profile != CourtProfile.CaliforniaSecondDistrictPetition.id =>
          add(s"$path.profile", Code.Setting, "Choose a supported court profile")
        case Op.Validate(profiles) if profiles.isEmpty || profiles.distinct != profiles || !profiles.forall(Profiles.contains) =>
          add(s"$path.profiles", Code.Setting, "Choose distinct profiles from 2b, 3b and 3u")
        case _ => ()
      val validArity = node.op match
        case Op.Input(_) | Op.Stop(_, _) => node.inputs.isEmpty
        case Op.Choose(_, _) => node.inputs.length == 4
        case Op.Assemble(_) => node.inputs.nonEmpty && node.inputs.length <= 4
        case Op.Keep(_, kinds) => node.inputs.length == kinds.length
        case Op.StopAfter(_, kinds, _) => node.inputs.length == kinds.length
        case _ => node.inputs.length == 1
      if !validArity then add(s"$path.inputs", Code.Arity, "Inputs do not match this operation's ports")
    }
    if recipe.nodes.count(_.op == Op.Write) > 3 then add("nodes", Code.Limit, "Use at most three PDF writing nodes")
    val duplicate = recipe.nodes.groupBy(_.id).exists(_._2.length > 1)
    if duplicate then add("nodes", Code.Duplicate, "Node IDs must be unique")
    val slots = recipe.nodes.collect { case Node(_, Op.Input(slot), _) => slot }.distinct
    if slots.length > 4 then add("nodes", Code.Limit, "Use at most four named document inputs")
    val fields = issues.result()
    if fields.nonEmpty then Left(fields)
    else
      val graph = recipe.nodes.map(n => n.id -> n).toMap
      recipe.nodes.zipWithIndex.foreach { (node, index) =>
        node.inputs.zipWithIndex.foreach { (id, port) => graph.get(id) match
          case None => add(s"nodes[$index].inputs[$port]", Code.Reference, s"Missing node: $id")
          case Some(source) =>
            val expected = node.op match
              case Op.Choose(_, kind) if port > 0 => kind
              case Op.Keep(_, kinds) => kinds(port)
              case Op.StopAfter(_, kinds, _) => kinds(port)
              case _ => node.op.input
            if source.op.output != expected then add(s"nodes[$index].inputs[$port]", Code.Type,
              s"${node.id} needs $expected at port ${port + 1}; $id produces ${source.op.output}")
        }
      }
      recipe.outputs.zipWithIndex.foreach { (id, index) => graph.get(id) match
        case None => add(s"outputs[$index]", Code.Reference, s"Missing output node: $id")
        case Some(node) if node.op.output == Kind.Document => add(s"outputs[$index]", Code.Output, "Write a PDF or validate written bytes before exposing an output")
        case _ => ()
      }
      if recipe.outputs.flatMap(graph.get).count(_.op.output == Kind.File) > 3 then
        add("outputs", Code.Limit, "Expose at most three PDF files per recipe")
      val refs = issues.result()
      if refs.nonEmpty then Left(refs)
      else
        // Kahn ordering is stable, independent of the order in the saved JSON.
        var remaining = recipe.nodes
        var sorted = Vector.empty[Node]
        var seen = Set.empty[String]
        var progress = true
        while remaining.nonEmpty && progress do
          val ready = remaining.filter(_.inputs.forall(seen))
          progress = ready.nonEmpty
          sorted ++= ready
          seen ++= ready.map(_.id)
          remaining = remaining.filterNot(n => seen(n.id))
        if remaining.nonEmpty then Left(Vector(Issue("nodes", Code.Cycle, "The graph contains a cycle; connect every path toward an output")))
        else
          var needed = recipe.outputs.toSet
          sorted.reverseIterator.foreach(node => if needed(node.id) then needed ++= node.inputs)
          Right(new Checked(recipe, sorted.filter(n => needed(n.id))))

  enum Value:
    case Document(value: PdfDocument)
    case File(value: PdfFile)
    case Receipt(value: PdfReceipt)

  final case class ValidationReport(json: String, verdicts: Map[String, Verdict] = Map.empty)
  final case class Decision(node: String, outcome: Outcome, selected: String, reason: String) derives Schema

  /** Expensive/application-specific operations remain at the interpreter boundary. */
  trait Services:
    /** Typed entry points. Legacy callbacks are checked once at this adapter. */
    def convert(file: PdfFile): PdfFile < (Abort[PdfError] & Sync) =
      convertPdfA3b(file.bytes).map(bytes => accept(PdfFile.checked(bytes)))
    def verify(file: PdfFile, profiles: Vector[PdfAProfile]): VerificationResult < (Abort[PdfError] & Sync) =
      validationReport(file.bytes, profiles.map(_.code)).map(result =>
        accept(VerificationResult.fromLegacy(result.json, result.verdicts)))
    def inspect(file: PdfFile, rules: Policy.Checked): ReportJson < (Abort[PdfError] & Sync) =
      preflightRules(file.bytes, rules).map(json => accept(ReportJson.parse(json)))
    def inspect(file: PdfFile, profile: String): ReportJson < (Abort[PdfError] & Sync) =
      preflight(file.bytes, profile).map(json => accept(ReportJson.parse(json)))
    def pageNumberEvidence(@scala.annotation.unused document: PdfDocument): (Outcome, String) < (Abort[PdfError] & Sync) =
      (Outcome.Unknown, "Visible page-number inspection is unavailable")
    def preflightRules(@scala.annotation.unused bytes: Vector[Byte], @scala.annotation.unused rules: Policy.Checked): String < (Abort[PdfError] & Sync) =
      Abort.fail(PdfError.InvalidPdf("Policy interpreter is unavailable"))
    def preflight(@scala.annotation.unused bytes: Vector[Byte], @scala.annotation.unused profile: String): String < (Abort[PdfError] & Sync) =
      Abort.fail(PdfError.InvalidPdf("Court preflight service is unavailable"))
    def convertPdfA3b(@scala.annotation.unused bytes: Vector[Byte]): Vector[Byte] < (Abort[PdfError] & Sync) =
      Abort.fail(PdfError.InvalidPdf("PDF/A-3b conversion service is unavailable"))
    def stamp(document: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync)
    def number(document: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync)
    def validate(@scala.annotation.unused bytes: Vector[Byte], @scala.annotation.unused profiles: Vector[String]): String < (Abort[PdfError] & Sync) =
      Abort.fail(PdfError.InvalidPdf("Provide a typed verify service or a legacy validation adapter"))
    /** Existing interpreters remain valid; opaque receipts have unknown verdicts. */
    def validationReport(bytes: Vector[Byte], profiles: Vector[String]): ValidationReport < (Abort[PdfError] & Sync) =
      validate(bytes, profiles).map(json => ValidationReport(json))

  final case class Run(outputs: Vector[(String, Value)], executed: Vector[String], skipped: Vector[String] = Vector.empty, decisions: Vector[Decision] = Vector.empty)
  final case class Deferred(node: String, reason: String)
  final case class Explanation(outputs: Vector[(String, Kind)], decisions: Vector[Decision], deferred: Vector[Deferred])

  private def accept[A](checked: Either[Vector[DataIssue], A]): A < Abort[PdfError] = checked match
    case Right(value) => value
    case Left(issues) => Abort.fail(PdfError.InvalidPdf(s"Invalid pipeline output: ${issues.mkString(", ")}"))

  /** Resolve choices only from available documents and already-resolved routes. */
  def explain(plan: Checked, inputs: Map[String, PdfDocument], services: Services): Explanation < (Abort[PdfError] & Sync) = Sync.defer {
    val graph = plan.order.map(node => node.id -> node).toMap
    val resolved = scala.collection.mutable.Map.empty[String, Either[String, Value]]
    val decisions = Vector.newBuilder[Decision]
    val deferred = Vector.newBuilder[Deferred]

    def inspect(id: String): Either[String, Value] < (Abort[PdfError] & Sync) = resolved.get(id) match
      case Some(value) => value
      case None =>
        val node = graph(id)
        val result: Either[String, Value] < (Abort[PdfError] & Sync) = node.op match
          case Op.Input(slot) => inputs.get(slot).map(Value.Document.apply).toRight(s"Bind a document to $slot")
          case Op.Choose(predicate, _) => inspect(node.inputs.head).map {
            case Left(reason) =>
              deferred += Deferred(id, reason)
              Left(reason)
            case Right(source) => test(predicate, source, services).map { (outcome, reason) =>
              val selected = selectBranch(node, outcome)
              decisions += Decision(id, outcome, selected, reason)
              inspect(selected)
            }
          }
          case _ =>
            def visit(index: Int): Either[String, Value] < (Abort[PdfError] & Sync) =
              if index == node.inputs.size then Left(s"${node.id} requires ${node.op.productPrefix} during execution")
              else inspect(node.inputs(index)).map(_ => visit(index + 1))
            visit(0)
        result.map { value =>
          resolved(id) = value
          value
        }
    def visitOutputs(index: Int): Explanation < (Abort[PdfError] & Sync) =
      if index == plan.recipe.outputs.length then
        Explanation(plan.recipe.outputs.map(id => id -> graph(id).op.output), decisions.result(), deferred.result())
      else inspect(plan.recipe.outputs(index)).map(_ => visitOutputs(index + 1))
    visitOutputs(0)
  }

  private def selectBranch(node: Node, outcome: Outcome): String =
    node.inputs(outcome match
      case Outcome.Yes => 1
      case Outcome.No => 2
      case Outcome.Unknown => 3)

  final case class Limits(maxPages: Int = 64, maxObjects: Int = 8000, maxBytes: Int = 12 * 1024 * 1024,
      maxLiveBytes: Long = 64L * 1024 * 1024, maxLiveObjects: Int = 32000)

  private def test(predicate: Predicate, source: Value, services: Services): (Outcome, String) < (Abort[PdfError] & Sync) = (predicate, source) match
    case (Predicate.HasPageNumbers, Value.Document(document)) => services.pageNumberEvidence(document)
    case (Predicate.PageCount(min, max), Value.Document(document)) => PdfPages.pageRefs(document).map { pages =>
      val reason = s"${pages.length} pages; expected $min to $max inclusive"
      pageRange(min, max).eval[Boolean](PageFacts(pages.length)) match
        case Right(Seq(matches)) => (if matches then Outcome.Yes else Outcome.No, reason)
        case _ => Abort.fail(PdfError.InvalidPdf("Page-count expression did not return one boolean"))
    }
    case (Predicate.Declares3b, Value.Document(document)) => PdfAIdentification.inspect(document).map { found =>
      val outcome = found.state match
        case PdfADeclarationState.Unreadable | PdfADeclarationState.Conflicting => Outcome.Unknown
        case _ => if found.declares3b then Outcome.Yes else Outcome.No
      (outcome, s"Active XMP: ${found.state}; this tests a declaration, not conformance")
    }
    case (Predicate.Validation(profile), Value.Receipt(receipt)) =>
      val verdict = receipt match
        case validation: PdfReceipt.Validation => PdfAProfile.parse(profile).toOption
          .flatMap(validation.result.verdicts.get).getOrElse(Verdict.Unable)
        case _: PdfReceipt.Inspection => Verdict.Unable
      (verdict match
        case Verdict.Passed => Outcome.Yes
        case Verdict.Failed => Outcome.No
        case Verdict.Unable => Outcome.Unknown, s"PDF/A-$profile: $verdict")
    case _ => Abort.fail(PdfError.InvalidPdf("Checked condition port changed type"))

  /** Demand-driven interpretation. Choices evaluate their condition, then only the
    * selected branch. Each demanded node runs once; unused branch references are
    * released as soon as the choice is known. Branches execute sequentially.
    */
  def run(plan: Checked, inputs: Map[String, PdfDocument], services: Services, limits: Limits = Limits()): Run < (Abort[PdfError] & Sync) =
    runObserved(plan, inputs, services, limits)((_, _) => ())

  /** Observe completed steps and decisions even when a selected operation aborts. */
  def runObserved(plan: Checked, inputs: Map[String, PdfDocument], services: Services, limits: Limits = Limits())(
      observe: (Vector[String], Vector[Decision]) => Unit
  ): Run < (Abort[PdfError] & Sync) = Sync.defer {
    if limits.maxBytes <= 0 || limits.maxBytes > PdfFile.MaxBytes then
      Abort.fail(PdfError.InvalidPdf(s"Pipeline file limit must be between 1 and ${PdfFile.MaxBytes} bytes"))
    else runChecked(plan, inputs, services, limits)(observe)
  }

  private def runChecked(plan: Checked, inputs: Map[String, PdfDocument], services: Services, limits: Limits)(
      observe: (Vector[String], Vector[Decision]) => Unit
  ): Run < (Abort[PdfError] & Sync) = Sync.defer {
    val graph = plan.order.map(node => node.id -> node).toMap
    val uses = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
    val memo = scala.collection.mutable.Map.empty[String, Value]
    val dependencies = scala.collection.mutable.Map.from(plan.order.map(node => node.id -> node.inputs))
    val executed = scala.collection.mutable.ArrayBuffer.empty[String]
    val decisions = scala.collection.mutable.ArrayBuffer.empty[Decision]
    plan.order.foreach(node => node.inputs.foreach(id => uses(id) += 1))
    plan.recipe.outputs.foreach(id => uses(id) += 1)

    def release(id: String): Unit =
      uses(id) -= 1
      if uses(id) == 0 then
        if memo.remove(id).isEmpty then dependencies(id).foreach(release)

    def retained(value: Value): Vector[(AnyRef, (Long, Long))] = value match
      case Value.Document(document) =>
        def payload(value: PdfValue): Long = value match
          case PdfValue.Literal(bytes) => bytes.length.toLong
          case PdfValue.Hex(bytes) => bytes.length.toLong
          case PdfValue.Array(values) => values.iterator.map(payload).sum
          case PdfValue.Dict(fields) => fields.iterator.map(field => field.name.length.toLong * 2 + payload(field.value)).sum
          case PdfValue.Name(value) => value.length.toLong * 2
          case _ => 0L
        Vector(document -> (document.objects.iterator.map(obj => obj.stream.fold(0L)(_.length.toLong) + payload(obj.value)).sum, document.objects.length.toLong))
      case Value.File(file) => Vector(file.bytes -> (file.size, 0L))
      case Value.Receipt(receipt) => Vector(receipt.subject.bytes -> (receipt.subject.size, 0L),
        receipt.report -> (receipt.report.countedBytes, receipt.report.nodes.toLong))

    def budget(value: Value): Value < Abort[PdfError] =
      val unique = new java.util.IdentityHashMap[AnyRef, java.lang.Boolean]()
      val total = (memo.valuesIterator ++ Iterator(value)).flatMap(retained).filter { (key, _) =>
        unique.put(key, java.lang.Boolean.TRUE) == null
      }.map(_._2).foldLeft((0L, 0L)) { (a, b) => (a._1 + b._1, a._2 + b._2) }
      if total._1 > limits.maxLiveBytes || total._2 > limits.maxLiveObjects then Abort.fail(PdfError.RetentionExceeded("Pipeline exceeds its live byte or object budget"))
      else value match
        case Value.Document(document) => PdfPages.pageRefs(document).map { pages =>
          if pages.isEmpty || pages.length > limits.maxPages || document.objects.length > limits.maxObjects then Abort.fail(PdfError.RetentionExceeded("Pipeline document exceeds its page or object limit"))
          else value
        }
        case Value.File(file) if file.size > limits.maxBytes => Abort.fail(PdfError.TooLarge(limits.maxBytes.toLong, file.size))
        case _ => value


    def evaluate(id: String): Value < (Abort[PdfError] & Sync) = memo.get(id) match
      case Some(value) => value
      case None =>
        val node = graph(id)
        def finish(value: Value): Value < Abort[PdfError] = budget(value).map { value =>
          memo(id) = value
          executed += id
          observe(executed.toVector, decisions.toVector)
          dependencies(id).foreach(release)
          value
        }
        node.op match
          case Op.Choose(predicate, _) => evaluate(node.inputs.head).map { source => test(predicate, source, services).map { (outcome, reason) =>
            val selectedIndex = outcome.ordinal + 1
            val selected = selectBranch(node, outcome)
            dependencies(id) = Vector(node.inputs.head, selected)
            node.inputs.zipWithIndex.filter((_, index) => index > 0 && index != selectedIndex).foreach((unused, _) => release(unused))
            decisions += Decision(id, outcome, selected, reason)
            observe(executed.toVector, decisions.toVector)
            evaluate(selected).map(finish)
          }}
          case Op.Stop(message, _) => Abort.fail(PdfError.InvalidPdf(s"Pipeline stopped at $id: $message"))
          case _ =>
            def arguments(index: Int, found: Vector[Value]): Vector[Value] < (Abort[PdfError] & Sync) =
              if index == node.inputs.length then found
              else evaluate(node.inputs(index)).map(value => arguments(index + 1, found :+ value))
            arguments(0, Vector.empty).map { args =>
              def document(index: Int = 0): PdfDocument = args(index) match
                case Value.Document(value) => value
                case _ => throw IllegalStateException("Checked document port changed type")
              val program: Value < (Abort[PdfError] & Sync) = node.op match
                case Op.Input(slot) => inputs.get(slot).fold[Value < Abort[PdfError]](Abort.fail(PdfError.InvalidPdf(s"Bind input $slot to a current PDF; this path needs it")))(doc => Value.Document(doc))
                case Op.Select(pages) => PdfPages.select(document(), pages).map(Value.Document.apply)
                case Op.Assemble(catalog) => PdfAssembly.concatenate(args.indices.map(i => document(i)).toVector, catalog).map(Value.Document.apply)
                case Op.Stamp(text, placement) => services.stamp(document(), text, placement).map(Value.Document.apply)
                case Op.NumberPages(start, style, prefix) => services.number(document(), start, style, prefix).map(Value.Document.apply)
                case Op.Write => ByteLimit.fromBytes(limits.maxBytes.toLong).map { limit =>
                  PdfWriter.write(document(), limit).map(bytes => accept(PdfFile.checked(bytes)).map(Value.File.apply))
                }
                case Op.ConvertPdfA3b => args.head match
                  case Value.File(file) => services.convert(file).map(Value.File.apply)
                  case _ => Abort.fail(PdfError.InvalidPdf("Checked archival conversion port changed type"))
                case Op.Keep(index, _) => args(index)
                case Op.StopAfter(message, _, _) => Abort.fail(PdfError.InvalidPdf(s"Pipeline stopped at $id: $message"))
                case Op.PreflightRules(rules) => args.head match
                  case Value.File(file) => Policy.compile(rules) match
                    case Right(plan) => services.inspect(file, plan).map(report => Value.Receipt(new PdfReceipt.Inspection(file, report)))
                    case Left(_) => Abort.fail(PdfError.InvalidPdf("Checked policy became invalid"))
                  case _ => Abort.fail(PdfError.InvalidPdf("Checked preflight port changed type"))
                case Op.Preflight(profile) => args.head match
                  case Value.File(file) => services.inspect(file, profile).map(report => Value.Receipt(new PdfReceipt.Inspection(file, report)))
                  case _ => Abort.fail(PdfError.InvalidPdf("Checked preflight port changed type"))
                case Op.Validate(profiles) => args.head match
                  case Value.File(file) =>
                    val typed = profiles.map(profile => PdfAProfile.parse(profile).toOption.get)
                    services.verify(file, typed).map { result =>
                      val extra = result.verdicts.keySet.diff(typed.toSet).toVector.sortBy(_.code)
                      if extra.nonEmpty then accept[Value](Left(extra.map(DataIssue.UnrequestedProfile.apply)))
                      else Value.Receipt(new PdfReceipt.Validation(file, result))
                    }
                  case _ => Abort.fail(PdfError.InvalidPdf("Checked file port changed type"))
                case _ => Abort.fail(PdfError.InvalidPdf("Unexpected routing operation"))
              program.map(finish)
            }

    def outputs(index: Int, found: Vector[(String, Value)]): Run < (Abort[PdfError] & Sync) =
      if index == plan.recipe.outputs.length then
        Run(found, executed.toVector, plan.recipe.nodes.map(_.id).filterNot(executed.contains), decisions.toVector)
      else
        val id = plan.recipe.outputs(index)
        evaluate(id).map(value => outputs(index + 1, found :+ (id -> value)))
    outputs(0, Vector.empty)
  }
