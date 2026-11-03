package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*

/** A closed, versioned program. Nodes contain settings and references, never code. */
object PdfPipeline:
  enum Kind derives Schema:
    case Document, File, Receipt

  enum Op derives Schema:
    case Input(slot: String)
    case Select(pages: Vector[Int])
    case Assemble(catalog: Int = 0)
    case Stamp(text: String, placement: String = "top")
    case NumberPages(start: Int = 1, style: String = "page", prefix: String = "")
    case Write
    case Validate(profiles: Vector[String])

    def output: Kind = this match
      case Write => Kind.File
      case Validate(_) => Kind.Receipt
      case _ => Kind.Document
    def input: Kind = this match
      case Validate(_) => Kind.File
      case _ => Kind.Document

  final case class Node(id: String, op: Op, inputs: Vector[String] = Vector.empty) derives Schema
  final case class Recipe(version: Int, name: String, nodes: Vector[Node], outputs: Vector[String]) derives Schema:
    /** Inline a reusable fragment, binding its named document inputs to this graph.
      * IDs are namespaced; no runtime closures or hidden references are introduced.
      */
    def include(prefix: String, fragment: Recipe, bindings: Map[String, String]): Either[Vector[Issue], Recipe] =
      if fragment.version != 1 then return Left(Vector(Issue("fragment.version", Code.Version, "Use recipe version 1")))
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
        val recipe = copy(nodes = nodes ++ added, outputs = (outputs ++ fragment.outputs.map(id => aliases.getOrElse(id, s"$prefix-$id"))).distinct)
        check(recipe).map(_.recipe)

  enum Code derives Schema:
    case Structure, Version, Limit, Identifier, Duplicate, Setting, Reference, Arity, Type, Cycle, Output, Binding
  final case class Issue(path: String, code: Code, message: String) derives Schema
  final class Checked private[PdfPipeline] (val recipe: Recipe, val order: Vector[Node])

  val MaxNodes = 24
  val MaxOutputs = 8
  val Profiles = Set("2b", "3b", "3u")
  private def identifier(value: String): Boolean = value.matches("[A-Za-z][A-Za-z0-9_-]{0,47}")
  private def line(value: String, max: Int): Boolean = value.length <= max && !value.exists(Character.isISOControl)

  /** Accumulate independent field failures first; graph checks require unique IDs. */
  def check(recipe: Recipe): Either[Vector[Issue], Checked] =
    val issues = Vector.newBuilder[Issue]
    def add(path: String, code: Code, message: String): Unit = issues += Issue(path, code, message)
    if recipe.version != 1 then add("version", Code.Version, "Use recipe version 1")
    if recipe.name.trim.isEmpty || !line(recipe.name, 80) then add("name", Code.Setting, "Give the recipe a single-line name up to 80 characters")
    if recipe.nodes.isEmpty || recipe.nodes.length > MaxNodes then add("nodes", Code.Limit, s"Use 1 to $MaxNodes nodes")
    if recipe.outputs.isEmpty || recipe.outputs.length > MaxOutputs || recipe.outputs.distinct != recipe.outputs then
      add("outputs", Code.Output, s"Choose 1 to $MaxOutputs distinct output nodes")
    recipe.nodes.zipWithIndex.foreach { (node, index) =>
      val path = s"nodes[$index]"
      if !identifier(node.id) then add(s"$path.id", Code.Identifier, "Use a letter followed by up to 47 letters, digits, hyphens or underscores")
      if node.inputs.length > 4 then add(s"$path.inputs", Code.Limit, "A node may have at most four inputs")
      node.op match
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
        case Op.Validate(profiles) if profiles.isEmpty || profiles.distinct != profiles || !profiles.forall(Profiles.contains) =>
          add(s"$path.profiles", Code.Setting, "Choose distinct profiles from 2b, 3b and 3u")
        case _ => ()
      val validArity = node.op match
        case Op.Input(_) => node.inputs.isEmpty
        case Op.Assemble(_) => node.inputs.nonEmpty && node.inputs.length <= 4
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
          case Some(source) if source.op.output != node.op.input => add(s"nodes[$index].inputs[$port]", Code.Type,
            s"${node.id} needs ${node.op.input}; $id produces ${source.op.output}")
          case _ => ()
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
    case File(bytes: Vector[Byte])
    case Receipt(json: String)

  /** Expensive/application-specific operations remain at the interpreter boundary. */
  trait Services:
    def stamp(document: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync)
    def number(document: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync)
    def validate(bytes: Vector[Byte], profiles: Vector[String]): String < (Abort[PdfError] & Sync)

  final case class Run(outputs: Vector[(String, Value)], executed: Vector[String])
  final case class Limits(maxPages: Int = 64, maxObjects: Int = 8000, maxBytes: Int = 12 * 1024 * 1024,
      maxLiveBytes: Long = 64L * 1024 * 1024, maxLiveObjects: Int = 32000)

  /** Interpret each reachable node once. Fanout shares its immutable result.
    * Execution is sequential; dead intermediates are released after their last use.
    */
  def run(plan: Checked, inputs: Map[String, PdfDocument], services: Services, limits: Limits = Limits()): Run < (Abort[PdfError] & Sync) = Sync.defer {
    val slots = plan.order.collect { case Node(_, Op.Input(slot), _) => slot }.toSet
    if !slots.subsetOf(inputs.keySet) then Abort.fail(PdfError.InvalidPdf("Bind every recipe input before running"))
    else
      val uses = scala.collection.mutable.Map.empty[String, Int].withDefaultValue(0)
      plan.order.foreach(node => node.inputs.foreach(id => uses(id) += 1))
      plan.recipe.outputs.foreach(id => uses(id) += 1)
      def weight(value: Value): (Long, Long) = value match
        case Value.Document(document) =>
          def payload(value: PdfValue): Long = value match
            case PdfValue.Literal(bytes) => bytes.length.toLong
            case PdfValue.Hex(bytes) => bytes.length.toLong
            case PdfValue.Array(values) => values.iterator.map(payload).sum
            case PdfValue.Dict(fields) => fields.iterator.map(field => field.name.length.toLong * 2 + payload(field.value)).sum
            case PdfValue.Name(value) => value.length.toLong * 2
            case _ => 0L
          (document.objects.iterator.map(obj => obj.stream.fold(0L)(_.length.toLong) + payload(obj.value)).sum, document.objects.length.toLong)
        case Value.File(bytes) => (bytes.length.toLong, 0L)
        case Value.Receipt(json) => (json.length.toLong * 2, 0L)
      def budget(value: Value, live: Map[String, Value]): Value < Abort[PdfError] =
        val total = (live.valuesIterator ++ Iterator(value)).map(weight).foldLeft((0L, 0L)) { (a, b) => (a._1 + b._1, a._2 + b._2) }
        if total._1 > limits.maxLiveBytes || total._2 > limits.maxLiveObjects then
          Abort.fail(PdfError.RetentionExceeded("Pipeline exceeds its live byte or object budget"))
        else value match
          case Value.Document(document) => PdfPages.pageRefs(document).map { pages =>
            if pages.length > limits.maxPages || document.objects.length > limits.maxObjects then Abort.fail(PdfError.RetentionExceeded("Pipeline document exceeds its page or object limit"))
            else value
          }
          case Value.File(bytes) if bytes.length > limits.maxBytes => Abort.fail(PdfError.TooLarge(limits.maxBytes.toLong, bytes.length.toLong))
          case _ => value
      def loop(index: Int, live: Map[String, Value]): Run < (Abort[PdfError] & Sync) =
        if index == plan.order.length then Run(plan.recipe.outputs.map(id => id -> live(id)), plan.order.map(_.id))
        else
          val node = plan.order(index)
          val args = node.inputs.map(live)
          def document(index: Int = 0): PdfDocument = args(index) match
            case Value.Document(value) => value
            case _ => throw IllegalStateException("Checked document port changed type")
          val program: Value < (Abort[PdfError] & Sync) = node.op match
            case Op.Input(slot) => Value.Document(inputs(slot))
            case Op.Select(pages) => PdfPages.select(document(), pages).map(Value.Document.apply)
            case Op.Assemble(catalog) => PdfAssembly.concatenate(args.indices.map(i => document(i)).toVector, catalog).map(Value.Document.apply)
            case Op.Stamp(text, placement) => services.stamp(document(), text, placement).map(Value.Document.apply)
            case Op.NumberPages(start, style, prefix) => services.number(document(), start, style, prefix).map(Value.Document.apply)
            case Op.Write => PdfWriter.write(document()).map(Value.File.apply)
            case Op.Validate(profiles) => args.head match
              case Value.File(bytes) => services.validate(bytes, profiles).map(Value.Receipt.apply)
              case _ => Abort.fail(PdfError.InvalidPdf("Checked file port changed type"))
          program.map(value => budget(value, live).map { value =>
            node.inputs.foreach(id => uses(id) -= 1)
            val next = (live + (node.id -> value)).filter((id, _) => uses(id) > 0)
            loop(index + 1, next)
          })
      loop(0, Map.empty)
  }
