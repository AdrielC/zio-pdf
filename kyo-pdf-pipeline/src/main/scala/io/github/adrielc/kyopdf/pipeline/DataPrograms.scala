package io.github.adrielc.kyopdf.pipeline

import kyo.*
import zio.blocks.schema.{Schema as BSchema}
import zio.blocks.schema.json.Json
import StepRegistry.*
import PdfPipeline.{Outcome, Issue, Code}

/** Small, total predicates over schema data. No reflection, scripts, class
  * loading, arbitrary regexes, or closures occur in serialized definitions.
  */
object DataPrograms:
  enum Literal:
    case Text(value: String)
    case Number(value: Long)
    case Bool(value: Boolean)
  enum Comparison:
    case Equal, NotEqual, Less, AtMost, Greater, AtLeast
  enum Predicate:
    case Compare(path: Vector[String], comparison: Comparison, value: Literal)
    case All(tests: Vector[Predicate])
    case Any(tests: Vector[Predicate])
    case Not(test: Predicate)
  enum Unknown:
    case Fail, KeepAndFlag, DropAndFlag
  final case class Filter(test: Predicate, unknown: Unknown = Unknown.Fail)
  final case class ItemFlag(index: Int, outcome: Outcome, message: String)
  final case class Collection[A](items: Vector[A], flags: Vector[ItemFlag] = Vector.empty)
  final case class Gate(test: Predicate)
  private val filterSchema = new DataSchema("data.filter.settings", 1, BSchema.derived[Filter])
  private val gateSchema = new DataSchema("data.gate.settings", 1, BSchema.derived[Gate])

  def validate(test: Predicate): Vector[Issue] =
    val pending = scala.collection.mutable.ArrayDeque(test -> 0)
    var count = 0
    var invalid = false
    while pending.nonEmpty && count <= 64 do
      val (next, depth) = pending.removeLast()
      count += 1
      if depth > 16 then invalid = true
      else next match
        case Predicate.Compare(path, _, literal) =>
          if path.isEmpty || path.size > 16 || path.exists(p => p.isEmpty || p.length > 64) then invalid = true
          literal match
            case Literal.Text(value) if value.length > 1024 => invalid = true
            case _ => ()
        case Predicate.All(tests) =>
          if tests.isEmpty || tests.size > 16 then invalid = true
          else tests.foreach(t => pending.append(t -> (depth + 1)))
        case Predicate.Any(tests) =>
          if tests.isEmpty || tests.size > 16 then invalid = true
          else tests.foreach(t => pending.append(t -> (depth + 1)))
        case Predicate.Not(test) => pending.append(test -> (depth + 1))
    if invalid || count > 64 then Vector(Issue("settings.test", Code.Limit, "Predicate exceeds its path, depth, literal, or node bound")) else Vector.empty

  def evaluate(test: Predicate, value: Json): Outcome =
    import Outcome.{Yes, No, Unknown as Unresolved}
    def field(path: Vector[String]): Option[Json] = path.foldLeft(Option(value)) { (found, key) => found.flatMap {
      case obj: Json.Object => obj.value.find(_._1 == key).map(_._2)
      case _ => None
    }}
    def compare(order: Int, comparison: Comparison): Outcome =
      val matches = comparison match
        case Comparison.Equal => order == 0
        case Comparison.NotEqual => order != 0
        case Comparison.Less => order < 0
        case Comparison.AtMost => order <= 0
        case Comparison.Greater => order > 0
        case Comparison.AtLeast => order >= 0
      if matches then Yes else No
    test match
      case Predicate.Compare(path, comparison, literal) => (field(path), literal) match
        case (Some(Json.String(actual)), Literal.Text(expected)) => compare(actual.compareTo(expected), comparison)
        case (Some(Json.Number(actual)), Literal.Number(expected)) => compare(actual.compare(BigDecimal(expected)), comparison)
        case (Some(Json.Boolean(actual)), Literal.Bool(expected)) => compare(actual.compare(expected), comparison)
        case _ => Unresolved
      case Predicate.Not(test) => evaluate(test, value) match
        case Yes => No
        case No => Yes
        case Unresolved => Unresolved
      case Predicate.All(tests) =>
        val results = tests.map(evaluate(_, value))
        if results.contains(No) then No else if results.contains(Unresolved) then Unresolved else Yes
      case Predicate.Any(tests) =>
        val results = tests.map(evaluate(_, value))
        if results.contains(Yes) then Yes else if results.contains(Unresolved) then Unresolved else No

  def filter[A](id: String, collection: DataSchema[Collection[A]], item: DataSchema[A], example: Filter): Step[Collection[A], Collection[A], Filter] =
    new Step(id, 1, Port.data(collection), Port.data(collection), filterSchema, Vector(example), StepRegistry.Access.Document,
      Set(Capability.Inspect), p => validate(p.test))((input, settings, _) =>
      if input.items.size > ExecutionPlanner.MaxItems || input.flags.size > ExecutionPlanner.MaxItems then
        Abort.fail(Error(ErrorCode.Limit, "Collection has too many items or flags"))
      else
        val kept = Vector.newBuilder[A]
        val flags = Vector.newBuilder[ItemFlag]
        flags ++= input.flags
        var needsReview = false
        input.items.zipWithIndex.foreach { (entry, index) => evaluate(settings.test, Json.parseUnsafe(item.encode(entry))) match
          case Outcome.Yes => kept += entry
          case Outcome.No => ()
          case Outcome.Unknown => settings.unknown match
            case Unknown.Fail => needsReview = true
            case Unknown.KeepAndFlag => kept += entry; flags += ItemFlag(index, Outcome.Unknown, "Filter needs missing or incompatible evidence")
            case Unknown.DropAndFlag => flags += ItemFlag(index, Outcome.Unknown, "Filter omitted an unresolved item")
        }
        if needsReview then Abort.fail(Error(ErrorCode.Rejected, "Filter has unknown evidence; review required"))
        else
          val found = flags.result()
          if found.size > ExecutionPlanner.MaxItems then Abort.fail(Error(ErrorCode.Limit, "Collection flag budget exceeded"))
          else Collection(kept.result(), found))

  def gate[A](id: String, schema: DataSchema[A], example: Gate): Step[A, A, Gate] =
    new Step(id, 1, Port.data(schema), Port.data(schema), gateSchema, Vector(example), StepRegistry.Access.Document,
      Set(Capability.Verify), p => validate(p.test))((input, settings, _) => evaluate(settings.test, Json.parseUnsafe(schema.encode(input))) match
        case Outcome.Yes => input
        case Outcome.No => Abort.fail(Error(ErrorCode.Rejected, "Required data check failed"))
        case Outcome.Unknown => Abort.fail(Error(ErrorCode.Rejected, "Required data check needs review")))
