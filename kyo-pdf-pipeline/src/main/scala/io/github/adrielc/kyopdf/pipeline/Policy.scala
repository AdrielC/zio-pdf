package io.github.adrielc.kyopdf.pipeline

import java.time.LocalDate
import scala.util.Try
import zio.blocks.schema.{DynamicOptic, Schema as BlocksSchema, SchemaExpr}
import zio.blocks.schema.json.{Json, JsonCodecDeriver}
import PdfPipeline.{Code, Issue}

/** Versioned validation programs over PDF evidence. Unknown evidence requires review. */
object Policy:
  enum Scope derives kyo.Schema:
    case Document, Page, Bookmark

  enum Metric(val scope: Scope) derives kyo.Schema:
    case FileBytes extends Metric(Scope.Document)
    case PageCount extends Metric(Scope.Document)
    case BookmarkCount extends Metric(Scope.Document)
    case TextCharacters extends Metric(Scope.Page)
    case PageNumber extends Metric(Scope.Page)
    case PageLabelMatches extends Metric(Scope.Page)
    case VisibleNumberMatches extends Metric(Scope.Page)
    case DestinationResolves extends Metric(Scope.Bookmark)
    case ExhibitDestinationMatches extends Metric(Scope.Bookmark)

  enum Comparison derives kyo.Schema:
    case Equal, NotEqual, LessThan, LessThanOrEqual, GreaterThan, GreaterThanOrEqual

  enum Operand derives kyo.Schema:
    case Fact(metric: Metric)
    case Number(value: Long)

  enum Condition derives kyo.Schema:
    case Compare(left: Operand, comparison: Comparison, right: Operand)
    case All(conditions: Vector[Condition])
    case Any(conditions: Vector[Condition])
    case Not(condition: Condition)
    case Review

  object Condition:
    given blocksSchema: BlocksSchema[Condition] = BlocksSchema.derived

  enum Status(val label: String) derives kyo.Schema:
    case Pass extends Status("pass")
    case FixRequired extends Status("fix-required")
    case NeedsReview extends Status("needs-review")

  final case class Rule(id: String, scope: Scope, condition: Condition, citation: String, source: String,
      message: String, onFailure: Status = Status.FixRequired, when: Option[Condition] = None) derives kyo.Schema
  final case class Profile(version: Int, id: String, name: String, authority: String, documentType: String,
      effectiveDate: String, source: String, rules: Vector[Rule]) derives kyo.Schema

  object Rule:
    given blocksSchema: BlocksSchema[Rule] = BlocksSchema.derived
  object Profile:
    given blocksSchema: BlocksSchema[Profile] = BlocksSchema.derived

  final case class Observation(scope: Scope, values: Map[Metric, Long], page: Option[Int] = None,
      label: String = "", notes: Vector[String] = Vector.empty)
  final case class Finding(check: String, status: String, rule: String, source: String, message: String,
      page: Option[Int], evidence: Map[String, Long])
  final case class Report(kind: String, profile: Profile, sha256: String, profileSha256: String, bytes: Long,
      status: String, findings: Vector[Finding])

  private final case class Facts(values: Map[String, Long])
  private lazy val factsSchema = BlocksSchema.derived[Facts]
  private lazy val profileCodec = BlocksSchema[Profile].derive(JsonCodecDeriver.withRejectExtraFields(true))
  private lazy val reportCodec = BlocksSchema.derived[Report].derive(JsonCodecDeriver)
  val MaxJsonChars = 64 * 1024

  def encode(profile: Profile): String = profileCodec.encodeToString(profile)
  def encodeReport(report: Report): String = reportCodec.encodeToString(report)
  /** Recursive condition references are explicit so schema export stays finite. */
  lazy val jsonSchema: Json =
    import Json.{Object as Obj, Array as Arr, String as Str, Number as Num, Boolean as Bool}
    def enumValues(values: Iterable[String]): Json = Obj("enum" -> Arr(values.toSeq.map(Str.apply)*))
    def obj(fields: (String, Json)*): Json = Obj("type" -> Str("object"),
      "additionalProperties" -> Bool(false),
      "required" -> Arr(fields.map((name, _) => Str(name))*),
      "properties" -> Obj(fields*))
    def tagged(name: String, fields: (String, Json)*): Json = obj(name -> obj(fields*))
    val text = Obj("type" -> Str("string"), "minLength" -> Num(1), "maxLength" -> Num(1000))
    val number = Obj("type" -> Str("integer"), "minimum" -> Num(Long.MinValue), "maximum" -> Num(Long.MaxValue))
    val condition = Obj("$ref" -> Str("#/$defs/condition"))
    val conditions = Obj("type" -> Str("array"), "minItems" -> Num(1), "maxItems" -> Num(16), "items" -> condition)
    val operand = Obj("oneOf" -> Arr(
      tagged("Fact", "metric" -> enumValues(Metric.values.map(_.toString))),
      tagged("Number", "value" -> number)))
    val conditionSchema = Obj("oneOf" -> Arr(
      tagged("Compare", "left" -> operand, "comparison" -> enumValues(Comparison.values.map(_.toString)), "right" -> operand),
      tagged("All", "conditions" -> conditions), tagged("Any", "conditions" -> conditions),
      tagged("Not", "condition" -> condition), tagged("Review")))
    val ruleFields = Vector("id" -> text, "scope" -> enumValues(Scope.values.map(_.toString)),
      "condition" -> condition, "citation" -> text, "source" -> text, "message" -> text)
    val rule = Obj("type" -> Str("object"), "additionalProperties" -> Bool(false),
      "required" -> Arr(ruleFields.map((key, _) => Str(key))*),
      "properties" -> Obj((ruleFields ++ Vector(
        "onFailure" -> enumValues(Vector("FixRequired", "NeedsReview")),
        "when" -> Obj("anyOf" -> Arr(condition, Obj("type" -> Str("null"))))))*))
    val fields = Vector("version" -> Obj("const" -> Num(1)), "id" -> text, "name" -> text, "authority" -> text,
      "documentType" -> text, "effectiveDate" -> text, "source" -> text,
      "rules" -> Obj("type" -> Str("array"), "minItems" -> Num(1), "maxItems" -> Num(64), "items" -> rule))
    Obj("$id" -> Str("urn:kyo-pdf:policy:v1"), "$defs" -> Obj("condition" -> conditionSchema),
      "type" -> Str("object"), "additionalProperties" -> Bool(false),
      "required" -> Arr(fields.map((key, _) => Str(key))*), "properties" -> Obj(fields*))

  def decode(json: String): Either[Vector[Issue], Checked] =
    if json.length > MaxJsonChars then Left(Vector(Issue("profile", Code.Limit, "Profile JSON exceeds 64 KiB of characters")))
    else if !boundedJsonText(json) then Left(Vector(Issue("profile", Code.Limit, "Profile JSON exceeds its nesting budget")))
    else
      Json.parse(json).left.map(_ => Vector(Issue("profile", Code.Structure, "Expected valid profile JSON"))).flatMap { value =>
        def bounded(value: Json, depth: Int): Boolean = depth <= 40 && (value match
          case Json.Object(fields) => fields.map(_._1).distinct.size == fields.size && fields.forall(v => bounded(v._2, depth + 1))
          case Json.Array(values) => values.size <= 256 && values.forall(bounded(_, depth + 1))
          case _ => true)
        if !bounded(value, 0) then Left(Vector(Issue("profile", Code.Limit, "Use bounded profile JSON with unique fields")))
        else profileCodec.decode(normalizeProfile(value).print).left.map(error => Vector(Issue("profile", Code.Structure, error.toString))).flatMap(compile)
      }

  /** Migrate the first profile field names while rejecting ambiguous mixed input. */
  private[pipeline] def normalizeProfile(value: Json): Json = value match
    case obj: Json.Object =>
      val names = obj.value.map(_._1).toSet
      obj.copy(value = obj.value.map {
        case ("court", value) if !names("authority") => "authority" -> value
        case ("filingType", value) if !names("documentType") => "documentType" -> value
        case field => field
      })
    case other => other

  private def boundedJsonText(json: String): Boolean =
    var depth = 0
    var quoted = false
    var escaped = false
    var containers = 0
    json.forall { char =>
      if escaped then escaped = false
      else if quoted && char == '\\' then escaped = true
      else if char == '"' then quoted = !quoted
      else if !quoted then
        if char == '{' || char == '[' then
          depth += 1
          containers += 1
        else if char == '}' || char == ']' then depth -= 1
      depth >= 0 && depth <= 40 && containers <= 4096
    }

  private def line(value: String, max: Int): Boolean = value.trim.nonEmpty && value.length <= max && !value.exists(Character.isISOControl)
  private def identifier(value: String): Boolean = value.matches("[A-Za-z][A-Za-z0-9_-]{0,79}")

  def compile(profile: Profile): Either[Vector[Issue], Checked] =
    val issues = Vector.newBuilder[Issue]
    def add(path: String, code: Code, message: String): Unit = issues += Issue(path, code, message)
    if profile.version != 1 then add("profile.version", Code.Version, "Use policy version 1")
    if !identifier(profile.id) then add("profile.id", Code.Identifier, "Use a profile identifier up to 80 characters")
    Vector("name" -> profile.name, "authority" -> profile.authority, "documentType" -> profile.documentType).foreach { (key, value) =>
      if !line(value, 160) then add(s"profile.$key", Code.Setting, "Supply single-line text up to 160 characters")
    }
    if Try(LocalDate.parse(profile.effectiveDate)).isFailure then add("profile.effectiveDate", Code.Setting, "Use an ISO date")
    if !line(profile.source, 1000) then add("profile.source", Code.Setting, "Supply a source citation up to 1000 characters")
    if profile.rules.isEmpty || profile.rules.size > 64 then add("profile.rules", Code.Limit, "Use one to 64 rules")
    if profile.rules.map(_.id).distinct.size != profile.rules.size then add("profile.rules", Code.Duplicate, "Rule IDs must be unique")
    var expressions = 0
    def condition(value: Condition, scope: Scope, path: String, depth: Int): Unit =
      expressions += 1
      if depth > 8 || expressions > 256 then add(path, Code.Limit, "Use at most 256 expressions with depth up to eight")
      else value match
        case Condition.Compare(left, _, right) =>
          Vector("left" -> left, "right" -> right).foreach {
            case (side, Operand.Fact(metric)) if metric.scope != scope => add(s"$path.$side", Code.Type, s"$metric requires ${metric.scope} scope")
            case _ => ()
          }
        case Condition.All(values) =>
          if values.isEmpty || values.size > 16 then add(path, Code.Limit, "All requires one to 16 expressions")
          else values.zipWithIndex.foreach((v, i) => condition(v, scope, s"$path.conditions[$i]", depth + 1))
        case Condition.Any(values) =>
          if values.isEmpty || values.size > 16 then add(path, Code.Limit, "Any requires one to 16 expressions")
          else values.zipWithIndex.foreach((v, i) => condition(v, scope, s"$path.conditions[$i]", depth + 1))
        case Condition.Not(value) => condition(value, scope, s"$path.condition", depth + 1)
        case Condition.Review => ()
    profile.rules.take(64).zipWithIndex.foreach { (rule, i) =>
      val path = s"profile.rules[$i]"
      if !identifier(rule.id) then add(s"$path.id", Code.Identifier, "Supply a rule identifier")
      Vector("citation" -> rule.citation, "source" -> rule.source, "message" -> rule.message).foreach { (key, value) =>
        if !line(value, 1000) then add(s"$path.$key", Code.Setting, "Supply single-line text up to 1000 characters")
      }
      if rule.onFailure == Status.Pass then add(s"$path.onFailure", Code.Setting, "A failed rule requires a fix or review")
      rule.when.foreach(condition(_, rule.scope, s"$path.when", 0))
      condition(rule.condition, rule.scope, s"$path.condition", 0)
    }
    val errors = issues.result()
    if errors.nonEmpty then Left(errors)
    else Right(new Checked(profile, profile.rules.map(rule => CompiledRule(rule, lower(rule.condition), rule.when.map(lower)))))

  private enum Expression:
    case Compare(value: SchemaExpr[Facts, Boolean], metrics: Set[Metric])
    case All(values: Vector[Expression])
    case Any(values: Vector[Expression])
    case Not(value: Expression)
    case Review

  private def lower(value: Condition): Expression = value match
    case Condition.Compare(left, comparison, right) =>
      def operand(value: Operand): SchemaExpr[Facts, Long] = value match
        case Operand.Number(value) => SchemaExpr.literal[Facts, Long](value)
        case Operand.Fact(metric) => SchemaExpr.optic[Facts, Long](DynamicOptic.root.field("values").atKey(metric.toString), factsSchema)
      val operator = comparison match
        case Comparison.Equal => SchemaExpr.RelationalOperator.Equal
        case Comparison.NotEqual => SchemaExpr.RelationalOperator.NotEqual
        case Comparison.LessThan => SchemaExpr.RelationalOperator.LessThan
        case Comparison.LessThanOrEqual => SchemaExpr.RelationalOperator.LessThanOrEqual
        case Comparison.GreaterThan => SchemaExpr.RelationalOperator.GreaterThan
        case Comparison.GreaterThanOrEqual => SchemaExpr.RelationalOperator.GreaterThanOrEqual
      val metrics = Vector(left, right).collect { case Operand.Fact(metric) => metric }.toSet
      Expression.Compare(SchemaExpr.relational(operand(left), operand(right), operator), metrics)
    case Condition.All(values) => Expression.All(values.map(lower))
    case Condition.Any(values) => Expression.Any(values.map(lower))
    case Condition.Not(value) => Expression.Not(lower(value))
    case Condition.Review => Expression.Review

  private def evaluate(value: Expression, observation: Observation): Option[Boolean] = value match
    case Expression.Compare(expression, metrics) =>
      if !metrics.subsetOf(observation.values.keySet) then None
      else expression.eval[Boolean](Facts(observation.values.map((metric, value) => metric.toString -> value))) match
        case Right(Seq(result)) => Some(result)
        case _ => None
    case Expression.All(values) =>
      val outcomes = values.map(evaluate(_, observation))
      if outcomes.contains(Some(false)) then Some(false) else if outcomes.forall(_.contains(true)) then Some(true) else None
    case Expression.Any(values) =>
      val outcomes = values.map(evaluate(_, observation))
      if outcomes.contains(Some(true)) then Some(true) else if outcomes.forall(_.contains(false)) then Some(false) else None
    case Expression.Not(value) => evaluate(value, observation).map(!_)
    case Expression.Review => None

  private final case class CompiledRule(rule: Rule, condition: Expression, when: Option[Expression])
  final class Checked private[Policy] (val profile: Profile, private val rules: Vector[CompiledRule]):
    /** Return all independent findings. Missing evidence never becomes a passing rule. */
    def run(observations: Vector[Observation]): Vector[Finding] =
      rules.flatMap { compiled =>
        val rule = compiled.rule
        val candidates = observations.filter(_.scope == rule.scope) match
          case Vector() => Vector(Observation(rule.scope, Map.empty, notes = Vector("Inspection supplied no evidence for this scope.")))
          case values => values
        candidates.flatMap { observation =>
          val applies = compiled.when.fold(Option(true))(evaluate(_, observation))
          if applies.contains(false) then None
          else
            val outcome = if applies.isEmpty then None else evaluate(compiled.condition, observation)
            val status = outcome match
              case Some(true) => Status.Pass
              case Some(false) => rule.onFailure
              case None => Status.NeedsReview
            val detail = (Vector(observation.label).filter(_.nonEmpty) ++ observation.notes).mkString(" ")
            Some(Finding(rule.id, status.label, rule.citation, rule.source,
              rule.message + (if detail.isEmpty then "" else s" $detail"), observation.page,
              observation.values.map((metric, value) => metric.toString -> value)))
        }
      }
