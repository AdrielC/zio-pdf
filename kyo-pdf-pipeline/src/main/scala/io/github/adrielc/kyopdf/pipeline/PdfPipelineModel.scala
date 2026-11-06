package io.github.adrielc.kyopdf.pipeline

import PdfPipeline.*
import zio.blocks.chunk.{ChunkMap, NonEmptyChunk}
import zio.blocks.schema.{Schema as BlocksSchema, SchemaError, DynamicOptic, DynamicValue, Reflect}
import zio.blocks.schema.binding.Binding
import zio.blocks.schema.toon.ToonCodecDeriver
import zio.blocks.schema.json.{Json, JsonSchema, JsonCodecDeriver, NonNegativeInt, RegexPattern}

/** Schema-derived transport plus registry-driven graph constraints. */
object PdfPipelineModel:
  val BranchFields = Vector("source", "yes", "no", "unknown")
  private def encoded[A: BlocksSchema](value: A): Json =
    Json.parseUnsafe(BlocksSchema[A].derive(JsonCodecDeriver).encodeToString(value))
  private def nonEmpty[A](values: Vector[A]): NonEmptyChunk[A] = NonEmptyChunk.fromIterableOption(values)
    .getOrElse(throw IllegalArgumentException("Expected non-empty schema alternatives"))
  private def count(value: Int): NonNegativeInt = NonNegativeInt.unsafe(value)
  private def alternatives(values: Vector[JsonSchema]): JsonSchema = JsonSchema.Object(oneOf = Some(nonEmpty(values)))
  private def choices(values: String*): JsonSchema = JsonSchema.enumOfStrings(nonEmpty(values.toVector))
  private def array(items: JsonSchema, min: Int = 0, max: Int): JsonSchema = JsonSchema.array(items, count(min), count(max))
  private def jsonObject(value: Json): Json.Object = value match
    case obj: Json.Object => obj
    case _ => throw IllegalArgumentException("Expected an encoded object")
  private def blocksSchema(value: Json): JsonSchema = JsonSchema.fromJson(value)
    .fold(error => throw IllegalArgumentException(error.message), identity)

  private val text = BlocksSchema[String].toJsonSchema
  private val integer = BlocksSchema[Int].toJsonSchema
  private val pageIndex = JsonSchema.integer(minimum = Some(BigDecimal(1)), maximum = Some(BigDecimal(64)))
  private val identifier = JsonSchema.string(RegexPattern.unsafe("^[A-Za-z][A-Za-z0-9_-]{0,47}$"))
  private case class Field(name: String, schema: JsonSchema, example: Option[Json] = None, optional: Boolean = false):
    def documented: JsonSchema = (schema, example) match
      case (obj: JsonSchema.Object, Some(value)) => obj.copy(examples = Some(NonEmptyChunk(value)))
      case _ => schema
  private def record(fields: Vector[Field]): JsonSchema.Object = JsonSchema.Object(
    `type` = Some(zio.blocks.schema.json.SchemaType.Single(zio.blocks.schema.json.JsonSchemaType.Object)),
    properties = Some(ChunkMap.from(fields.map(f => f.name -> f.documented))),
    required = Some(fields.filterNot(_.optional).map(_.name).toSet), additionalProperties = Some(JsonSchema.False))
  private case class Variant(name: String, tag: String, fields: Vector[Field], description: String = "", arity: Option[(Int, Int)] = None):
    def described(value: String): Variant = copy(description = value)
    def wired(min: Int, max: Int): Variant = copy(arity = Some(min -> max))
    def schema(discriminator: String, common: Vector[Field] = Vector.empty): JsonSchema.Object =
      val fieldsWithArity = (common ++ fields).map { field => (field.name, field.schema, arity) match
        case ("inputs", obj: JsonSchema.Object, Some((min, max))) =>
          field.copy(schema = obj.copy(minItems = Some(count(min)), maxItems = Some(count(max))))
        case _ => field
      }
      val base = record(fieldsWithArity)
      base.copy(properties = Some(ChunkMap.from(Vector(discriminator -> JsonSchema.constOf(encoded(name))) ++
        base.properties.toVector.flatMap(_.iterator))), required = Some(base.required.getOrElse(Set.empty) + discriminator),
        description = Option.when(description.nonEmpty)(description))
  private def variant(name: String, tag: String, fields: Field*): Variant = Variant(name, tag, fields.toVector)
  private def str(name: String, value: String, optional: Boolean = false): Field = Field(name, text, Some(encoded(value)), optional)
  private def int(name: String, value: Int, optional: Boolean = false): Field = Field(name, integer, Some(encoded(value)), optional)
  private val kinds = alternatives(Vector(choices("document", "file", "receipt"),
    record(Vector(Field("Data", blocksSchema(StepSchema.exported(BlocksSchema[DataIdentity], "model_data_kind")))))))
  private val port = Field("kind", kinds, Some(encoded("document")), true)
  private val ports = Field("kinds", array(kinds, max = 4), Some(encoded(Vector("document"))))
  private val profiles = choices(Profiles.toVector.sorted*)
  private val predicates = Vector(
    variant("policy-passed", "PolicyPassed").described("Read a policy result: Yes passes, No fails, Unknown requires review."),
    variant("page-count", "PageCount", Field("min", pageIndex, Some(encoded(1))), Field("max", pageIndex, Some(encoded(3)))).described("Inspect a Document's inclusive page-count range."),
    variant("declares-3b", "Declares3b").described("Inspect a Document's declaration; this does not prove conformance."),
    variant("validation", "Validation", Field("profile", profiles, Some(encoded("3b")))).described("Inspect a validation Receipt."),
    variant("has-page-numbers", "HasPageNumbers").described("Inspect a Document for visible numbering; preserve unknown evidence.")
  )
  private def registeredCallSchema(registry: StepRegistry.Registry): JsonSchema = alternatives(registry.entries.map { entry =>
    val descriptor = entry.descriptor.fields.toMap
    record(Vector(
      Field("operation", JsonSchema.constOf(encoded(entry.id))), Field("version", JsonSchema.constOf(encoded(entry.version))),
      Field("settings", blocksSchema(descriptor("parameters"))),
      Field("inputs", JsonSchema.constOf(encoded(entry.inputs.map(kindValue(_, canonical = true))))),
      Field("output", JsonSchema.constOf(encoded(kindValue(entry.output, canonical = true)))),
      Field("onError", choices("Fail", "RecoverAndFlag"), optional = true)))
  })
  def registeredSchema(registry: StepRegistry.Registry = StepRegistry.standard): Json.Object = jsonObject(registeredCallSchema(registry).toJson)
  private lazy val operations = Vector(
    variant("registered", "Registered", Field("call", registeredCallSchema(StepRegistry.standard))),
    variant("input", "Input", str("slot", "body")).wired(0, 0).described("No inputs -> Document. Bind the uploaded document named by slot."),
    variant("select", "Select", Field("pages", array(integer, 1, 64), Some(encoded(Vector(1))))).wired(1, 1).described("Document -> Document. Select one-based pages in the requested order."),
    variant("assemble", "Assemble", int("catalog", 0, true)).wired(1, 4).described("One to four Documents -> Document. Concatenate in input order; catalog is a zero-based input index."),
    variant("stamp", "Stamp", str("text", "REVIEW"), Field("placement", choices("top", "center"), Some(encoded("top")), true)).wired(1, 1).described("Document -> Document. Add visible text at top or center."),
    variant("number", "NumberPages", int("start", 1, true), Field("style", choices("page", "number", "bates"), Some(encoded("page")), true), str("prefix", "", true)).wired(1, 1).described("Document -> Document. Add visible page numbers; prefix precedes each number."),
    variant("write", "Write").wired(1, 1).described("Document -> File. Write PDF bytes before using File operations. Do not write an existing File again."),
    variant("convert-pdfa-3b", "ConvertPdfA3b").wired(1, 1).described("File -> File. Convert through the configured service; validate the resulting bytes separately."),
    variant("preflight-rules", "PreflightRules", Field("rules", blocksSchema(Policy.jsonSchema))).wired(1, 1).described("File -> Receipt. Evaluate the supplied serializable policy."),
    variant("preflight", "Preflight", Field("profile", choices(CourtProfile.CaliforniaSecondDistrictPetition.id))).wired(1, 1).described("File -> Receipt. Evaluate a named preflight profile."),
    variant("validate", "Validate", Field("profiles", array(profiles, 1, 3), Some(encoded(Vector("3b"))))).wired(1, 1).described("File -> Receipt. Validate these exact bytes against the requested profiles."),
    variant("choose", "Choose", Field("test", alternatives(predicates.map(_.schema("kind")))), port).described("source is the inspected node ID; yes/no/unknown are result node IDs. This choose node returns the selected result. Later steps consume the choose node ID. All branches have the same output kind; kind defaults to document, so specify file for File branches. Only the selected branch runs."),
    variant("stop", "Stop", str("message", "Review required"), port).wired(0, 0).described("No inputs. Fail with message when selected. Set kind to match the branch result (document by default)."),
    variant("keep", "Keep", int("index", 0), ports).wired(1, 4).described("Select the zero-based index from input nodes; kinds describes the input port types."),
    variant("stop-after", "StopAfter", str("message", "Review required"), ports, port).wired(1, 4).described("Demand every input, then fail with message. Set kinds for inputs and kind for the result.")
  )
  private val id = Field("id", identifier match
    case obj: JsonSchema.Object => obj.copy(description = Some("Unique node ID. Every node needs a different ID; reference these IDs in wiring."))
    case other => other)
  private val connections = Field("inputs", JsonSchema.Object(`type` = Some(zio.blocks.schema.json.SchemaType.Single(zio.blocks.schema.json.JsonSchemaType.Array)), items = Some(identifier), maxItems = Some(count(4)),
    description = Some("Ordered node IDs. Never put operation names, settings, JSON strings, or byte data here.")))
  private val branches = BranchFields.map(name => Field(name, identifier))
  private def wiring(op: Variant, named: Boolean): Vector[Field] = if named && op.name == "choose" then branches else Vector(connections)
  private def nodeArray(variants: Vector[Variant]): JsonSchema = array(alternatives(variants.map(v => v.schema("op", id +: wiring(v, true)))), 1, MaxNodes)
  private lazy val recipeFields = Vector(
    Field("version", JsonSchema.enumOf(nonEmpty(Vector(1, 2, 3, 4).map(encoded(_))))), str("name", "Review packet"),
    Field("nodes", nodeArray(operations)), Field("outputs", array(identifier, 1, MaxOutputs)))
  private def signature(entry: StepRegistry.Entry): String =
    def label(kind: Kind): String = kind match
      case Kind.Data(id, version) => s"$id@$version"
      case other => other.toString
    s"${entry.inputs.map(label).mkString("[", ", ", "]")} -> ${label(entry.output)}. ${entry.description}".trim
  private def compactVariant(entry: StepRegistry.Entry, registry: StepRegistry.Registry): Variant =
    val descriptor = entry.descriptor.fields.toMap
    variant(entry.id, "Registered", Field("version", JsonSchema.constOf(encoded(entry.version)), optional = registry.versions(entry.id).size == 1),
      Field("settings", blocksSchema(descriptor("parameters")), optional = entry.check("{}").isEmpty),
      Field("onError", choices("Fail", "RecoverAndFlag"), optional = true)).wired(entry.inputs.size, entry.inputs.size).described(signature(entry))
  private def variantsFor(registry: StepRegistry.Registry): Vector[Variant] =
    val builtins = operations.filterNot(_.tag == "Registered")
    val (reserved, compact) = registry.entries.partition(entry => operations.exists(_.name == entry.id))
    val legacy = Option.when(reserved.nonEmpty)(operations.head.copy(fields = Vector(Field("call", registeredCallSchema(registry))))).toVector
    builtins ++ compact.map(compactVariant(_, registry)) ++ legacy
  private def schemaFor(variants: Vector[Variant], version4: Boolean = false): Json.Object =
    val fields = recipeFields.map { field =>
      if field.name == "nodes" then field.copy(schema = nodeArray(variants))
      else if field.name == "version" && version4 then field.copy(schema = JsonSchema.constOf(encoded(4)))
      else field
    }
    jsonObject(StepSchema.hoist(record(fields).toJson))
  lazy val recipeSchema: Json.Object = recipeSchemaFor(StepRegistry.standard)
  def recipeSchemaFor(registry: StepRegistry.Registry): Json.Object = schemaFor(variantsFor(registry))
  def toolSchema(withFiles: Boolean, registry: StepRegistry.Registry = StepRegistry.standard): Json.Object =
    val fields = Vector(Field("recipe", blocksSchema(recipeSchemaFor(registry)))) ++ Option.when(withFiles)(Field("files",
      JsonSchema.Object(`type` = Some(zio.blocks.schema.json.SchemaType.Single(zio.blocks.schema.json.JsonSchemaType.Object)), additionalProperties = Some(text), maxProperties = Some(count(4))))).toVector
    jsonObject(StepSchema.hoist(record(fields).toJson))

  final case class AuthoringDefinition(schema: DynamicValue, instructions: String, responseFormat: String) derives BlocksSchema
  private lazy val authoringCodec = BlocksSchema[AuthoringDefinition].derive(JsonCodecDeriver.withRejectExtraFields(true))
  private lazy val authoringToonCodec = BlocksSchema[AuthoringDefinition].derive(ToonCodecDeriver.withRejectExtraFields(true))
  private def schemaIssues(error: SchemaError): Vector[Issue] = error.errors.toVector.map { single =>
    val base = single.source.nodes.foldLeft("") {
      case (prefix, DynamicOptic.Node.Field(name)) => if prefix.isEmpty then name else s"$prefix.$name"
      case (prefix, DynamicOptic.Node.AtIndex(index)) => s"$prefix[$index]"
      case (prefix, _) => prefix
    }
    val path = single match
      case SchemaError.MissingField(_, name) => if base.isEmpty then name else s"$base.$name"
      case SchemaError.DuplicatedField(_, name) => if base.isEmpty then name else s"$base.$name"
      case _ => base
    Issue(if path.isEmpty then "recipe" else path, Code.Structure, single.message.take(500))
  }
  final class Authoring private[PdfPipelineModel] (val operations: Set[String], val schema: Json.Object,
      val instructions: String, registry: StepRegistry.Registry):
    private val validator = blocksSchema(schema)
    def definition: AuthoringDefinition = AuthoringDefinition(schema.toDynamicValue, instructions, "json")
    def exported: Json.Object = jsonObject(Json.parseUnsafe(authoringCodec.encodeToString(definition)))
    def exportedToon: String = authoringToonCodec.encodeToString(definition)
    def decodeToon(text: String): Either[Vector[Issue], Checked] = PdfPipelineToon.toJson(text).flatMap(decode)
    def decode(json: String): Either[Vector[Issue], Checked] = PdfPipelineModel.decode(json, registry).flatMap { checked =>
      val issues = checked.recipe.nodes.zipWithIndex.flatMap { (node, index) =>
        val name = node.op match
          case Op.Registered(call) => call.operation
          case other => PdfPipelineModel.operations.find(_.tag == other.productPrefix).get.name
        Option.when(!operations(name))(Issue(s"nodes[$index].op", Code.Setting, "Operation is outside this authoring scope"))
      }
      if issues.nonEmpty then Left(issues)
      else validator.check(Json.parseUnsafe(json)) match
        case Some(error) => Left(schemaIssues(error))
        case _ => Right(checked)
    }
  def authoring(allowed: Set[String], registry: StepRegistry.Registry = StepRegistry.standard): Either[Vector[Issue], Authoring] =
    val known = operations.filterNot(_.tag == "Registered").map(_.name).toSet ++ registry.entries.map(_.id)
    val issues = Option.when(allowed.isEmpty)(Issue("operations", Code.Setting, "Choose at least one operation")).toVector ++
      allowed.toVector.sorted.zipWithIndex.collect { case (name, index) if !known(name) =>
        Issue(s"operations[$index]", Code.Setting, s"Unknown operation: $name")
      }
    if issues.nonEmpty then Left(issues)
    else
      val subset = registry.entries.filter(entry => allowed(entry.id)).foldLeft(StepRegistry.empty)((r, e) => r.add(e).toOption.get)
      val selected = variantsFor(subset).filter(v => allowed(v.name) || v.tag == "Registered" && v.name == "registered")
      val guide = """Return exactly one JSON object matching the recipe schema, without markdown or explanations. Use version 4. Every node needs a distinct id.
inputs contains ordered node IDs, never operation names or encoded settings. Include an empty inputs array on input and stop nodes. Put registered parameters in settings.
Document and File are different types: input binds a Document; write converts it to File. Reuse node IDs to share outputs.
Only list requested final results in outputs. Preserve every requested value. Omit settings when valid defaults suffice.
Use named source/yes/no/unknown wiring for choose. A choose returns the selected branch value; subsequent steps consume its node ID. Document branches can be chosen before a single write. File branches require kind: file. Page-count min/max must be within 1 through 64. Unknown evidence follows unknown, never silently no.
The response format is JSON even when this definition is provided as TOON context. Settings are JSON objects, not encoded strings.
Available operations:
""" + selected.map(v => s"${v.name}: ${v.description}").mkString("\n")
      Right(new Authoring(allowed, schemaFor(selected, version4 = true), guide, registry))

  // These are transport records only. A successful decode always returns the
  // existing checked, typed graph; no executor is reconstructed from data.
  private final case class RawRecipe(version: Int, name: String, nodes: Vector[RawNode], outputs: Vector[String]) derives BlocksSchema
  private final case class RawTest(kind: String, min: Option[Int] = None, max: Option[Int] = None,
      profile: Option[String] = None) derives BlocksSchema
  private final case class RawCall(operation: String, version: Int, settings: DynamicValue,
      inputs: Vector[DynamicValue], output: DynamicValue, onError: StepRegistry.OnError = StepRegistry.OnError.Fail) derives BlocksSchema
  private final case class DataIdentity(schema: String, version: Int) derives BlocksSchema
  private final case class DataKind(Data: DataIdentity) derives BlocksSchema
  private final case class EmptySettings() derives BlocksSchema
  private final case class RawNode(id: String, op: String, inputs: Option[Vector[String]] = None,
      slot: Option[String] = None, pages: Option[Vector[Int]] = None, catalog: Option[Int] = None,
      text: Option[String] = None, placement: Option[String] = None, start: Option[Int] = None,
      style: Option[String] = None, prefix: Option[String] = None, profiles: Option[Vector[String]] = None,
      profile: Option[String] = None, rules: Option[DynamicValue] = None, test: Option[RawTest] = None,
      kind: Option[DynamicValue] = None, kinds: Option[Vector[DynamicValue]] = None, index: Option[Int] = None,
      message: Option[String] = None, source: Option[String] = None, yes: Option[String] = None,
      no: Option[String] = None, unknown: Option[String] = None, call: Option[RawCall] = None,
      settings: Option[DynamicValue] = None, version: Option[Int] = None,
      onError: Option[StepRegistry.OnError] = None) derives BlocksSchema

  private val transportDeriver = JsonCodecDeriver.withRejectExtraFields(true).withTransientEmptyCollection(false)
  private lazy val recipeCodec = BlocksSchema[RawRecipe].derive(transportDeriver)
  private lazy val recipeToonCodec = BlocksSchema[RawRecipe].derive(ToonCodecDeriver.withRejectExtraFields(true).withTransientEmptyCollection(false))
  private lazy val nodeCodec = BlocksSchema[RawNode].derive(transportDeriver)
  private lazy val dataKindCodec = BlocksSchema[DataKind].derive(transportDeriver)
  private val emptySettings = BlocksSchema[EmptySettings].toDynamicValue(EmptySettings())

  private def kindValue(kind: Kind, canonical: Boolean = false): DynamicValue = kind match
    case Kind.Data(id, version) => BlocksSchema[DataKind].toDynamicValue(DataKind(DataIdentity(id, version)))
    case other => BlocksSchema[String].toDynamicValue(if canonical then other.toString else other.toString.toLowerCase(java.util.Locale.ROOT))

  private def rawTest(test: Predicate): RawTest = test match
    case Predicate.PageCount(min, max) => RawTest("page-count", Some(min), Some(max))
    case Predicate.Declares3b => RawTest("declares-3b")
    case Predicate.HasPageNumbers => RawTest("has-page-numbers")
    case Predicate.Validation(profile) => RawTest("validation", profile = Some(profile))
    case Predicate.PolicyPassed => RawTest("policy-passed")

  /** Encode transport records through their derived Blocks codec. */
  def encode(plan: Checked, namedBranches: Boolean = true): String = recipeCodec.encodeToString(transport(plan, namedBranches))
  private[pipeline] def encodeToon(plan: Checked): String = recipeToonCodec.encodeToString(transport(plan, true))
  private[pipeline] def toonToJson(text: String): Either[Vector[Issue], String] =
    PdfPipelineToon.complete(recipeToonCodec).decode(text).left.map(schemaIssues).map(recipeCodec.encodeToString)

  private def transport(plan: Checked, namedBranches: Boolean): RawRecipe =
    val nodes = plan.recipe.nodes.map { node =>
      val base = RawNode(node.id, "", inputs = Some(node.inputs))
      node.op match
        case Op.Input(slot) => base.copy(op = "input", slot = Some(slot))
        case Op.Select(pages) => base.copy(op = "select", pages = Some(pages))
        case Op.Assemble(catalog) => base.copy(op = "assemble", catalog = Option.when(catalog != 0)(catalog))
        case Op.Stamp(text, placement) => base.copy(op = "stamp", text = Some(text), placement = Option.when(placement != "top")(placement))
        case Op.NumberPages(start, style, prefix) => base.copy(op = "number", start = Option.when(start != 1)(start),
          style = Option.when(style != "page")(style), prefix = Option.when(prefix.nonEmpty)(prefix))
        case Op.Write => base.copy(op = "write")
        case Op.ConvertPdfA3b => base.copy(op = "convert-pdfa-3b")
        case Op.Validate(profiles) => base.copy(op = "validate", profiles = Some(profiles))
        case Op.Preflight(profile) => base.copy(op = "preflight", profile = Some(profile))
        case Op.PreflightRules(rules) => base.copy(op = "preflight-rules", rules = Some(Json.parseUnsafe(Policy.encode(rules)).toDynamicValue))
        case Op.Stop(message, kind) => base.copy(op = "stop", message = Some(message), kind = Option.when(kind != Kind.Document)(kindValue(kind)))
        case Op.StopAfter(message, kinds, kind) => base.copy(op = "stop-after", message = Some(message),
          kinds = Some(kinds.map(kindValue(_))), kind = Option.when(kind != Kind.Document)(kindValue(kind)))
        case Op.Keep(index, kinds) => base.copy(op = "keep", index = Some(index), kinds = Some(kinds.map(kindValue(_))))
        case Op.Choose(test, kind) =>
          val choice = base.copy(op = "choose", test = Some(rawTest(test)), kind = Option.when(kind != Kind.Document)(kindValue(kind)))
          if !namedBranches then choice
          else choice.copy(inputs = None, source = node.inputs.lift(0), yes = node.inputs.lift(1), no = node.inputs.lift(2), unknown = node.inputs.lift(3))
        case Op.Registered(call) =>
          val parameters = Json.parseUnsafe(call.settings).toDynamicValue
          if operations.exists(_.name == call.operation) then
            base.copy(op = "registered", call = Some(RawCall(call.operation, call.version, parameters,
              call.inputs.map(kindValue(_, canonical = true)), kindValue(call.output, canonical = true), call.onError)))
          else base.copy(op = call.operation,
            settings = Option.when(parameters != emptySettings)(parameters),
            version = Option.when(plan.registry.versions(call.operation).size != 1)(call.version),
            onError = Option.when(call.onError != StepRegistry.OnError.Fail)(call.onError))
    }
    RawRecipe(plan.recipe.version, plan.recipe.name, nodes, plan.recipe.outputs)

  def decode(json: String): Either[Vector[Issue], Checked] = decode(json, StepRegistry.standard)
  def decode(json: String, registry: StepRegistry.Registry): Either[Vector[Issue], Checked] = decodeText(json, true, registry)
  def decodeValue(value: Json, namedBranches: Boolean = true, registry: StepRegistry.Registry = StepRegistry.standard): Either[Vector[Issue], Checked] =
    decodeText(value.print, namedBranches, registry)

  private def decodeText(json: String, namedBranches: Boolean, registry: StepRegistry.Registry): Either[Vector[Issue], Checked] =
    if json.length > 32 * 1024 then Left(Vector(Issue("recipe", Code.Limit, "Recipe exceeds 32 KiB of JSON characters")))
    else if ReportJson.parse(json).isLeft then Left(Vector(Issue("recipe", Code.Structure, "Expected bounded object JSON with unique fields")))
    else recipeCodec.decode(json).left.map { error =>
      val independent = structuralIssues(Json.parseUnsafe(json))
      if independent.nonEmpty then independent else schemaIssues(error)
    }.flatMap { raw =>
      val errors = Vector.newBuilder[Issue]
      def issue(path: String, code: Code, message: String): Unit = errors += Issue(path, code, message)
      def required[A](value: Option[A], path: String): Option[A] =
        if value.isEmpty then issue(path, Code.Structure, "Missing or invalid field")
        value
      def kind(value: DynamicValue, path: String): Option[Kind] = value match
        case DynamicValue.Primitive(zio.blocks.schema.PrimitiveValue.String(name)) => name.toLowerCase(java.util.Locale.ROOT) match
          case "document" => Some(Kind.Document)
          case "file" => Some(Kind.File)
          case "receipt" => Some(Kind.Receipt)
          case _ => issue(path, Code.Structure, "Unknown port kind"); None
        case _ => dataKindCodec.decode(value.toJson.print) match
          case Right(data) => Some(Kind.Data(data.Data.schema, data.Data.version))
          case Left(_) => issue(path, Code.Structure, "Expected a port kind"); None
      def resultKind(node: RawNode, path: String): Option[Kind] = node.kind match
        case Some(value) => kind(value, s"$path.kind")
        case None => Some(Kind.Document)
      def kinds(node: RawNode, path: String): Option[Vector[Kind]] = required(node.kinds, s"$path.kinds").flatMap { values =>
        val converted = values.zipWithIndex.map((value, index) => kind(value, s"$path.kinds[$index]"))
        Option.when(converted.forall(_.nonEmpty))(converted.flatten)
      }
      def predicate(value: RawTest, path: String): Option[Predicate] = value.kind match
        case "page-count" =>
          val min = required(value.min, s"$path.min")
          val max = required(value.max, s"$path.max")
          for a <- min; b <- max yield Predicate.PageCount(a, b)
        case "declares-3b" => Some(Predicate.Declares3b)
        case "has-page-numbers" => Some(Predicate.HasPageNumbers)
        case "validation" => required(value.profile, s"$path.profile").map(Predicate.Validation.apply)
        case "policy-passed" => Some(Predicate.PolicyPassed)
        case _ => issue(s"$path.kind", Code.Structure, "Unknown predicate"); None
      val compact = scala.collection.mutable.Set.empty[Int]
      val nodes = raw.nodes.zipWithIndex.flatMap { (node, index) =>
        val path = s"nodes[$index]"
        val meta = operations.find(_.name == node.op)
        val registered = meta.isEmpty
        if registered then compact += index
        val allowed = if registered then Set("id", "op", "inputs", "settings", "version", "onError")
          else meta.toVector.flatMap(v => Vector(id) ++ wiring(v, namedBranches) ++ v.fields).map(_.name).toSet + "op"
        val supplied = Json.parseUnsafe(nodeCodec.encodeToString(node)).fields.map(_._1).toSet
        (supplied -- allowed).toVector.sorted.foreach(name => issue(s"$path.$name", Code.Structure, "Unknown field"))
        val refs = if node.op == "choose" && namedBranches then
          val values = Vector(node.source, node.yes, node.no, node.unknown)
          values.zip(BranchFields).foreach((value, name) => required(value, s"$path.$name"))
          Option.when(values.forall(_.nonEmpty))(values.flatten)
        else required(node.inputs, s"$path.inputs")
        val operation: Option[Op] = node.op match
          case "input" => required(node.slot, s"$path.slot").map(Op.Input.apply)
          case "select" => required(node.pages, s"$path.pages").map(Op.Select.apply)
          case "assemble" => Some(Op.Assemble(node.catalog.getOrElse(0)))
          case "stamp" => required(node.text, s"$path.text").map(text => Op.Stamp(text, node.placement.getOrElse("top")))
          case "number" => Some(Op.NumberPages(node.start.getOrElse(1), node.style.getOrElse("page"), node.prefix.getOrElse("")))
          case "write" => Some(Op.Write)
          case "convert-pdfa-3b" => Some(Op.ConvertPdfA3b)
          case "validate" => required(node.profiles, s"$path.profiles").map(Op.Validate.apply)
          case "preflight" => required(node.profile, s"$path.profile").map(Op.Preflight.apply)
          case "preflight-rules" => required(node.rules, s"$path.rules").flatMap { rules =>
            Policy.decode(rules.toJson.print) match
              case Right(checked) => Some(Op.PreflightRules(checked.profile))
              case Left(issues) => issues.foreach(i => issue(s"$path.rules.${i.path.stripPrefix("profile.")}", i.code, i.message)); None
          }
          case "choose" =>
            val test = required(node.test, s"$path.test").flatMap(predicate(_, s"$path.test"))
            val output = resultKind(node, path)
            for t <- test; k <- output yield Op.Choose(t, k)
          case "stop" =>
            val message = required(node.message, s"$path.message")
            val output = resultKind(node, path)
            for m <- message; k <- output yield Op.Stop(m, k)
          case "keep" =>
            val position = required(node.index, s"$path.index")
            val ports = kinds(node, path)
            for i <- position; k <- ports yield Op.Keep(i, k)
          case "stop-after" =>
            val message = required(node.message, s"$path.message")
            val ports = kinds(node, path)
            val output = resultKind(node, path)
            for m <- message; k <- ports; o <- output yield Op.StopAfter(m, k, o)
          case "registered" => required(node.call, s"$path.call").flatMap { call =>
            val ports = call.inputs.zipWithIndex.map((value, port) => kind(value, s"$path.call.inputs[$port]"))
            val output = kind(call.output, s"$path.call.output")
            Option.when(ports.forall(_.nonEmpty))(ports.flatten).flatMap { inputs =>
              output.map(out => Op.Registered(StepRegistry.Call(call.operation, call.version, call.settings.toJson.print, inputs, out, call.onError)))
            }
          }
          case name =>
            val candidates = registry.versions(name)
            val entry = node.version match
              case None if candidates.size == 1 => candidates.headOption
              case None if candidates.nonEmpty => issue(s"$path.version", Code.Setting, "Multiple versions are registered; choose an explicit version"); None
              case Some(version) if candidates.nonEmpty =>
                val found = registry.find(name, version)
                if found.isEmpty then issue(s"$path.version", Code.Setting, "Unknown registered operation/version")
                found
              case _ => None
            if candidates.isEmpty then issue(s"$path.op", Code.Setting, "Unknown registered operation")
            entry.flatMap { definition =>
              val parameters = node.settings.getOrElse(emptySettings)
              parameters match
                case _: DynamicValue.Record => definition.resolve(parameters, node.onError.getOrElse(StepRegistry.OnError.Fail)) match
                  case Right(call) => Some(Op.Registered(call))
                  case Left(error) => issue(s"$path.settings", Code.Setting, error.message); None
                case _ => issue(s"$path.settings", Code.Structure, "Expected parameter object"); None
            }
        for op <- operation; inputs <- refs yield Node(node.id, op, inputs)
      }
      val failures = errors.result()
      if failures.nonEmpty then Left(failures)
      else
        val Reference = """nodes\[(\d+)\]\.inputs\[([0-3])\]""".r
        val RegisteredField = """nodes\[(\d+)\]\.call\.(.+)""".r
        PdfPipeline.check(Recipe(raw.version, raw.name, nodes, raw.outputs), registry).left.map(_.map { error => error.path match
          case Reference(node, port) if namedBranches && raw.nodes(node.toInt).op == "choose" => error.copy(path = s"nodes[$node].${BranchFields(port.toInt)}")
          case RegisteredField(node, field) if compact(node.toInt) => error.copy(path = s"nodes[$node].$field")
          case _ => error
        })
    }

  /** On a structural codec failure, inspect independent supplied fields using
    * the same schema. The valid path still performs one derived codec decode.
    */
  private def structuralIssues(value: Json): Vector[Issue] =
    def walk(shape: Reflect[Binding, ?], value: Json, path: String): Vector[Issue] =
      def nested(name: String): String = if path.isEmpty then name else s"$path.$name"
      if shape.isOption && value != Json.Null then walk(shape.optionInnerType.get, value, path)
      else (shape.asRecord, shape.asSequenceUnknown, value) match
        case (Some(record), _, obj: Json.Object) =>
          val fields = record.fields.map(field => field.name -> field.value).toMap
          obj.fields.toVector.flatMap { (name, child) => fields.get(name) match
            case Some(field) => walk(field, child, nested(name))
            case None => Vector(Issue(nested(name), Code.Structure, "Unknown field"))
          }
        case (_, Some(sequence), array: Json.Array) =>
          array.elements.zipWithIndex.toVector.flatMap((child, index) => walk(sequence.sequence.element, child, s"$path[$index]"))
        case _ => BlocksSchema(shape).derive(transportDeriver).decode(value.print).left.toOption.toVector.flatMap { error =>
          schemaIssues(error).map(issue => issue.copy(path = if path.isEmpty then "recipe" else path))
        }
    walk(BlocksSchema[RawRecipe].reflect, value, "")
