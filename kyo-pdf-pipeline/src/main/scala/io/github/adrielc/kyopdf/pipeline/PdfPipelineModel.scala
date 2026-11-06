package io.github.adrielc.kyopdf.pipeline

import PdfPipeline.*
import zio.blocks.schema.json.Json
import zio.blocks.schema.json.Json.{Array as Arr, Object as Obj, String as Str, Number as Num, Boolean as Bool}

/** Model-facing recipe format. The registry supplies the schema, decoder and field examples. */
object PdfPipelineModel:
  val BranchFields = Vector("source", "yes", "no", "unknown")

  private val text = Obj("type" -> Str("string"))
  private val integer = Obj("type" -> Str("integer"))
  private def strings(values: Iterable[String]): Json = Arr(values.iterator.map(Str.apply).toSeq*)
  private def array(items: Json): Obj = Obj("type" -> Str("array"), "items" -> items)
  private def choices(values: String*): Obj = Obj("enum" -> strings(values))
  private val kinds = Obj("oneOf" -> Arr(choices("document", "file", "receipt"),
    Obj("type" -> Str("object"), "required" -> Arr(Str("Data")), "properties" -> Obj("Data" -> Obj("type" -> Str("object"))))))
  private val profiles = choices(Profiles.toVector.sorted*)
  private val inputs = Obj("type" -> Str("array"), "items" -> text, "maxItems" -> Num(4))

  private case class Field(name: String, schema: Obj, example: Json, optional: Boolean = false):
    def documented: Obj = Obj((schema.value.toVector :+ ("examples" -> Arr(example)))*)

  private case class Variant(name: String, tag: String, fields: Vector[Field]):
    def schema(discriminator: String, common: Vector[Field] = Vector.empty): Obj =
      val all = common ++ fields
      shape(all, discriminator -> Obj("const" -> Str(name)))

  private def shape(fields: Vector[Field], discriminator: (String, Json)): Obj =
    Obj(
      "type" -> Str("object"),
      "additionalProperties" -> Bool(false),
      "required" -> strings(discriminator._1 +: fields.filterNot(_.optional).map(_.name)),
      "properties" -> Obj((discriminator +: fields.map(field => field.name -> field.documented))*)
    )

  private def variant(name: String, tag: String, fields: Field*): Variant = Variant(name, tag, fields.toVector)
  private def str(name: String, example: String, optional: Boolean = false): Field = Field(name, text, Str(example), optional)
  private def int(name: String, example: Int, optional: Boolean = false): Field = Field(name, integer, Num(example), optional)
  private val port = Field("kind", kinds, Str("document"), true)
  private val ports = Field("kinds", array(kinds), strings(Vector("document")))
  private val predicates = Vector(
    variant("policy-passed", "PolicyPassed"),
    variant("page-count", "PageCount", int("min", 1), int("max", 3)),
    variant("declares-3b", "Declares3b"),
    variant("validation", "Validation", Field("profile", profiles, Str("3b"))),
    variant("has-page-numbers", "HasPageNumbers")
  )
  private def canonicalKind(kind: Kind): Json = kind match
    case Kind.Data(schema, version) => Obj("Data" -> Obj("schema" -> Str(schema), "version" -> Num(version)))
    case other => Str(other.toString)
  def registeredSchema(registry: StepRegistry.Registry = StepRegistry.standard): Obj =
    Obj("oneOf" -> Arr(registry.entries.map { entry =>
      val descriptor = entry.descriptor.asInstanceOf[Obj].value.toMap
      Obj("type" -> Str("object"), "additionalProperties" -> Bool(false),
        "required" -> strings(Vector("operation", "version", "settings", "inputs", "output")),
        "properties" -> Obj(
          "operation" -> Obj("const" -> Str(entry.id)), "version" -> Obj("const" -> Num(entry.version)),
          "settings" -> descriptor("parameters"),
          "inputs" -> Obj("const" -> Arr(entry.inputs.map(canonicalKind)*)),
          "output" -> Obj("const" -> canonicalKind(entry.output)),
          "onError" -> choices("Fail", "RecoverAndFlag")))
    }*))
  private lazy val operations = Vector(
    variant("registered", "Registered", Field("call", registeredSchema(),
      Obj("operation" -> Str("pdf.cleanup"), "version" -> Num(1), "settings" -> Obj("dpi" -> Num(144)),
        "inputs" -> Arr(Str("File")), "output" -> Str("File")))),
    variant("input", "Input", str("slot", "body")),
    variant("select", "Select", Field("pages", array(integer), Arr(Num(1)))),
    variant("assemble", "Assemble", int("catalog", 0, true)),
    variant("stamp", "Stamp", str("text", "REVIEW"), Field("placement", choices("top", "center"), Str("top"), true)),
    variant("number", "NumberPages", int("start", 1, true), Field("style", choices("page", "number", "bates"), Str("page"), true), str("prefix", "", true)),
    variant("write", "Write"),
    variant("convert-pdfa-3b", "ConvertPdfA3b"),
    variant("preflight-rules", "PreflightRules", Field("rules", Policy.jsonSchema.asInstanceOf[Obj],
      Json.parseUnsafe(Policy.encode(CourtProfile.CaliforniaSecondDistrictPetition.rules)))),
    variant("preflight", "Preflight", Field("profile", choices(CourtProfile.CaliforniaSecondDistrictPetition.id), Str(CourtProfile.CaliforniaSecondDistrictPetition.id))),
    variant("validate", "Validate", Field("profiles", array(profiles), strings(Vector("3b")))),
    variant("choose", "Choose", Field("test", Obj("oneOf" -> Arr(predicates.map(_.schema("kind"))*)), Obj("kind" -> Str("has-page-numbers"))), port),
    variant("stop", "Stop", str("message", "Review required"), port),
    variant("keep", "Keep", int("index", 0), ports),
    variant("stop-after", "StopAfter", str("message", "Review required"), ports, port)
  )
  private val id = str("id", "node")
  private val connections = Field("inputs", inputs, Arr(Str("body")))
  private val branches = BranchFields.map(name => str(name, if name == "source" then "body" else name))
  private def wiring(op: Variant, named: Boolean): Vector[Field] =
    if named && op.name == "choose" then branches else Vector(connections)

  private lazy val recipeFields = Vector(
    Field("version", Obj("enum" -> Arr(Num(1), Num(2), Num(3), Num(4))), Num(2)),
    str("name", "Review packet"),
    Field("nodes", Obj("type" -> Str("array"), "minItems" -> Num(1), "maxItems" -> Num(MaxNodes),
      "items" -> Obj("oneOf" -> Arr(operations.map(op => op.schema("op", id +: wiring(op, true)))*))), Arr()),
    Field("outputs", Obj("type" -> Str("array"), "items" -> text, "minItems" -> Num(1), "maxItems" -> Num(MaxOutputs)), Arr(Str("pdf")))
  )

  lazy val recipeSchema: Obj = recipeSchemaFor(StepRegistry.standard)

  def recipeSchemaFor(registry: StepRegistry.Registry): Obj =
    val variants = operations.map { op =>
      if op.tag == "Registered" then op.copy(fields = op.fields.map(f => f.copy(schema = registeredSchema(registry)))) else op
    }
    val fields = recipeFields.map { field =>
      if field.name == "nodes" then field.copy(schema = Obj("type" -> Str("array"), "minItems" -> Num(1), "maxItems" -> Num(MaxNodes),
        "items" -> Obj("oneOf" -> Arr(variants.map(op => op.schema("op", id +: wiring(op, true)))*)))) else field
    }
    StepSchema.hoist(Obj("type" -> Str("object"), "additionalProperties" -> Bool(false),
      "required" -> strings(fields.map(_.name)), "properties" -> Obj(fields.map(f => f.name -> f.documented)*))).asInstanceOf[Obj]

  def toolSchema(withFiles: Boolean, registry: StepRegistry.Registry = StepRegistry.standard): Obj =
    val properties = Vector("recipe" -> recipeSchemaFor(registry)) ++ Option.when(withFiles)(
      "files" -> Obj("type" -> Str("object"), "maxProperties" -> Num(4), "additionalProperties" -> text))
    StepSchema.hoist(Obj("type" -> Str("object"), "additionalProperties" -> Bool(false),
      "required" -> strings(properties.map(_._1)), "properties" -> Obj(properties*))).asInstanceOf[Obj]

  /** Render a checked plan in the same format accepted by model-facing tools. */
  def encode(plan: Checked, namedBranches: Boolean = true): String =
    def lowerKind(value: Json): Json = value match
      case Str(name) => Str(name.toLowerCase(java.util.Locale.ROOT))
      case Arr(values) => Arr(values.iterator.map(lowerKind).toSeq*)
      case other => other

    val root = Json.parseUnsafe(PdfPipelineJson.encode(plan)).asInstanceOf[Obj].value.toMap
    val canonicalNodes = root("nodes").asInstanceOf[Arr].value
    val nodes = canonicalNodes.iterator.map { node =>
      val fields = node.asInstanceOf[Obj].value.toMap
      val (tag, rawSettings) = fields("op").asInstanceOf[Obj].value.head
      val op = operations.find(_.tag == tag).get
      val opSettings = rawSettings.asInstanceOf[Obj].value.iterator.map {
        case ("call", obj: Obj) =>
          val fields = obj.value.toMap
          "call" -> Obj((obj.value.toVector.map {
            case ("settings", Str(json)) => "settings" -> Json.parseUnsafe(json)
            case pair => pair
          } ++ Option.when(!fields.contains("inputs"))("inputs" -> Arr()).toVector)*)
        case ("test", Obj(tests)) =>
          val (testTag, testSettings) = tests.head
          val test = predicates.find(_.tag == testTag).get
          "test" -> Obj((("kind" -> Str(test.name)) +: testSettings.asInstanceOf[Obj].value.toVector)*)
        case (key @ ("kind" | "kinds"), value) => key -> lowerKind(value)
        case pair => pair
      }.toVector
      val refs = fields.getOrElse("inputs", Arr())
      val connections = if namedBranches && op.name == "choose" then BranchFields.zip(refs.asInstanceOf[Arr].value.toVector)
        else Vector("inputs" -> refs)
      Obj((Vector("id" -> fields("id"), "op" -> Str(op.name)) ++ connections ++ opSettings)*)
    }.toVector
    Obj((root.updated("nodes", Arr(nodes*)).toVector)*).print

  def decode(json: String): Either[Vector[Issue], Checked] = decode(json, StepRegistry.standard)
  def decode(json: String, registry: StepRegistry.Registry): Either[Vector[Issue], Checked] =
    if json.length > 32 * 1024 then Left(Vector(Issue("recipe", Code.Limit, "Recipe exceeds 32 KiB of JSON characters")))
    else Json.parse(json).left.map(_ => Vector(Issue("recipe", Code.Structure, "Expected valid JSON"))).flatMap(decodeValue(_, registry = registry))

  def decodeValue(value: Json, namedBranches: Boolean = true, registry: StepRegistry.Registry = StepRegistry.standard): Either[Vector[Issue], Checked] =
    val errors = Vector.newBuilder[Issue]
    def issue(path: String, message: String): Unit = errors += Issue(path, Code.Structure, message)
    def objectFields(value: Json, path: String): Map[String, Json] = value match
      case Obj(values) =>
        if values.map(_._1).toSet.size != values.size then issue(path, "Duplicate field")
        values.toMap
      case _ => issue(path, "Expected an object"); Map.empty
    def lookup(obj: Map[String, Json], key: String, path: String, variants: Vector[Variant]): Option[Variant] =
      val found = variants.find(v => obj.get(key).contains(Str(v.name)))
      if found.isEmpty then issue(s"$path.$key", s"Choose ${variants.map(_.name).mkString(", ")}")
      found
    def conforms(value: Json, schema: Obj): Boolean =
      val fields = schema.value.toMap
      fields.get("enum") match
        case Some(Arr(allowed)) => allowed.contains(value)
        case _ => fields.get("type") match
          case Some(Str("string")) => value.isInstanceOf[Str]
          case Some(Str("integer")) => value match
            case Num(n) => n.isValidInt
            case _ => false
          case Some(Str("array")) => value match
            case Arr(items) => fields.get("items").forall {
              case obj: Obj if !obj.value.toMap.contains("oneOf") => items.forall(conforms(_, obj))
              case _ => true
            }
            case _ => false
          case _ => true
    def fields(obj: Map[String, Json], allowed: Vector[Field], path: String, extra: Set[String] = Set.empty): Unit =
      val unknown = obj.keySet -- allowed.map(_.name) -- extra
      unknown.toVector.sorted.foreach(name => issue(s"$path.$name", "Unknown field"))
      allowed.foreach { field =>
        obj.get(field.name) match
          case Some(Arr(values)) if field.schema.value.toMap.get("type").contains(Str("array")) =>
            field.schema.value.toMap.get("items") match
              case Some(item: Obj) => values.iterator.zipWithIndex.foreach { (value, index) =>
                if !conforms(value, item) then issue(s"$path.${field.name}[$index]", "Invalid array item")
              }
              case _ => ()
          case Some(value) if conforms(value, field.schema) => ()
          case None if field.optional => ()
          case _ => issue(s"$path.${field.name}", "Missing or invalid field")
      }
    def settings(obj: Map[String, Json], variant: Variant): Vector[(String, Json)] =
      variant.fields.flatMap(field => obj.get(field.name).map { value =>
        def kind(value: Json): Json = value match
          case Str(name) => Str(name.capitalize)
          case other => other
        field.name -> (field.name match
          case "call" => value match
            case call: Obj => Obj(call.value.toVector.map {
              case ("settings", parameters) => "settings" -> Str(parameters.print)
              case pair => pair
            }*)
            case other => other
          case "rules" => Policy.normalizeProfile(value)
          case "kind" => kind(value)
          case "kinds" => value match
            case Arr(values) => Arr(values.iterator.map(kind).toSeq*)
            case other => other
          case _ => value)
      })
    val root = objectFields(value, "recipe")
    fields(root, recipeFields, "recipe")
    // Registry parameters are objects in model JSON; strict codecs own their fields.
    ReportJson.parse(value.print).left.foreach(_ => issue("recipe", "Expected bounded, duplicate-free graph data"))
    val nodes = root.get("nodes") match
      case Some(Arr(values)) => values.iterator.zipWithIndex.flatMap { (raw, index) =>
        val path = s"nodes[$index]"
        val obj = objectFields(raw, path)
        lookup(obj, "op", path, operations).map { op =>
          fields(obj, Vector(id) ++ wiring(op, namedBranches) ++ op.fields, path, Set("op"))
          if op.name == "registered" then
            val call = objectFields(obj.getOrElse("call", Json.Null), s"$path.call")
            val allowed = Set("operation", "version", "settings", "inputs", "output", "onError")
            (call.keySet -- allowed).toVector.sorted.foreach(key => issue(s"$path.call.$key", "Unknown field"))
            if !call.get("settings").exists(_.isInstanceOf[Obj]) then issue(s"$path.call.settings", "Expected parameter object")
          val opSettings = if op.name != "choose" then settings(obj, op) else
            val testPath = s"$path.test"
            val test = objectFields(obj.getOrElse("test", Json.Null), testPath)
            val converted = lookup(test, "kind", testPath, predicates).map { predicate =>
              fields(test, predicate.fields, testPath, Set("kind"))
              Obj(predicate.tag -> Obj(settings(test, predicate)*))
            }.getOrElse(Json.Null)
            settings(obj, op).filterNot(_._1 == "test") :+ ("test" -> converted)
          val refs = if namedBranches && op.name == "choose" then Arr(BranchFields.map(key => obj.getOrElse(key, Json.Null))*)
            else obj.getOrElse("inputs", Arr())
          Obj("id" -> obj.getOrElse("id", Json.Null), "op" -> Obj(op.tag -> Obj(opSettings*)), "inputs" -> refs)
        }
      }.toVector
      case _ => Vector.empty
    val failures = errors.result()
    val Reference = """nodes\[(\d+)\]\.inputs\[([0-3])\]""".r
    if failures.nonEmpty then Left(failures)
    else
      val canonical = Obj((root.updated("nodes", Arr(nodes*)).toVector)*)
      PdfPipelineJson.decode(canonical.print, registry).left.map(issues => if !namedBranches then issues else issues.map { issue =>
        issue.path match
          case Reference(node, port) if nodes(node.toInt).value.exists { case ("op", Obj(settings)) => settings.exists(_._1 == "Choose"); case _ => false } =>
            issue.copy(path = s"nodes[$node].${BranchFields(port.toInt)}")
          case _ => issue
      })
