package io.github.adrielc.kyopdf.pipeline

import zio.blocks.schema.Schema
import zio.blocks.schema.json.{Json, JsonCodecDeriver}

/** JSON transport for executable recipes. Decoding always rechecks the graph before it can run. */
object PdfPipelineJson:
  import PdfPipeline.{Checked, Code, Issue, Recipe}

  private val MaxJsonChars = 128 * 1024
  private lazy val codec = Schema.derived[Recipe].derive(JsonCodecDeriver.withRejectExtraFields(true))

  def encode[B](name: String, arrow: PdfArrow[Unit, B]): Either[Vector[Issue], String] =
    PdfArrow.recipe(name, arrow).map(recipe => normalizeKinds(Json.parseUnsafe(codec.encodeToString(recipe)), false).print)

  def encode(plan: Checked): String = normalizeKinds(Json.parseUnsafe(codec.encodeToString(plan.recipe)), false).print

  def decode(json: String): Either[Vector[Issue], Checked] = decode(json, StepRegistry.standard)
  def decode(json: String, registry: StepRegistry.Registry): Either[Vector[Issue], Checked] =
    if json.length > MaxJsonChars then
      Left(Vector(Issue("json", Code.Limit, s"Use at most $MaxJsonChars JSON characters")))
    else if ReportJson.parse(json).isLeft then
      Left(Vector(Issue("json", Code.Structure, "Expected bounded object JSON with unique field names")))
    else
      Json.parse(json)
        .flatMap(value => codec.decode(normalizeKinds(normalizePolicies(value), true).print))
        .left.map(error => Vector(Issue("json", Code.Structure, error.toString)))
        .flatMap(PdfPipeline.check(_, registry))

  private def normalizePolicies(value: Json): Json =
    def mapField(value: Json, key: String)(transform: Json => Json): Json = value match
      case obj: Json.Object => obj.copy(value = obj.value.map { (name, field) =>
        name -> (if name == key then transform(field) else field)
      })
      case other => other
    mapField(value, "nodes") {
      case array: Json.Array => array.copy(value = array.value.map { node =>
        mapField(node, "op") { op =>
          mapField(op, "PreflightRules") { settings =>
            mapField(settings, "rules")(Policy.normalizeProfile)
          }
        }
      })
      case other => other
    }

  /** Keep v1–v3 scalar port strings stable despite the mixed data-port enum. */
  private def normalizeKinds(value: Json, decoding: Boolean): Json =
    import Json.{Object as Obj, Array as Arr, String as Str}
    def kind(value: Json): Json = value match
      case Str(name) if decoding && Set("Document", "File", "Receipt")(name) => Obj(name -> Obj())
      case Obj(fields) if !decoding && fields.size == 1 && Set("Document", "File", "Receipt")(fields.head._1) => Str(fields.head._1)
      case other => other
    def kinds(value: Json): Json = value match
      case Arr(values) => Arr(values.map(kind).toSeq*)
      case other => kind(other)
    value match
      case obj: Obj => Obj(obj.value.toVector.map { (key, field) =>
        key -> (if key == "kind" || key == "kinds" then kinds(field)
          else if key == "call" then field match
            case call: Obj => Obj(call.value.toVector.map { (name, data) =>
              name -> (if name == "inputs" || name == "output" then kinds(data) else data)
            }*)
            case other => other
          else normalizeKinds(field, decoding))
      }*)
      case Arr(values) => Arr(values.map(normalizeKinds(_, decoding)).toSeq*)
      case other => other
