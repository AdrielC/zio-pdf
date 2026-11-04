package io.github.adrielc.kyopdf.pipeline

import zio.blocks.schema.Schema
import zio.blocks.schema.json.{Json, JsonFormat}

/** JSON transport for executable recipes. Decoding always rechecks the graph before it can run. */
object PdfPipelineJson:
  import PdfPipeline.{Checked, Code, Issue, Recipe}

  private val MaxJsonChars = 128 * 1024
  private lazy val codec = Schema.derived[Recipe].derive(JsonFormat.deriver)

  def encode[B](name: String, arrow: PdfArrow[Unit, B]): Either[Vector[Issue], String] =
    PdfArrow.recipe(name, arrow).map(codec.encodeToString(_))

  def encode(plan: Checked): String = codec.encodeToString(plan.recipe)

  def decode(json: String): Either[Vector[Issue], Checked] =
    if json.length > MaxJsonChars then
      Left(Vector(Issue("json", Code.Limit, s"Use at most $MaxJsonChars JSON characters")))
    else
      Json.parse(json)
        .flatMap(value => codec.decode(normalizePolicies(value).print))
        .left.map(error => Vector(Issue("json", Code.Structure, error.toString)))
        .flatMap(PdfPipeline.check)

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
