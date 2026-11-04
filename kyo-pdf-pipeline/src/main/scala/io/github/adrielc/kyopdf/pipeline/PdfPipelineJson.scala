package io.github.adrielc.kyopdf.pipeline

import zio.blocks.schema.Schema
import zio.blocks.schema.json.JsonFormat

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
      codec.decode(json)
        .left.map(error => Vector(Issue("json", Code.Structure, error.toString)))
        .flatMap(PdfPipeline.check)
