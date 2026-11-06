package io.github.adrielc.kyopdf.pipeline

import zio.test.*
import zio.blocks.schema.{Schema as BlocksSchema}
import zio.blocks.schema.json.{Json, JsonSchema, JsonCodecDeriver}
import zio.blocks.chunk.ChunkMap
import zio.blocks.schema.toon.{ToonCodecDeriver, ToonFormat, ArrayFormat}
import PdfArrow.*

object AuthoringSpec extends ZIOSpecDefault:
  private val operations = Set("input", "write", "pdf.select-content", "pdf.plan-redaction", "pdf.redact", "pdf.ocr")
  private def checked[A](arrow: PdfArrow[Unit, A]) = PdfArrow.recipe("Synthetic authoring", arrow).flatMap(PdfPipeline.check)
    .fold(issues => throw AssertionError(issues.toString), identity)
  private def packet = checked(input("body") >>> write >>> PdfSteps.redactArrow(
    PdfSteps.Select(Vector(PdfSteps.Region(1, 18, 18, 220, 47)))) >>> registered(PdfSteps.ocr, PdfSteps.Ocr()))

  def spec = suite("Schema-driven authoring and TOON")(
    test("automatic TOON derivation handles optional records, arrays and quoted colon strings") {
      final case class Child(label: String) derives BlocksSchema
      final case class Envelope(child: Option[Child], values: Option[Vector[String]], rows: Option[Vector[Child]]) derives BlocksSchema
      val codec = BlocksSchema[Envelope].derive(ToonCodecDeriver.withTransientEmptyCollection(false).withArrayFormat(ArrayFormat.Tabular))
      val examples = Vector(Envelope(Some(Child("https://example.org/policy")), Some(Vector("a", "b")), Some(Vector(Child("one")))),
        Envelope(None, Some(Vector.empty), None))
      val dynamic = BlocksSchema[String].toDynamicValue("https://example.org/a:b")
      val dynamicCodec = BlocksSchema[zio.blocks.schema.DynamicValue].derive(ToonFormat)
      assertTrue(examples.forall(value => codec.decode(codec.encodeToString(value)) == Right(value)),
        dynamicCodec.decode(dynamicCodec.encodeToString(dynamic)) == Right(dynamic))
    },
    test("scoped schema, guide and decoder share the registry and exact connection counts") {
      val profile = PdfPipelineModel.authoring(operations).toOption.get
      val json = PdfPipelineModel.encode(packet)
      val schema = JsonSchema.fromJson(profile.schema).toOption.get
      val definitionCodec = BlocksSchema[PdfPipelineModel.AuthoringDefinition].derive(ToonFormat)
      val wrong = json.replace("\"inputs\":[\"pdf\",\"pdf-plan-redaction\"]", "\"inputs\":[\"pdf\"]")
      assertTrue(profile.decode(json).isRight, schema.check(Json.parseUnsafe(json)).isEmpty,
        profile.instructions.contains("[File, pdf.redaction-plan@1] -> File"),
        profile.definition.responseFormat == "json", profile.instructions.startsWith("Return exactly one JSON object"),
        !profile.exported.print.contains("pdf.replace-text"), profile.exportedToon.startsWith("schema:\n"),
        definitionCodec.decode(profile.exportedToon) == Right(profile.definition),
        profile.decode(wrong).isLeft, schema.check(Json.parseUnsafe(wrong)).nonEmpty)
    },
    test("unknown scope choices accumulate and excluded operations cannot bypass scope checks") {
      val invalid = PdfPipelineModel.authoring(Set("missing-a", "missing-b")).left.toOption.get
      val profile = PdfPipelineModel.authoring(Set("input", "write")).toOption.get
      val outside = profile.decode(PdfPipelineModel.encode(packet)).left.toOption.get
      assertTrue(invalid.map(_.path) == Vector("operations[0]", "operations[1]"),
        PdfPipelineModel.authoring(Set.empty).isLeft, outside.size == 4,
        outside.forall(_.message.contains("outside this authoring scope")))
    },
    test("one recipe schema derives both JSON and TOON and preserves the checked redaction graph") {
      val plan = packet
      val toon = PdfPipelineToon.encode(plan).fold(issues => throw AssertionError(issues.toString), identity)
      val profile = PdfPipelineModel.authoring(operations).toOption.get
      assertTrue(PdfPipelineToon.decode(toon).toOption.get.recipe == plan.recipe,
        profile.decodeToon(toon).toOption.get.recipe == plan.recipe,
        toon.contains("pdf.redact"), !toon.contains("\"call\""))
    },
    test("named conditional branches and policy parameters survive both derived codecs") {
      val choice = choose(pages(2, 64))(stamp("REVIEW"), id[Document], stop[Document, Document]("Review required"))
      val conditional = checked(input("body") >>> choice >>> write)
      val policy = checked(input("body") >>> write >>> preflight(CourtProfile.CaliforniaSecondDistrictPetition.rules))
      val results = Vector(conditional, policy).map { plan =>
        val toon = PdfPipelineToon.encode(plan).fold(issues => throw AssertionError(issues.toString + "\n" + PdfPipelineModel.encodeToon(plan)), identity)
        PdfPipelineToon.decode(toon).toOption.get.recipe == plan.recipe &&
          PdfPipelineModel.decode(PdfPipelineModel.encode(plan)).toOption.get.recipe == plan.recipe
      }
      assertTrue(results.forall(identity))
    },
    test("TOON rejects huge declared arrays, deep nesting, duplicate fields, trailing data and precision loss") {
      val huge = PdfPipelineToon.decode("nodes[ 999999999 ]:")
      val deep = PdfPipelineToon.decode(" " * 130 + "value: 1")
      val valid = PdfPipelineModel.encodeToon(packet)
      val duplicate = PdfPipelineToon.decode("version: 4\n" + valid)
      val trailing = PdfPipelineToon.decode("version: 4\nname: X\nnodes[0]:\noutputs[0]:\n  unexpected: value")
      val precision = PdfPipelineToon.decode("value: 0.12345678901234567890123456789")
      assertTrue(huge.left.toOption.get.exists(_.code == PdfPipeline.Code.Limit), deep.isLeft,
        duplicate.left.toOption.exists(_.exists(_.message.toLowerCase.contains("duplicat"))), trailing.isLeft, precision.isLeft)
    },
    test("DynamicSchema checks constraints after typed codec decoding") {
      final case class Value(n: Int) derives BlocksSchema
      val shape = JsonSchema.obj(properties = Some(ChunkMap("n" -> JsonSchema.integer(minimum = Some(BigDecimal(1))))),
        required = Some(Set("n")), additionalProperties = Some(JsonSchema.False))
      val data = new StepRegistry.DataSchema("test.positive", 1, BlocksSchema.fromJsonSchema(shape))
      val codec = BlocksSchema[Value].derive(JsonCodecDeriver)
      assertTrue(data.decode(codec.encodeToString(Value(1))).isRight, data.decode(codec.encodeToString(Value(0))).isLeft)
    }
  )
