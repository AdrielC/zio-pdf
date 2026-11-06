package io.github.adrielc.kyopdf.pipeline

import zio.test.*
import zio.blocks.schema.json.{Json, JsonSchema}
import PdfPipeline.{Code, Kind, Op}

object CompactPipelineModelSpec extends ZIOSpecDefault:
  private val packet = """{"version":4,"name":"Redact and OCR","nodes":[
    {"id":"body","op":"input","slot":"body","inputs":[]},
    {"id":"pdf","op":"write","inputs":["body"]},
    {"id":"select","op":"pdf.select-content","inputs":["pdf"],"settings":{"regions":[{"page":1,"x":18,"y":18,"width":220,"height":47}]}},
    {"id":"plan","op":"pdf.plan-redaction","inputs":["select"]},
    {"id":"redact","op":"pdf.redact","inputs":["pdf","plan"]},
    {"id":"ocr","op":"pdf.ocr","inputs":["redact"]}],"outputs":["ocr"]}"""
  private def single(operation: String, extra: String = "", inputs: String = "[\"pdf\"]"): String =
    s"""{"version":4,"name":"Registry inference","nodes":[
      {"id":"body","op":"input","slot":"body","inputs":[]},
      {"id":"pdf","op":"write","inputs":["body"]},
      {"id":"step","op":"$operation","inputs":$inputs$extra}],"outputs":["step"]}"""
  private def versioned(version: Int, id: String = "test.versioned") = new StepRegistry.Step(id, version,
    StepRegistry.Port.file, StepRegistry.Port.file, PdfSteps.empty, Vector(PdfSteps.Empty()),
    StepRegistry.Access.Document, Set(StepRegistry.Capability.Inspect))((file, _, _) => file)

  def spec = suite("Compact model recipes")(
    test("redaction and OCR infer typed ports, pass the exported tool schema, and persist exact calls") {
      val checked = PdfPipelineModel.decode(packet).toOption.get
      val compact = PdfPipelineModel.encode(checked)
      val canonical = PdfPipelineJson.encode(checked)
      val calls = checked.recipe.nodes.collect { case PdfPipeline.Node(_, Op.Registered(call), _) => call }
      val arguments = Json.Object("recipe" -> Json.parseUnsafe(compact), "files" -> Json.Object("body" -> Json.String("fixture")))
      val schema = JsonSchema.fromJson(PdfPipelineModel.toolSchema(withFiles = true)).toOption.get
      assertTrue(calls.map(_.version) == Vector(1, 1, 1, 1),
        calls(2).inputs == Vector(Kind.File, PdfSteps.redactionPlan.kind), calls(2).output == Kind.File,
        calls(3).settings == "{}", PdfSteps.ocr.parameters.decode(calls(3).settings) == Right(PdfSteps.Ocr()),
        PdfPipelineModel.decode(compact).toOption.get.recipe == checked.recipe,
        PdfPipelineJson.decode(canonical).toOption.get.recipe == checked.recipe,
        !compact.contains("\"call\""), !compact.contains("\"output\":"), !compact.contains("\"Data\""),
        schema.check(arguments).isEmpty)
    },
    test("legacy verbose recipes remain readable and forged legacy types are rejected") {
      val legacy = single("registered", """, "call":{"operation":"pdf.ocr","version":1,"settings":{},"inputs":["File"],"output":"File"}""")
      val checked = PdfPipelineModel.decode(legacy).toOption.get
      val forged = PdfPipelineModel.decode(legacy.replace("\"output\":\"File\"", "\"output\":\"Document\""))
      assertTrue(PdfPipelineModel.encode(checked).contains("\"op\":\"pdf.ocr\""),
        forged.left.toOption.get.exists(_.code == Code.Type))
    },
    test("registered IDs colliding with built-in spellings retain a schema-valid legacy representation") {
      val results = Vector("input", "registered").map { id =>
        val registry = StepRegistry.standard.add(versioned(1, id)).toOption.get
        val legacy = single("registered", s""", "call":{"operation":"$id","version":1,"settings":{},"inputs":["File"],"output":"File"}""")
        val checked = PdfPipelineModel.decode(legacy, registry).toOption.get
        val encoded = PdfPipelineModel.encode(checked)
        val schema = JsonSchema.fromJson(PdfPipelineModel.recipeSchemaFor(registry)).toOption.get
        PdfPipelineModel.decode(encoded, registry).toOption.get.recipe == checked.recipe &&
          schema.check(Json.parseUnsafe(encoded)).isEmpty
      }
      assertTrue(results.forall(identity))
    },
    test("compact recipes reject wrong wiring, missing ports, unknown operations, and type claims") {
      val wrong = PdfPipelineModel.decode(packet.replace("[\"pdf\",\"plan\"]", "[\"pdf\",\"pdf\"]"))
      val arity = PdfPipelineModel.decode(single("pdf.ocr", inputs = "[]"))
      val unknown = PdfPipelineModel.decode(single("pdf.missing"))
      val forged = PdfPipelineModel.decode(single("pdf.ocr", """, "output":"File""""))
      assertTrue(wrong.left.toOption.get.exists(i => i.code == Code.Type && i.path == "nodes[4].inputs[1]"),
        arity.left.toOption.get.exists(i => i.code == Code.Arity && i.path == "nodes[2].inputs"),
        unknown.left.toOption.get.map(_.path) == Vector("nodes[2].op"),
        forged.left.toOption.get.map(_.path) == Vector("nodes[2].output"))
    },
    test("independent parameter failures accumulate at compact paths") {
      val invalid = packet.replace("\"op\":\"pdf.redact\",", "\"op\":\"pdf.redact\",\"settings\":{\"dpi\":1},")
        .replace("\"op\":\"pdf.ocr\",", "\"op\":\"pdf.ocr\",\"settings\":{\"dpi\":400,\"language\":\"bad!\"},")
      val issues = PdfPipelineModel.decode(invalid).left.toOption.get
      val extra = PdfPipelineModel.decode(single("pdf.ocr", """, "settings":{"execute":"no"}"""))
      val duplicate = PdfPipelineModel.decode(single("pdf.ocr", """, "settings":{"dpi":144,"dpi":200}"""))
      val objectRequired = PdfPipelineModel.decode(single("pdf.ocr", """, "settings":[]"""))
      assertTrue(issues.map(_.path) == Vector("nodes[4].settings.dpi", "nodes[5].settings.dpi", "nodes[5].settings.language"),
        extra.left.toOption.get.exists(_.path == "nodes[2].settings"), duplicate.isLeft,
        objectRequired.left.toOption.get.map(_.path) == Vector("nodes[2].settings"))
    },
    test("version inference requires a unique match and canonical storage keeps the resolved version") {
      val v1 = StepRegistry.standard.add(versioned(1)).toOption.get
      val both = v1.add(versioned(2)).toOption.get
      val inferred = PdfPipelineModel.decode(single("test.versioned"), v1).toOption.get
      val ambiguous = PdfPipelineModel.decode(single("test.versioned"), both)
      val explicit = PdfPipelineModel.decode(single("test.versioned", ",\"version\":1"), both).toOption.get
      val unknown = PdfPipelineModel.decode(single("test.versioned", ",\"version\":999"), both)
      val schema = JsonSchema.fromJson(PdfPipelineModel.recipeSchemaFor(both)).toOption.get
      val onlyV2 = StepRegistry.standard.add(versioned(2)).toOption.get
      val inferredV2 = PdfPipelineModel.decode(single("test.versioned"), onlyV2).toOption.get
      assertTrue(!PdfPipelineModel.encode(inferred).contains("\"version\":1"),
        ambiguous.left.toOption.get.map(_.path) == Vector("nodes[2].version"),
        unknown.left.toOption.get.map(_.path) == Vector("nodes[2].version"),
        PdfPipelineJson.decode(PdfPipelineJson.encode(inferred), both).toOption.get.recipe == inferred.recipe,
        PdfPipelineModel.decode(PdfPipelineModel.encode(explicit), both).toOption.get.recipe == inferred.recipe,
        schema.check(Json.parseUnsafe(PdfPipelineModel.encode(explicit))).isEmpty,
        schema.check(Json.parseUnsafe(single("test.versioned"))).nonEmpty,
        inferredV2.recipe.nodes.last.op == Op.Registered(versioned(2).call(PdfSteps.Empty())))
    },
    test("error policy survives compact serialization and critical recovery remains forbidden") {
      val registry = StepRegistry.standard.add(versioned(1)).toOption.get
      val checked = PdfPipelineModel.decode(single("test.versioned", """, "onError":"RecoverAndFlag""""), registry).toOption.get
      val encoded = PdfPipelineModel.encode(checked)
      val unsafe = PdfPipelineModel.decode(single("pdf.redact", """, "onError":"RecoverAndFlag"""", "[\"pdf\",\"pdf\"]"))
      assertTrue(PdfPipelineModel.decode(encoded, registry).toOption.get.recipe == checked.recipe,
        encoded.contains("RecoverAndFlag"), unsafe.left.toOption.get.exists(_.path == "nodes[2].onError"))
    }
  )
