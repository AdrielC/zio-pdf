package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*
import zio.test.*
import PdfArrow.*
import PdfSteps.given
import StepRegistry.{Error, ErrorCode, OnError}
import java.nio.file.{Files, Path}

object RegisteredStepsSpec extends ZIOSpecDefault:
  private val document =
    import PdfValue.*
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "MediaBox" -> Array(Vector(Number(0), Number(0), Number(300), Number(400)))))
    ), dict("Root" -> Ref(ObjectRef(1))))
  private class Services extends PdfPipeline.Services:
    def stamp(doc: PdfDocument, text: String, placement: String) = doc
    def number(doc: PdfDocument, start: Int, style: String, prefix: String) = doc
  private def result[E: ConcreteTag, A](program: A < (Abort[E] & Sync)): Either[E, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[E](Sync.Unsafe.run(program)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def checked[B](arrow: PdfArrow[Unit, B], registry: StepRegistry.Registry = StepRegistry.standard) =
    PdfArrow.recipe("Registered workflow", arrow, registry).flatMap(PdfPipeline.check(_, registry)).fold(e => throw AssertionError(e.toString), identity)
  private def run(plan: PdfPipeline.Checked, services: Services = new Services) = result(PdfPipeline.run(plan, Map("body" -> document), services))
  private def file = result(PdfWriter.write(document)).flatMap(bytes => PdfFile.checked(bytes).left.map(_ => PdfError.InvalidPdf("Fixture"))).toOption.get
  private def failure[A](code: ErrorCode, recoverable: Boolean = false): A < Abort[Error] = Abort.fail(Error(code, "expected failure", recoverable))

  def spec = suite("Registered workflows")(
    test("registered arrows round-trip through canonical and model JSON, fuse scans, and expose typed data") {
      val arrow = input("body") >>> write >>>
        (registered(PdfSteps.byteCount, PdfSteps.Empty()) &&& registered(PdfSteps.highByteCount, PdfSteps.Empty()))
      val plan = checked(arrow)
      val model = PdfPipelineModel.encode(plan)
      val decoded = PdfPipelineModel.decode(model).toOption.get
      val canonical = PdfPipelineJson.decode(PdfPipelineJson.encode(plan)).toOption.get
      val executed = run(decoded).toOption.get
      val args = zio.blocks.schema.json.Json.Object("recipe" -> zio.blocks.schema.json.Json.parseUnsafe(model),
        "files" -> zio.blocks.schema.json.Json.Object("body" -> zio.blocks.schema.json.Json.String("document-id")))
      val toolSchema = zio.blocks.schema.json.JsonSchema.fromJson(PdfPipelineModel.toolSchema(withFiles = true)).toOption.get
      val counts = executed.outputs.collect { case (_, PdfPipeline.Value.Data(kind, data)) if kind == PdfSteps.count.kind =>
        PdfSteps.count.decode(data.encoded).toOption.get.value
      }
      assertTrue(plan.recipe.version == 4, decoded.recipe == plan.recipe, canonical.recipe == plan.recipe,
        toolSchema.check(args).map(_.toString.take(500)).isEmpty,
        executed.sourcePasses == 1, counts == Vector(file.size, file.bytes.count(_ < 0).toLong), model.contains("\"settings\":{}"),
        StepRegistry.standard.exported.print.contains("pdf.redact"),
        ExecutionPlanner.explain(plan, file.size).fusedScans.size == 1)
    },
    test("generic schema filters and gates round-trip with a custom registry") {
      val items = new StepRegistry.DataSchema("test.counts", 1, zio.blocks.schema.Schema.derived[DataPrograms.Collection[PdfSteps.Count]])
      val source = StepRegistry.literal("test.counts", items, DataPrograms.Collection(Vector(PdfSteps.Count(1), PdfSteps.Count(4))))
      val predicate = DataPrograms.Predicate.Compare(Vector("value"), DataPrograms.Comparison.AtLeast, DataPrograms.Literal.Number(2))
      val filter = DataPrograms.filter("test.filter", items, PdfSteps.count, DataPrograms.Filter(predicate))
      val registry = StepRegistry.standard.add(source).toOption.get.add(filter).toOption.get
      val plan = checked(registered(source, DataPrograms.Collection(Vector(PdfSteps.Count(1), PdfSteps.Count(4)))) >>>
        registered(filter, DataPrograms.Filter(predicate)), registry)
      val model = PdfPipelineModel.encode(plan)
      val decoded = PdfPipelineModel.decode(model, registry).toOption.get
      val executed = run(decoded).toOption.get
      val output = executed.outputs.head._2.asInstanceOf[PdfPipeline.Value.Data]
      val absent = DataPrograms.Predicate.Compare(Vector("missing"), DataPrograms.Comparison.Equal, DataPrograms.Literal.Bool(true))
      val gate = DataPrograms.gate("test.gate", items, DataPrograms.Gate(absent))
      val unknownPlan = checked(registered(source, DataPrograms.Collection(Vector(PdfSteps.Count(1)))) >>> registered(gate, DataPrograms.Gate(absent)),
        registry.add(gate).toOption.get)
      val schema = zio.blocks.schema.json.JsonSchema.fromJson(PdfPipelineModel.recipeSchemaFor(registry)).toOption.get
      assertTrue(items.decode(output.value.encoded).toOption.get.items == Vector(PdfSteps.Count(4)),
        schema.check(zio.blocks.schema.json.Json.parseUnsafe(model)).map(_.toString.take(500)).isEmpty,
        run(unknownPlan).isLeft, DataPrograms.evaluate(absent, zio.blocks.schema.json.Json.Object()) == PdfPipeline.Outcome.Unknown)
    },
    test("registry rejects unknown versions, forged ports, extra parameters, and independent bad fields") {
      val call = PdfSteps.replaceText.call(PdfSteps.Replace("", "x", font = "unknown", dpi = 1))
      val bad = StepRegistry.standard.check(call)
      val ports = StepRegistry.standard.check(call.copy(inputs = Vector(PdfPipeline.Kind.Document)))
      val unknown = StepRegistry.standard.check(call.copy(version = 999))
      val extra = StepRegistry.standard.check(PdfSteps.cleanup.call(PdfSteps.Edit()).copy(settings = "{\"dpi\":144,\"execute\":\"no\"}"))
      val duplicate = StepRegistry.standard.add(PdfSteps.cleanup)
      assertTrue(bad.size >= 2, ports.exists(_.code == PdfPipeline.Code.Type), unknown.nonEmpty, extra.nonEmpty, duplicate.isLeft)
    },
    test("policy results drive yes/no/unknown branches and filters preserve evidence identity") {
      val base = CourtProfile.CaliforniaSecondDistrictPetition.rules
      def rule(condition: Policy.Condition) = base.copy(rules = Vector(base.rules.head.copy(
        scope = Policy.Scope.Document, condition = condition, when = None)))
      val pass = rule(Policy.Condition.Compare(Policy.Operand.Fact(Policy.Metric.PageCount), Policy.Comparison.Equal, Policy.Operand.Number(1)))
      val no = rule(Policy.Condition.Compare(Policy.Operand.Fact(Policy.Metric.PageCount), Policy.Comparison.Equal, Policy.Operand.Number(2)))
      val unknown = rule(Policy.Condition.Review)
      def route(profile: Policy.Profile) =
        val evidence = registered(PdfSteps.inspect, PdfSteps.Empty()) >>> registered(PdfSteps.evaluatePolicy, PdfSteps.Evaluate(profile))
        val condition = policyPassed.on(keepRight[File, PdfSteps.PolicyResult])
        val branch = choose(condition)(keepLeft[File, PdfSteps.PolicyResult], stop[(File, PdfSteps.PolicyResult), File]("policy rejected"),
          stop[(File, PdfSteps.PolicyResult), File]("policy needs review"))
        checked(input("body") >>> write >>> (id[File] &&& evidence) >>> branch)
      val success = run(route(pass)).toOption.get
      val filtered = run(checked(input("body") >>> write >>> registered(PdfSteps.inspect, PdfSteps.Empty()) >>>
        registered(PdfSteps.pageCollection, PdfSteps.Empty()) >>> registered(PdfSteps.filterPages, PdfSteps.PageFilter(2, 3))))
      assertTrue(success.decisions.map(_.outcome) == Vector(PdfPipeline.Outcome.Yes),
        run(route(no)).left.toOption.exists(_.message.contains("policy rejected")),
        run(route(unknown)).left.toOption.exists(_.message.contains("policy needs review")), filtered.isRight)
    },
    test("recoverable errors are flagged, hard errors fail fast, and redaction cannot recover") {
      val optional = new StepRegistry.Step("test.optional", 1, StepRegistry.Port.file, StepRegistry.Port.file,
        PdfSteps.empty, Vector(PdfSteps.Empty()), StepRegistry.Access.Stream, Set(StepRegistry.Capability.Inspect))(
        (_, _, _) => failure(ErrorCode.Unavailable, recoverable = true))
      val hard = new StepRegistry.Step("test.hard", 1, StepRegistry.Port.file, StepRegistry.Port.file,
        PdfSteps.empty, Vector(PdfSteps.Empty()), StepRegistry.Access.Stream, Set(StepRegistry.Capability.Inspect))(
        (_, _, _) => failure(ErrorCode.Verification, recoverable = true))
      val registry = StepRegistry.standard.add(optional).toOption.get.add(hard).toOption.get
      val recovered = run(checked(input("body") >>> write >>> registered(optional, PdfSteps.Empty(), OnError.RecoverAndFlag), registry)).toOption.get
      val rejected = run(checked(input("body") >>> write >>> registered(hard, PdfSteps.Empty(), OnError.RecoverAndFlag), registry))
      assertTrue(recovered.flags.size == 1, recovered.outputs.collect { case (_, PdfPipeline.Value.File(f)) => f } == Vector(file),
        rejected.isLeft, StepRegistry.standard.check(PdfSteps.cleanup.call(PdfSteps.Edit(), OnError.RecoverAndFlag)).nonEmpty)
    },
    test("staging spills at the threshold, reuses bytes, closes resources, and stops at quotas") {
      val chunks = Iterator(Array[Byte](1, 2), Array[Byte](3, 4))
      var escaped: Option[ExecutionPlanner.Staged] = None
      val spooled = result(ExecutionPlanner.withStaged(chunks, ExecutionPlanner.Storage(2, 4)) { stage =>
        escaped = Some(stage)
        stage.withInput { stream =>
          val bytes = stream.readNBytes(5).toVector
          (stage.strategy, stage.size, bytes)
        }
      })
      val gone = result(escaped.get.withInput(_ => ())).isLeft
      val memory = result(ExecutionPlanner.withStaged(Iterator(Array[Byte](5, 6)), ExecutionPlanner.Storage(8, 8)) { stage =>
        stage.withInput(stream => (stage.strategy, stage.size, stream.readNBytes(9).toVector))
      })
      var pulled = 0
      val over = result(ExecutionPlanner.withStaged(Iterator.fill(10) { pulled += 1; Array[Byte](1, 2) }, ExecutionPlanner.Storage(2, 3))(_ => ()))
      var abortedStage: Option[ExecutionPlanner.Staged] = None
      val aborted = result(ExecutionPlanner.withStaged(Iterator(Array[Byte](1, 2)), ExecutionPlanner.Storage(1, 4)) { stage =>
        abortedStage = Some(stage)
        failure[Unit](ErrorCode.Rejected)
      })
      val closedAfterAbort = result(abortedStage.get.withInput(_ => ())).isLeft
      val tooLargeChunk = result(ExecutionPlanner.withStaged(Iterator.single(new Array[Byte](ExecutionPlanner.MaxChunkBytes + 1)))(_ => ()))
      assertTrue(spooled.contains((ExecutionPlanner.Strategy.DiskSpool, 4L, Vector[Byte](1, 2, 3, 4))), gone,
        over.left.toOption.exists(_.code == ErrorCode.Limit), pulled == 2, tooLargeChunk.isLeft, aborted.isLeft, closedAfterAbort,
        memory.contains((ExecutionPlanner.Strategy.BoundedMemory, 2L, Vector[Byte](5, 6))))
    },
    test("bounded collections stop pulling on hard errors and retain explicitly recoverable flags") {
      var pulls = 0
      def input = Iterator(1, 2, 3).map { n => pulls += 1; n }
      val failed = result(ExecutionPlanner.mapBounded(input, 3)((_: Int) => 8L)(n => if n == 2 then failure(ErrorCode.Rejected) else n))
      val failPulls = pulls
      val continued = result(ExecutionPlanner.mapBounded(Iterator(1, 2, 3), 3, recoverAndFlag = true)((_: Int) => 8L)(
        n => if n == 2 then failure(ErrorCode.Unavailable, recoverable = true) else n))
      val quota = result(ExecutionPlanner.mapBounded(Iterator(1, 2), 1)((_: Int) => 8L)(identity))
      assertTrue(failed.isLeft, failPulls == 2, continued.toOption.exists(_.map(_.result.isRight) == Vector(true, false, true)), quota.isLeft)
    },
    test("a failed required final check exposes no provisional file") {
      val builder = new RecipeBuilder
      val pdf = builder.write(builder.input("body"))
      val failed = builder.stopFile("final check")
      val plan = PdfPipeline.check(builder.outputs("Transactional result", pdf, failed)).toOption.get
      assertTrue(run(plan).isLeft)
    },
    test("missing backend, mismatched plan hash, and tampered receipts fail closed") {
      val arrow = input("body") >>> write >>> PdfSteps.redactArrow(PdfSteps.Select(Vector(PdfSteps.Region(1, 10, 10, 20, 20))))
      val unavailable = run(checked(arrow))
      val backend = new SemanticBackend:
        def locate(file: PdfFile, settings: PdfSteps.Select) = SemanticBackend.unavailable.locate(file, settings)
        def redact(file: PdfFile, plan: PdfSteps.RedactionPlan, settings: PdfSteps.Edit) =
          SemanticBackend.Artifact(file, PdfSteps.sha256(file), "forged", "redact", verified = true)
        def replace(file: PdfFile, settings: PdfSteps.Replace) = SemanticBackend.unavailable.replace(file, settings)
        def ocr(file: PdfFile, settings: PdfSteps.Ocr) = SemanticBackend.unavailable.ocr(file, settings)
        def cleanup(file: PdfFile, settings: PdfSteps.Edit) = SemanticBackend.unavailable.cleanup(file, settings)
      val services = new Services:
        override def semantic = backend
      val tampered = run(checked(arrow), services)
      assertTrue(unavailable.left.toOption.exists(_.message.contains("Unavailable")),
        tampered.left.toOption.exists {
          case PdfError.OperationFailed(_, _, error) => error.code == ErrorCode.Verification
          case _ => false
        })
    },
    test("real local worker redacts then OCRs through a serialized graph on this platform") {
      val python = sys.env("KYO_PDF_SEMANTIC_PYTHON")
      val root = Path.of(sys.env("KYO_PDF_SEMANTIC_FIXTURES"))
      val backend = new ProcessSemanticBackend(Path.of(python), Path.of("scripts/semantic-worker.py"))
      val services = new Services:
        override def semantic = backend
      val bytes = Files.readAllBytes(root.resolve("source.pdf"))
      val limit = result(ByteLimit.fromBytes(bytes.length.toLong)).toOption.get
      val source = result(PdfDocument.decode(bytes, limit)).toOption.get
      val arrow = input("body") >>> write >>> PdfSteps.redactArrow(PdfSteps.Select(Vector(PdfSteps.Region(1, 18, 18, 220, 47)))) >>>
        registered(PdfSteps.ocr, PdfSteps.Ocr())
      val plan = PdfPipelineModel.decode(PdfPipelineModel.encode(checked(arrow))).toOption.get
      val output = result(PdfPipeline.run(plan, Map("body" -> source), services)).fold(e => throw AssertionError(e.message), identity).outputs.collectFirst {
        case (_, PdfPipeline.Value.File(file)) => file
      }.get
      val keep = result(backend.locate(output, PdfSteps.Select(terms = Vector("KEEP VISIBLE"))))
      val secret = result(backend.locate(output, PdfSteps.Select(terms = Vector("SECRET42"))))
      val wrong = result(backend.redact(output, PdfSteps.RedactionPlan("wrong", Vector(PdfSteps.Region(1, 1, 1, 5, 5)), "test"), PdfSteps.Edit()))
      Files.write(root.resolve("graph-output.pdf"), output.bytes.toArray)
      assertTrue(keep.isRight, secret.isLeft, wrong.isLeft, output != file)
    } @@ TestAspect.ifEnvSet("KYO_PDF_SEMANTIC_PYTHON")
  ) @@ TestAspect.sequential
