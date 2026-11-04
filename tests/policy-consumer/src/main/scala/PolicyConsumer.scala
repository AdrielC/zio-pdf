import io.github.adrielc.kyopdf.*
import io.github.adrielc.kyopdf.pipeline.{PdfArrow, PdfPipeline, PdfPipelineJson, PdfPipelineModel, Policy}
import io.github.adrielc.kyopdf.zio.PdfZIO
import kyo.*

object PolicyConsumer:
  private def value[A](program: A < (Abort[PdfError] & Sync)): A =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case kyo.Result.Success(result) => result
      case kyo.Result.Failure(error) => throw AssertionError(error.message)
      case kyo.Result.Panic(error) => throw error

  def main(args: Array[String]): Unit =
    import Policy.*
    val profile = Profile(1, "report-v1", "Report policy", "Example organization", "Report",
      "2026-11-03", "Synthetic report requirements", Vector(
        Rule("size", Scope.Document,
          Condition.Compare(Operand.Fact(Metric.FileBytes), Comparison.LessThanOrEqual, Operand.Number(5000)),
          "Rule 1", "Synthetic report requirements", "File must fit within the limit.")
      ))
    val policy = Policy.decode(Policy.encode(profile)).toOption.get
    def status(bytes: Option[Long]): String =
      policy.run(Vector(Observation(Scope.Document, bytes.map(Metric.FileBytes -> _).toMap))).head.status
    assert(status(Some(200)) == "pass")
    assert(status(Some(6000)) == "fix-required")
    assert(status(None) == "needs-review")
    assert(Policy.jsonSchema.print.contains("authority"))

    import PdfValue.*
    val document = PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)),
        "MediaBox" -> Array(Vector(Number(0), Number(0), Number(300), Number(400)))))
    ), dict("Root" -> Ref(ObjectRef(1))))

    import PdfArrow.*
    val route = choose(pages(1, 1))(
      id[Document], stop[Document, Document]("Unexpected page count"), stop[Document, Document]("Review required"))
    val recipe = PdfArrow.recipe("Policy consumer", input("body") >>> route >>> write >>>
      (id[File] &&& preflight(profile))).toOption.get
    val plan = PdfPipeline.check(recipe).toOption.get
    val loaded = PdfPipelineJson.decode(PdfPipelineJson.encode(plan)).toOption.get
    assert(PdfPipelineModel.decode(PdfPipelineModel.encode(loaded)).toOption.get.recipe == recipe)
    val services = new PdfPipeline.Services:
      def stamp(doc: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) =
        throw AssertionError("Unexpected stamp")
      def number(doc: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) =
        throw AssertionError("Unexpected numbering")
      def validate(bytes: Vector[Byte], profiles: Vector[String]): String < (Abort[PdfError] & Sync) =
        throw AssertionError("Unexpected archival validation")
      override def preflightRules(bytes: Vector[Byte], rules: Policy.Checked): String < (Abort[PdfError] & Sync) =
        val findings = rules.run(Vector(Observation(Scope.Document, Map(Metric.FileBytes -> bytes.length.toLong))))
        assert(findings.map(_.status) == Vector("pass"))
        "policy-passed"
    val run = value(PdfPipeline.run(loaded, Map("body" -> document), services))
    assert(run.decisions.map(_.outcome) == Vector(PdfPipeline.Outcome.Yes))
    assert(run.outputs.exists(_._2 == PdfPipeline.Value.Receipt("policy-passed")))
    val output = run.outputs.collectFirst { case (_, PdfPipeline.Value.File(bytes)) => bytes }.get
    val adapted = _root_.zio.Unsafe.unsafe { implicit unsafe =>
      _root_.zio.Runtime.default.unsafe.run(PdfZIO.write(document)).getOrThrowFiberFailure()
    }
    assert(output == adapted)
    println("Published Policy API compiled SchemaExpr, round-tripped and ran a conditional pipeline, and linked the ZIO adapter.")
