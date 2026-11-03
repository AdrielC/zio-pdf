package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*
import zio.test.{check as _, *}
import PdfPipeline.*

object PdfPipelineSpec extends ZIOSpecDefault:
  private val document =
    import PdfValue.*
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "MediaBox" -> Array(Vector(Number(0), Number(0), Number(300), Number(400)))))
    ), dict("Root" -> Ref(ObjectRef(1))))
  private def result[A](program: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def value[A](program: A < (Abort[PdfError] & Sync)): A = result(program).fold(e => throw AssertionError(e.message), identity)
  private def checked(recipe: Recipe): Checked = check(recipe).fold(e => throw AssertionError(e.toString), identity)
  private class ServicesStub extends Services:
    var validations = Vector.empty[Vector[Byte]]
    var stamps = 0
    def stamp(document: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) = Sync.defer { stamps += 1; document }
    def number(document: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) = document
    def validate(bytes: Vector[Byte], profiles: Vector[String]): String < (Abort[PdfError] & Sync) = Sync.defer { validations :+= bytes; "{}" }
  private val base = Recipe(1, "Synthetic recipe", Vector(Node("body", Op.Input("body")), Node("pdf", Op.Write, Vector("body"))), Vector("pdf"))

  def spec = suite("Data-defined PDF pipelines")(
    test("typed Scala authoring produces an inspectable closed graph") {
      val builder = new RecipeBuilder
      val body = builder.input("body")
      val selected = builder.select(body, 1)
      val marked = builder.stamp(selected, "DRAFT")
      val file = builder.write(marked)
      val receipt = builder.validate(file, "3b", "2b")
      val recipe = builder.recipe("Packet", file, receipt)
      assertTrue(check(recipe).isRight, recipe.nodes.length == 5, recipe.outputs.length == 2)
    },
    test("invalid independent settings accumulate before dependent graph checks") {
      val invalid = Recipe(9, "", Vector(Node("stamp", Op.Stamp("", "outside"), Vector("missing")),
        Node("number", Op.NumberPages(0, "wrong", "x" * 25), Vector("missing"))), Vector("absent"))
      val errors = check(invalid).left.toOption.get
      assertTrue(errors.map(_.path) == Vector("version", "name", "nodes[0].text", "nodes[0].placement", "nodes[1].start", "nodes[1].style", "nodes[1].prefix"),
        !errors.exists(_.code == Code.Reference))
    },
    test("missing references and incompatible ports are reported together") {
      val recipe = base.copy(nodes = base.nodes ++ Vector(Node("bad", Op.Stamp("DRAFT"), Vector("pdf")), Node("missing", Op.Write, Vector("absent"))))
      val codes = check(recipe).left.toOption.get.map(_.code)
      assertTrue(codes == Vector(Code.Type, Code.Reference))
    },
    test("duplicate IDs, cycles, output types, unsupported profiles and oversized graphs are rejected") {
      val cycle = base.copy(nodes = Vector(Node("a", Op.Select(Vector(1)), Vector("b")), Node("b", Op.Select(Vector(1)), Vector("a")), Node("pdf", Op.Write, Vector("a"))))
      assertTrue(check(base.copy(nodes = base.nodes ++ base.nodes)).left.toOption.get.exists(_.code == Code.Duplicate),
        check(cycle).left.toOption.get.exists(_.code == Code.Cycle), check(base.copy(outputs = Vector("body"))).isLeft,
        check(base.copy(nodes = base.nodes :+ Node("checks", Op.Validate(Vector("4f")), Vector("pdf")))).isLeft,
        check(base.copy(nodes = Vector.fill(25)(Node("body", Op.Input("body"))))).isLeft)
    },
    test("fork/join runs shared steps once and validation receives the exact written bytes") {
      val recipe = Recipe(1, "Fork join", Vector(Node("checks2", Op.Validate(Vector("2b")), Vector("pdf")),
        Node("pdf", Op.Write, Vector("assembled")), Node("checks3", Op.Validate(Vector("3b")), Vector("pdf")),
        Node("assembled", Op.Assemble(), Vector("marked", "marked")), Node("marked", Op.Stamp("DRAFT"), Vector("body")), Node("body", Op.Input("body"))), Vector("pdf", "checks2", "checks3"))
      val services = new ServicesStub
      val run = value(PdfPipeline.run(checked(recipe), Map("body" -> document), services))
      val bytes = run.outputs.head._2.asInstanceOf[Value.File].bytes
      val decoded = value(ByteLimit.fromBytes(1024 * 1024).map(limit => PdfDocument.decode(bytes.toArray, limit)))
      assertTrue(services.stamps == 1, services.validations == Vector(bytes, bytes), run.executed.distinct == run.executed,
        value(PdfPages.pageRefs(decoded)).length == 2)
    },
    test("a suspended program can run twice without retaining consumed use counts") {
      val services = new ServicesStub
      val program = PdfPipeline.run(checked(base), Map("body" -> document), services)
      val first = value(program)
      val second = value(program)
      assertTrue(first.outputs == second.outputs)
    },
    test("unreachable steps are not executed and live values respect budgets") {
      val services = new ServicesStub
      val recipe = base.copy(nodes = base.nodes :+ Node("unused", Op.Stamp("DRAFT"), Vector("body")))
      val run = value(PdfPipeline.run(checked(recipe), Map("body" -> document), services))
      assertTrue(services.stamps == 0, !run.executed.contains("unused"),
        result(PdfPipeline.run(checked(base), Map.empty, services)).isLeft,
        result(PdfPipeline.run(checked(base), Map("body" -> document), services, Limits(maxLiveObjects = 1))).isLeft,
        result(PdfPipeline.run(checked(base), Map("body" -> document), services, Limits(maxBytes = 10))).isLeft)
    },
    test("saved subpipelines inline with namespaced IDs and explicit input bindings") {
      val fragment = Recipe(1, "Finishing", Vector(Node("input", Op.Input("source")), Node("mark", Op.Stamp("REVIEW"), Vector("input")), Node("file", Op.Write, Vector("mark"))), Vector("file"))
      val combined = base.include("review", fragment, Map("source" -> "body")).toOption.get
      val services = new ServicesStub
      val run = value(PdfPipeline.run(checked(combined), Map("body" -> document), services))
      assertTrue(combined.nodes.find(_.id == "review-mark").get.inputs == Vector("body"),
        run.outputs.map(_._1) == Vector("pdf", "review-file"), services.stamps == 1,
        base.include("review", fragment, Map.empty).isLeft,
        combined.include("review", fragment, Map("source" -> "body")).isLeft)
    }
  )
