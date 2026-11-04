package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*
import zio.test.{check as _, *}
import scala.compiletime.testing.typeCheckErrors
import PdfArrow.*
import PdfPipeline.{Kind, Op, Value, Verdict, ValidationReport}

object PdfArrowSpec extends ZIOSpecDefault:
  private val document =
    import PdfValue.*
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "MediaBox" -> Array(Vector(Number(0), Number(0), Number(300), Number(400)))))
    ), dict("Root" -> Ref(ObjectRef(1))))
  private class Services(status: Verdict = Verdict.Passed) extends PdfPipeline.Services:
    var marks = Vector.empty[String]
    var checked = Vector.empty[Vector[Byte]]
    def stamp(doc: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) = Sync.defer {
      marks :+= text
      val catalog = doc.objects.head
      doc.copy(objects = doc.objects.updated(0, catalog.copy(value = catalog.dictionary.get.updated("Mark", PdfValue.Literal(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).toVector)))))
    }
    def number(doc: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) = doc
    def validate(bytes: Vector[Byte], profiles: Vector[String]): String < (Abort[PdfError] & Sync) = Sync.defer { checked :+= bytes; "{}" }
    override def validationReport(bytes: Vector[Byte], profiles: Vector[String]): ValidationReport < (Abort[PdfError] & Sync) =
      validate(bytes, profiles).map(json => ValidationReport(json, profiles.map(_ -> status).toMap))
  private def result[A](program: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def compile[B](arrow: PdfArrow[Unit, B]): PdfPipeline.Checked =
    PdfArrow.recipe("Arrow regression", arrow).flatMap(PdfPipeline.check).fold(e => throw AssertionError(e.toString), identity)
  private def run[B](arrow: PdfArrow[Unit, B], services: Services = new Services): PdfPipeline.Run =
    result(PdfPipeline.run(compile(arrow), Map("body" -> document), services)).fold(e => throw AssertionError(e.message), identity)
  private def files(run: PdfPipeline.Run): Vector[Vector[Byte]] = run.outputs.collect { case (_, Value.File(bytes)) => bytes }

  def spec = suite("Typed PDF arrows")(
    test("archive conversion validates the exact converted bytes and fails closed") {
      class Archiver(verdict: Verdict) extends Services(verdict):
        var conversions = 0
        override def convertPdfA3b(bytes: Vector[Byte]): Vector[Byte] < (Abort[PdfError] & Sync) = Sync.defer {
          conversions += 1
          bytes :+ 0.toByte
        }
      val arrow = input("body") >>> write >>> archivePdfA3b
      val checked = compile(arrow)
      val plan = PdfPipelineModel.decode(PdfPipelineModel.encode(checked)).fold(e => throw AssertionError(e.toString), identity)
      val passed = new Archiver(Verdict.Passed)
      val failed = new Archiver(Verdict.Failed)
      val unable = new Archiver(Verdict.Unable)
      def runWith(service: Archiver) = result(PdfPipeline.run(plan, Map("body" -> document), service))
      val archived = runWith(passed).fold(e => throw AssertionError(e.message), identity)
      val rejected = runWith(failed)
      val unverified = runWith(unable)
      assertTrue(
        plan.recipe == checked.recipe,
        passed.conversions == 1,
        failed.conversions == 1,
        unable.conversions == 1,
        files(archived) == passed.checked,
        passed.checked.size == 1,
        archived.outputs.size == 2,
        rejected.left.toOption.exists(_.message.contains("did not pass")),
        unverified.left.toOption.exists(_.message.contains("could not be validated"))
      )
    },
    test("a serialized conditional round-trips and executes only its selected branch") {
      val route = choose(pages(1, 1))(stamp("ONE"), stamp("MANY"), stop[Document, Document]("unknown"))
      val arrow = input("body") >>> route >>> write
      val original = compile(arrow)
      val json = PdfPipelineJson.encode("Arrow regression", arrow).fold(e => throw AssertionError(e.toString), identity)
      val portableJson =
        """{"version":3,"name":"Arrow regression","nodes":[
          |{"id":"body","op":{"Input":{"slot":"body"}}},
          |{"id":"stamp","op":{"Stamp":{"text":"ONE"}},"inputs":["body"]},
          |{"id":"stamp-2","op":{"Stamp":{"text":"MANY"}},"inputs":["body"]},
          |{"id":"stop","op":{"StopAfter":{"message":"unknown","kinds":["Document"]}},"inputs":["body"]},
          |{"id":"route","op":{"Choose":{"test":{"PageCount":{"min":1,"max":1}}}},"inputs":["body","stamp","stamp-2","stop"]},
          |{"id":"pdf","op":{"Write":{}},"inputs":["route"]}],"outputs":["pdf"]}""".stripMargin.linesIterator.mkString
      val decoded = PdfPipelineJson.decode(portableJson).fold(e => throw AssertionError(e.toString), identity)
      val twoPages = result(PdfAssembly.concatenate(Vector(document, document), 0))
        .fold(e => throw AssertionError(e.message), identity)

      val one = new Services
      val many = new Services
      val oneRun = result(PdfPipeline.run(decoded, Map("body" -> document), one))
        .fold(e => throw AssertionError(e.message), identity)
      val manyRun = result(PdfPipeline.run(decoded, Map("body" -> twoPages), many))
        .fold(e => throw AssertionError(e.message), identity)
      val expected = result(PdfPipeline.run(original, Map("body" -> twoPages), new Services))
        .fold(e => throw AssertionError(e.message), identity)

      assertTrue(
        json == portableJson,
        decoded.recipe == original.recipe,
        PdfPipelineJson.encode(decoded) == json,
        oneRun.decisions.map(_.outcome) == Vector(PdfPipeline.Outcome.Yes),
        manyRun.decisions.map(_.outcome) == Vector(PdfPipeline.Outcome.No),
        one.marks == Vector("ONE"),
        many.marks == Vector("MANY"),
        files(oneRun).size == 1,
        files(manyRun) == files(expected),
        PdfPipelineJson.decode(json.replace("\"version\":3", "\"version\":0"))
          .left.toOption.exists(_.exists(_.code == PdfPipeline.Code.Version)),
        PdfPipelineJson.decode("{").left.toOption.exists(_.exists(_.code == PdfPipeline.Code.Structure))
      )
    },
    test("category identity and associativity preserve the compiled program and written bytes") {
      val f = select(1); val g = stamp("A"); val h = stamp("B")
      val left = input("body") >>> ((f >>> g) >>> h) >>> write
      val right = input("body") >>> (f >>> (g >>> h)) >>> write
      val units = input("body") >>> (id[Document] >>> f >>> id[Document] >>> g >>> h) >>> write
      assertTrue(compile(left).recipe == compile(right).recipe, compile(left).recipe == compile(units).recipe,
        files(run(left)) == files(run(right)), files(run(left)) == files(run(units)))
    },
    test("tensor interchange preserves each independent output") {
      val a = stamp("A"); val b = stamp("B"); val c = stamp("C"); val d = stamp("D")
      val prefix = input("body") >>> fork[Document]
      val left = prefix >>> ((a >< b) >>> (c >< d)) >>> (write >< write)
      val right = prefix >>> ((a >>> c) >< (b >>> d)) >>> (write >< write)
      assertTrue(files(run(left)) == files(run(right)), files(run(left)).length == 2,
        files(run(left))(0) != files(run(left))(1))
    },
    test("braiding, association and unit morphisms preserve typed port order") {
      val pair = input("body") >>> fork[Document] >>> (stamp("LEFT") >< stamp("RIGHT"))
      val same = pair >>> swap[Document, Document] >>> swap[Document, Document] >>> (write >< write)
      val direct = pair >>> (write >< write)
      val reassociated = (pair &&& input("body")) >>> assocRight[Document, Document, Document] >>> assocLeft[Document, Document, Document] >>> assemble3() >>> write
      val original = (pair &&& input("body")) >>> assemble3() >>> write
      val unit = input("body") >>> spawnLeft[Document] >>> leftUnit[Document] >>> write
      assertTrue(files(run(same)) == files(run(direct)), files(run(reassociated)) == files(run(original)),
        files(run(unit)) == files(run(input("body") >>> write)))
    },
    test("reusable subarrows allocate fresh IDs each time and compilation is repeatable") {
      val finish = stamp("REUSED").named("mark") >>> write
      val arrow = input("body") >>> fork[Document] >>> (finish >< finish)
      val plan = compile(arrow)
      val services = new Services
      val completed = run(arrow, services)
      assertTrue(plan.recipe == compile(arrow).recipe, plan.recipe.nodes.map(_.id).distinct.length == plan.recipe.nodes.length,
        plan.recipe.nodes.exists(_.id == "mark-2"), services.marks == Vector("REUSED", "REUSED"), files(completed).length == 2)
    },
    test("fanout shares its preceding work and retains duplicate output positions") {
      val services = new Services
      val arrow = input("body") >>> stamp("ONCE") >>> write >>> (id[File] &&& id[File])
      val completed = run(arrow, services)
      assertTrue(services.marks == Vector("ONCE"), completed.outputs.length == 2,
        files(completed).distinct.length == 1, completed.executed.count(_.startsWith("stamp")) == 1)
    },
    test("a conditional takes arrows and executes only the chosen branch") {
      val route = choose(pages(1, 1))(stamp("CHOSEN"), select(99) >>> stamp("UNUSED"), stop[Document, Document]("unused stop"))
      val services = new Services
      val completed = run(input("body") >>> route >>> write, services)
      assertTrue(services.marks == Vector("CHOSEN"), completed.decisions.head.outcome == PdfPipeline.Outcome.Yes,
        completed.skipped.length == 3, completed.outputs.length == 1)
    },
    test("nested conditional arrows chain and rejoin before a shared write") {
      val inner = choose(pages(2, 4))(stop[Document, Document]("unused"), stamp("INNER"), stop[Document, Document]("unknown"))
      val outer = choose(pages(1, 1))(inner, stamp("OUTER"), stop[Document, Document]("unknown"))
      val services = new Services
      val completed = run(input("body") >>> outer >>> stamp("AFTER") >>> write, services)
      assertTrue(services.marks == Vector("INNER", "AFTER"), completed.decisions.map(_.outcome) == Vector(PdfPipeline.Outcome.Yes, PdfPipeline.Outcome.No),
        completed.executed.count(_ == "pdf") == 1)
    },
    test("typed validation predicates route passed, failed and unable independently") {
      val checked = id[File] &&& validate("3b")
      val condition = verified("3b").on(keepRight[File, Receipt])
      val route = choose(condition)(keepLeft[File, Receipt], stop[(File, Receipt), File]("failed verdict"), stop[(File, Receipt), File]("unable verdict"))
      val arrow = input("body") >>> write >>> checked >>> route
      val pass = new Services(Verdict.Passed)
      val failed = new Services(Verdict.Failed)
      val unable = new Services(Verdict.Unable)
      val success = run(arrow, pass)
      val failure = result(PdfPipeline.run(compile(arrow), Map("body" -> document), failed))
      val unknown = result(PdfPipeline.run(compile(arrow), Map("body" -> document), unable))
      assertTrue(files(success) == pass.checked, failed.checked.length == 1, unable.checked.length == 1,
        failure.left.toOption.get.message.contains("failed verdict"), unknown.left.toOption.get.message.contains("unable verdict"))
    },
    test("a projection still executes the other arrow in a tensor") {
      val services = new Services
      val arrow = input("body") >>> fork[Document] >>> (stamp("LEFT") >< stamp("RIGHT")) >>> keepLeft[Document, Document] >>> write
      val completed = run(arrow, services)
      assertTrue(services.marks == Vector("LEFT", "RIGHT"), completed.executed.count(_.startsWith("stamp")) == 2)
    },
    test("stopping after a check preserves sequential evaluation") {
      val services = new Services
      val arrow = input("body") >>> stamp("BEFORE") >>> write >>> validate("3b") >>> stop[Receipt, File]("stop after check")
      val failure = result(PdfPipeline.run(compile(arrow), Map("body" -> document), services))
      assertTrue(services.marks == Vector("BEFORE"), services.checked.length == 1,
        failure.left.toOption.get.message.contains("stop after check"))
    },
    test("independent arrow setting failures still accumulate in the graph boundary") {
      val arrow = input("body") >>> choose(pages(0, 65))(stamp(""), stamp("", "outside"), stop[Document, Document]("")) >>> write
      val issues = PdfArrow.recipe("Bad settings", arrow).left.toOption.get
      assertTrue(issues.count(_.code == PdfPipeline.Code.Setting) == 5)
    },
    test("wrong composition, mixed branches and invalid unit stops fail at compile time") {
      val composition = typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.PdfArrow.*; val bad = write >>> stamp("wrong")""")
      val branch = typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.PdfArrow.*; val bad = choose(pages(1, 1))(write, id[Document], write)""")
      val unit = typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.PdfArrow.*; val bad = stop[Document, Unit]("lost effect")""")
      assertTrue(composition.nonEmpty, branch.nonEmpty, unit.nonEmpty)
    },
    test("version 3 projection and stop settings validate without unsafe indexing") {
      val invalid = PdfPipeline.Recipe(2, "Bad arrow data", Vector(
        PdfPipeline.Node("keep", Op.Keep(9, Vector.empty)),
        PdfPipeline.Node("stop", Op.StopAfter("", Vector.empty, Kind.File))
      ), Vector("stop"))
      val issues = PdfPipeline.check(invalid).left.toOption.get
      assertTrue(issues.count(_.code == PdfPipeline.Code.Version) == 2, issues.count(_.code == PdfPipeline.Code.Setting) == 4)
    }
  )
