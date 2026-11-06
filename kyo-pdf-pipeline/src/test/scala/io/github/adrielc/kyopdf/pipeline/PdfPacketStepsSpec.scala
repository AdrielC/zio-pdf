package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*
import zio.test.*
import PdfArrow.*

object PdfPacketStepsSpec extends ZIOSpecDefault:
  import PdfValue.*
  private val document = PdfDocument("1.7", Vector(
    PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
    PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Kids" -> PdfValue.Array(Vector(Ref(ObjectRef(3)))), "Count" -> Number(1))),
    PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)),
      "MediaBox" -> PdfValue.Array(Vector(Number(0), Number(0), Number(300), Number(400)))))
  ), dict("Root" -> Ref(ObjectRef(1))))
  private def run[A](value: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(value)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private val services = new PdfPipeline.Services:
    def stamp(doc: PdfDocument, text: String, placement: String) = doc
    def number(doc: PdfDocument, start: Int, style: String, prefix: String) = doc
  private def checked =
    val a = input("first") >>> write
    val b = input("second") >>> write
    val c = input("third") >>> write
    val arrow = ((a &&& b) &&& c) >>> registered(PdfPacketSteps.three,
      PdfPacketSteps.Options("Combined packet", Vector("Petition", "Declaration", "Exhibits")))
    PdfArrow.recipe("Packet composition", arrow).flatMap(PdfPipeline.check).toOption.get

  def spec = suite("Registered packet arrows")(
    test("typed packet composition round-trips through model JSON and runs on this platform") {
      val plan = checked
      val model = PdfPipelineModel.decode(PdfPipelineModel.encode(plan)).toOption.get
      val canonical = PdfPipelineJson.decode(PdfPipelineJson.encode(plan)).toOption.get
      val result = run(PdfPipeline.run(model, Map("first" -> document, "second" -> document, "third" -> document), services)).toOption.get
      val file = result.outputs.collectFirst { case (_, PdfPipeline.Value.File(file)) => file }.get
      val decoded = run(ByteLimit.fromBytes(file.size).map(limit => PdfDocument.decode(file.bytes.toArray, limit))).toOption.get
      val pages = run(PdfPages.pageRefs(decoded)).toOption.get
      assertTrue(model.recipe == plan.recipe, canonical.recipe == plan.recipe, pages.size == 4,
        result.executed.last.startsWith("pdf-packet-3"), StepRegistry.standard.find("pdf.packet-2", 1).nonEmpty,
        StepRegistry.standard.find("pdf.packet-4", 1).nonEmpty)
    },
    test("independent titles accumulate and packet rewriting cannot recover silently") {
      val errors = StepRegistry.standard.check(PdfPacketSteps.three.call(PdfPacketSteps.Options("", Vector("", "Emoji \ud83d\udcc4"))))
      val recovery = StepRegistry.standard.check(PdfPacketSteps.two.call(PdfPacketSteps.Options(), StepRegistry.OnError.RecoverAndFlag))
      assertTrue(errors.map(_.path).toSet == Set("settings.title", "settings.titles", "settings.titles[0]", "settings.titles[1]"),
        recovery.exists(_.path == "onError"))
    },
    test("an interactive input fails the entire packet before an output is exposed") {
      val form = document.copy(objects = document.objects.map { obj =>
        if obj.index == ObjectRef(1) then obj.copy(value = dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)), "AcroForm" -> dict())) else obj
      })
      val failed = run(PdfPipeline.run(checked, Map("first" -> document, "second" -> form, "third" -> document), services))
      assertTrue(failed.left.toOption.exists(_.message.contains("Interactive PDF forms")))
    }
  )
