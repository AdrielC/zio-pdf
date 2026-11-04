package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import kyo.*
import zio.test.*
import scala.compiletime.testing.typeCheckErrors

object RuntimeDataSpec extends ZIOSpecDefault:
  import PdfPipeline.*
  private val bytes = "%PDF-1.7\n%%EOF\n".getBytes(java.nio.charset.StandardCharsets.US_ASCII).toVector
  private def result[A](program: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private val document =
    import PdfValue.*
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "Resources" -> dict(),
        "MediaBox" -> Array(Vector(Number(0), Number(0), Number(300), Number(400)))))
    ), dict("Root" -> Ref(ObjectRef(1))))

  def spec = suite("Checked pipeline runtime data")(
    test("PDF payloads enforce their header and supported length without copying") {
      val file = PdfFile.checked(bytes).toOption.get
      assertTrue(file.bytes eq bytes, file.size == bytes.length.toLong,
        PdfFile.checked(Vector.empty).isLeft, PdfFile.checked("not a pdf".getBytes.toVector).isLeft,
        PdfFile.checked("%PDF-no!".getBytes.toVector).isLeft,
        PdfFile.checkLength(PdfFile.MaxBytes.toLong).isRight,
        PdfFile.checkLength(PdfFile.MaxBytes.toLong + 1) == Left(DataIssue.PdfTooLarge(PdfFile.MaxBytes.toLong + 1, PdfFile.MaxBytes)))
    },
    test("invalid report structure and independent profile IDs accumulate") {
      val errors = VerificationResult.fromLegacy("not-json", Map("other" -> Verdict.Passed, "wrong" -> Verdict.Failed)).left.toOption.get
      assertTrue(errors.head.isInstanceOf[DataIssue.InvalidJson],
        errors.tail == Vector(DataIssue.UnknownProfile("other"), DataIssue.UnknownProfile("wrong")),
        ReportJson.parse("[]") == Left(Vector(DataIssue.ObjectRequired)),
        ReportJson.parse("{\"status\":true,\"status\":false}").isLeft,
        PdfAProfile.parse("3b") == Right(PdfAProfile.PdfA3b))
    },
    test("report size, nesting and node budgets are enforced") {
      val deep = "[" * (ReportJson.MaxDepth + 1) + "0" + "]" * (ReportJson.MaxDepth + 1)
      val many = "{\"values\":[" + Vector.fill(ReportJson.MaxNodes)("0").mkString(",") + "]}"
      assertTrue(ReportJson.parse(" " * (ReportJson.MaxChars + 1)).isLeft,
        ReportJson.parse(deep) == Left(Vector(DataIssue.ReportTooDeep(ReportJson.MaxDepth))),
        ReportJson.parse(many).left.toOption.get.contains(DataIssue.TooManyJsonNodes(ReportJson.MaxNodes)),
        ReportJson.parse("""{"text":"[[[[[\""}""").isRight)
    },
    test("payload constructors and untyped Value construction are inaccessible") {
      assertTrue(typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.*; new PdfFile(Vector.empty)""").nonEmpty,
        typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.*; PdfPipeline.Value.File(Vector.empty)""").nonEmpty,
        typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.*; PdfPipeline.Value.Receipt("{}")""").nonEmpty)
    },
    test("bounded writing rejects sparse xrefs and huge decimal expansions before rendering") {
      val limit = result(ByteLimit.fromBytes(4096)).toOption.get
      val sparse = document.copy(objects = document.objects :+ PdfObject(ObjectRef(Int.MaxValue), PdfValue.Null))
      val expanded = document.copy(objects = document.objects :+ PdfObject(ObjectRef(4), PdfValue.Number(BigDecimal("1e2147483647"))))
      val overflowing = document.copy(objects = document.objects :+ PdfObject(ObjectRef(4), PdfValue.Number(BigDecimal("100e2147483647"))))
      val original = result(PdfWriter.write(document)).toOption.get
      val exact = result(ByteLimit.fromBytes(original.length.toLong)).toOption.get
      val short = result(ByteLimit.fromBytes(original.length.toLong - 1)).toOption.get
      assertTrue(result(PdfWriter.write(sparse, limit)).left.toOption.get.isInstanceOf[PdfError.TooLarge],
        result(PdfWriter.write(expanded, limit)).left.toOption.get.isInstanceOf[PdfError.TooLarge],
        result(PdfWriter.write(overflowing, limit)).left.toOption.get.isInstanceOf[PdfError.TooLarge],
        result(PdfWriter.write(document, exact)) == Right(original), result(PdfWriter.write(document, short)).isLeft)
    },
    test("validation receipts retain their exact subject and reject unrequested verdicts") {
      import PdfArrow.*
      val arrow = input("body") >>> write >>> (id[File] &&& validate("3b"))
      val plan = PdfArrow.recipe("Typed results", arrow).flatMap(PdfPipeline.check).toOption.get
      class TypedService(extra: Boolean) extends Services:
        def stamp(d: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) = d
        def number(d: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) = d
        override def verify(file: PdfFile, profiles: Vector[PdfAProfile]): VerificationResult < (Abort[PdfError] & Sync) =
          Predef.assert(profiles == Vector(PdfAProfile.PdfA3b))
          VerificationResult(ReportJson.parse("{\"status\":\"passed\"}").toOption.get,
            Map((if extra then PdfAProfile.PdfA3u else PdfAProfile.PdfA3b) -> Verdict.Passed))
      val run = result(PdfPipeline.run(plan, Map("body" -> document), new TypedService(false))).toOption.get
      val file = run.outputs.collectFirst { case (_, Value.File(value)) => value }.get
      val receipt = run.outputs.collectFirst { case (_, Value.Receipt(value: PdfReceipt.Validation)) => value }.get
      val bad = result(PdfPipeline.run(plan, Map("body" -> document), new TypedService(true)))
      assertTrue(receipt.subject eq file, receipt.result.verdicts == Map(PdfAProfile.PdfA3b -> Verdict.Passed),
        bad.left.toOption.exists(_.message.contains("UnrequestedProfile")))
    },
    test("receipt subjects are counted once when shared and remain counted without a file output") {
      import PdfArrow.*
      val report = ReportJson.parse("{\"note\":\"" + "x" * 300 + "\"}").toOption.get
      val small = ReportJson.parse("{}").toOption.get
      class Inspector(metadata: ReportJson) extends Services:
        def stamp(d: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) = d
        def number(d: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) = d
        override def verify(file: PdfFile, profiles: Vector[PdfAProfile]): VerificationResult < (Abort[PdfError] & Sync) =
          VerificationResult(metadata, Map(PdfAProfile.PdfA3b -> Verdict.Passed))
      val file = result(PdfWriter.write(document)).toOption.get
      val shared = PdfArrow.recipe("Shared subject", input("body") >>> write >>> (id[File] &&& validate("3b")))
        .flatMap(PdfPipeline.check).toOption.get
      val onlyReceipt = PdfArrow.recipe("Retained subject", input("body") >>> write >>> validate("3b"))
        .flatMap(PdfPipeline.check).toOption.get
      val deduplicated = result(PdfPipeline.run(shared, Map("body" -> document), new Inspector(small),
        Limits(maxLiveBytes = file.length.toLong * 2 - 1)))
      val retained = result(PdfPipeline.run(onlyReceipt, Map("body" -> document), new Inspector(report),
        Limits(maxLiveBytes = file.length.toLong + report.countedBytes - 1)))
      assertTrue(deduplicated.isRight, retained.left.toOption.get.isInstanceOf[PdfError.RetentionExceeded])
    },
    test("inspection JSON cannot claim an archival verdict") {
      import PdfArrow.*
      val condition = verified("3b").on(keepRight[File, Receipt])
      val reject = stop[(File, Receipt), File]("Inspection was treated as archival verification")
      val arrow = input("body") >>> write >>> (id[File] &&& preflight(CourtProfile.CaliforniaSecondDistrictPetition)) >>>
        choose(condition)(reject, reject, keepLeft[File, Receipt])
      val plan = PdfArrow.recipe("Untrusted inspection claims", arrow).flatMap(PdfPipeline.check).toOption.get
      val services = new Services:
        def stamp(d: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) = d
        def number(d: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) = d
        override def inspect(file: PdfFile, profile: String): ReportJson < (Abort[PdfError] & Sync) =
          ReportJson.parse("""{"status":"passed","verdicts":{"3b":"Passed"}}""").toOption.get
      val run = result(PdfPipeline.run(plan, Map("body" -> document), services)).toOption.get
      assertTrue(run.decisions.map(_.outcome) == Vector(Outcome.Unknown))
    }
  )
