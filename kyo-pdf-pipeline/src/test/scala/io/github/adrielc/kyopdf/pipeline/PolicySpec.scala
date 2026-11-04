package io.github.adrielc.kyopdf.pipeline

import zio.test.*
import zio.blocks.schema.json.{Json, JsonSchema}
import Policy.*

object PolicySpec extends ZIOSpecDefault:
  private def compare(metric: Metric, comparison: Comparison, limit: Long): Condition =
    Condition.Compare(Operand.Fact(metric), comparison, Operand.Number(limit))
  private val rule = Rule("size", Scope.Document, compare(Metric.FileBytes, Comparison.LessThanOrEqual, 5000),
    "Example rule 1", "Synthetic test rule", "PDF size must fit.")
  private def profile(rules: Rule*): Profile =
    Profile(1, "synthetic-policy-v1", "Synthetic profile", "Synthetic organization", "Report", "2026-11-03", "Synthetic test rules", rules.toVector)
  private def checked(value: Profile): Checked = compile(value).fold(e => throw AssertionError(e.toString), identity)
  private def result(plan: Checked, values: Map[Metric, Long]): Vector[String] =
    plan.run(Vector(Observation(Scope.Document, values))).map(_.status)

  def spec = suite("Serializable policies")(
    test("a saved profile compiles to SchemaExpr and changes results when its data changes") {
      val original = profile(rule)
      val loaded = decode(encode(original)).fold(e => throw AssertionError(e.toString), identity)
      val stricter = checked(profile(rule.copy(condition = compare(Metric.FileBytes, Comparison.LessThanOrEqual, 100))))
      assertTrue(
        loaded.profile == original,
        encode(loaded.profile) == encode(original),
        result(loaded, Map(Metric.FileBytes -> 200)) == Vector("pass"),
        result(stricter, Map(Metric.FileBytes -> 200)) == Vector("fix-required"),
        result(loaded, Map.empty) == Vector("needs-review")
      )
    },
    test("earlier profiles and saved pipelines load using generic metadata") {
      import PdfArrow.*
      val original = profile(rule)
      val legacy = encode(original).replace("\"authority\"", "\"court\"").replace("\"documentType\"", "\"filingType\"")
      val plan = checked(original)
      val recipe = PdfArrow.recipe("Legacy profile", input("body") >>> write >>> preflight(original))
        .flatMap(PdfPipeline.check).fold(e => throw AssertionError(e.toString), identity)
      val saved = PdfPipelineJson.encode(recipe).replace("\"authority\"", "\"court\"")
        .replace("\"documentType\"", "\"filingType\"")
      val mixed = legacy.dropRight(1) + ""","authority":"Different"}"""
      assertTrue(
        decode(legacy).toOption.get.profile == original,
        result(decode(legacy).toOption.get, Map(Metric.FileBytes -> 200)) == result(plan, Map(Metric.FileBytes -> 200)),
        PdfPipelineJson.decode(saved).toOption.get.recipe == recipe.recipe,
        decode(mixed).isLeft
      )
    },
    test("logic preserves unknowns and independent findings while resolving definite outcomes") {
      val small = rule.condition
      val unknown = Condition.Review
      val rules = Vector(
        rule.copy(id = "all", condition = Condition.All(Vector(small, unknown))),
        rule.copy(id = "any", condition = Condition.Any(Vector(small, unknown))),
        rule.copy(id = "not", condition = Condition.Not(small)),
        rule.copy(id = "manual", condition = unknown)
      )
      val plan = checked(profile(rules*))
      assertTrue(
        result(plan, Map(Metric.FileBytes -> 200)) == Vector("needs-review", "pass", "fix-required", "needs-review"),
        result(plan, Map(Metric.FileBytes -> 6000)) == Vector("fix-required", "needs-review", "pass", "needs-review")
      )
    },
    test("conditions apply per observation and missing scope evidence needs review") {
      val pageRule = rule.copy(id = "text", scope = Scope.Page,
        condition = compare(Metric.TextCharacters, Comparison.GreaterThan, 0),
        when = Some(compare(Metric.PageNumber, Comparison.GreaterThan, 1)))
      val plan = checked(profile(pageRule))
      val observations = Vector(
        Observation(Scope.Page, Map(Metric.PageNumber -> 1, Metric.TextCharacters -> 0), Some(1)),
        Observation(Scope.Page, Map(Metric.PageNumber -> 2, Metric.TextCharacters -> 0), Some(2)),
        Observation(Scope.Page, Map(Metric.TextCharacters -> 20), Some(3))
      )
      val found = plan.run(observations)
      assertTrue(found.map(_.page) == Vector(Some(2), Some(3)),
        found.map(_.status) == Vector("fix-required", "needs-review"),
        plan.run(Vector.empty).map(_.status) == Vector("needs-review"))
    },
    test("independent profile failures accumulate with bounded recursive conditions") {
      val bad = profile(rule.copy(scope = Scope.Page, onFailure = Status.Pass)).copy(version = 9, effectiveDate = "yesterday")
      val issues = compile(bad).left.toOption.get
      val deep = (1 to 10).foldLeft(rule.condition)((condition, _) => Condition.Not(condition))
      assertTrue(
        issues.exists(_.path == "profile.version"),
        issues.exists(_.path == "profile.effectiveDate"),
        issues.exists(_.path == "profile.rules[0].onFailure"),
        issues.exists(_.code == PdfPipeline.Code.Type),
        compile(profile(rule.copy(condition = deep))).isLeft,
        decode("""{"version":1,"version":1}""").isLeft,
        decode("[" * 100 + "0" + "]" * 100).isLeft,
        decode(encode(profile(rule)).dropRight(1) + ""","extra":true}""").isLeft
      )
    },
    test("custom rules remain data through arrow composition and both JSON formats") {
      import PdfArrow.*
      val arrow = input("body") >>> write >>> (id[File] &&& preflight(profile(rule)))
      val recipe = PdfArrow.recipe("Synthetic organization", arrow).fold(e => throw AssertionError(e.toString), identity)
      val plan = PdfPipeline.check(recipe).fold(e => throw AssertionError(e.toString), identity)
      val model = PdfPipelineModel.encode(plan)
      val schema = JsonSchema.fromJson(Policy.jsonSchema).fold(e => throw AssertionError(e.toString), identity)
      assertTrue(
        Json.parseUnsafe(encode(profile(rule))).conforms(schema),
        Json.parseUnsafe(encode(CourtProfile.CaliforniaSecondDistrictPetition.rules)).conforms(schema),
        PdfPipelineJson.decode(PdfPipelineJson.encode(plan)).toOption.get.recipe == recipe,
        PdfPipelineModel.decode(model).toOption.get.recipe == recipe,
        model.contains("preflight-rules"),
        Policy.jsonSchema.print.contains("condition")
      )
    }
  )
