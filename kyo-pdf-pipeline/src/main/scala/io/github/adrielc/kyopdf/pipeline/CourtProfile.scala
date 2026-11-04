package io.github.adrielc.kyopdf.pipeline

enum CourtProfile(val id: String, val label: String, val effectiveDate: String, val source: String):
  case CaliforniaSecondDistrictPetition extends CourtProfile(
    "ca-2dca-petition-2026-07-01-v1",
    "California Second District · Petition",
    "2026-07-01",
    "https://appellate.courts.ca.gov/district-courts/2dca/rules-forms-filing/electronic-filing/electronic-formatting-requirements-and-guidelines"
  )

  def rules: CourtRules.Profile = CourtProfile.californiaRules

object CourtProfile:
  import CourtRules.*
  private val RuleSource = "https://courts.ca.gov/cms/rules/index/eight/rule8_74"
  private def compare(metric: Metric, comparison: Comparison, value: Long): Condition =
    Condition.Compare(Operand.Fact(metric), comparison, Operand.Number(value))

  private lazy val californiaRules: Profile =
    val preset = CaliforniaSecondDistrictPetition
    Profile(1, preset.id, preset.label, "California Second District Court of Appeal", "Petition",
      preset.effectiveDate, preset.source, Vector(
        Rule("file-size", Scope.Document, compare(Metric.FileBytes, Comparison.LessThanOrEqual, 100000000),
          "8.74(a)(5)", RuleSource, "The PDF must be within 100 MB."),
        Rule("searchability", Scope.Page, compare(Metric.TextCharacters, Comparison.GreaterThan, 0),
          "8.74(a)(1)", RuleSource, "Check that this page is searchable and readable.", Status.NeedsReview),
        Rule("pagination", Scope.Page, compare(Metric.PageLabelMatches, Comparison.Equal, 1),
          "8.74(a)(2)", RuleSource, "The PDF page label must match the page counter."),
        Rule("visible-pagination", Scope.Page, compare(Metric.VisibleNumberMatches, Comparison.Equal, 1),
          "8.74(a)(2)", RuleSource, "Confirm the visible Arabic page number matches the page counter.", Status.NeedsReview),
        Rule("bookmark-presence", Scope.Document, compare(Metric.BookmarkCount, Comparison.GreaterThan, 0),
          "8.74(a)(3)", RuleSource, "This petition requires electronic bookmarks."),
        Rule("bookmarks", Scope.Bookmark, compare(Metric.DestinationResolves, Comparison.Equal, 1),
          "8.74(a)(3)", RuleSource, "The bookmark must resolve to a page in this PDF."),
        Rule("exhibit-destinations", Scope.Bookmark, compare(Metric.ExhibitDestinationMatches, Comparison.Equal, 1),
          "8.74(a)(3)", RuleSource, "Confirm the exhibit bookmark opens the matching exhibit.", Status.NeedsReview),
        Rule("manual-checks", Scope.Document, Condition.Review,
          "8.74(a)(3), (a)(4), (b)", RuleSource,
          "Confirm bookmark coverage and zoom, cover stamp space, formatting, text quality, and sensitive information.", Status.NeedsReview)
      ))
