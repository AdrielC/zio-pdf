package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*

object PipelineExamples:
  def pages: Recipe = PipelineWiring.conditionalExample

  val validation: Recipe = Recipe(2, "Route by PDF-A verdict", Vector(
    Node("body", Op.Input("body")),
    Node("original", Op.Write, Vector("body")),
    Node("before", Op.Validate(Vector("3b")), Vector("original")),
    Node("review", Op.Stamp("REVIEW REQUIRED"), Vector("body")),
    Node("review-pdf", Op.Write, Vector("review")),
    Node("unable", Op.Stop("PDF/A-3b was not checked successfully; inspect the validation evidence", Kind.File)),
    Node("route", Op.Choose(Predicate.Validation("3b"), Kind.File), Vector("before", "original", "review-pdf", "unable")),
    Node("after", Op.Validate(Vector("3b")), Vector("route"))
  ), Vector("route", "before", "after"))
