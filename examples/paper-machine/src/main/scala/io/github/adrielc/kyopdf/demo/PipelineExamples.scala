package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*
import io.github.adrielc.kyopdf.pipeline.PdfArrow

object PipelineExamples:
  def pages: Recipe = PipelineWiring.conditionalExample

  val validation: Recipe =
    import PdfArrow.*
    type Context = ((Document, File), Receipt)
    val before = write.named("original") >>> (id[File] &&& validate("3b").named("before"))
    val context = input("body") >>> (id[Document] &&& before) >>> assocLeft[Document, File, Receipt]
    val receipt = keepRight[(Document, File), Receipt]
    val condition = verified("3b").on(receipt.named("before-test"))
    val original = (swap[Document, File].first[Receipt] >>> assocRight[File, Document, Receipt] >>> keepLeft[File, (Document, Receipt)]).named("passed-file")
    val review = assocRight[Document, File, Receipt] >>> keepLeft[Document, (File, Receipt)].named("review-source") >>>
      stamp("REVIEW REQUIRED").named("review") >>> write.named("review-pdf")
    val unable = stop[Context, File]("PDF/A-3b was not checked successfully; inspect the validation evidence").named("unable")
    val route = choose(condition)(original, review, unable).named("route")
    val after = id[File] &&& validate("3b").named("after")
    val outputs = (route &&& receipt.named("before-output")) >>> (after >< id[Receipt]) >>>
      assocRight[File, Receipt, Receipt] >>> swap[Receipt, Receipt].second[File]
    PdfArrow.recipe("Route by PDF-A verdict", context >>> outputs).fold(errors => throw IllegalStateException(errors.mkString(", ")), identity)
