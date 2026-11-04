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

  val finishBody: PdfArrow[PdfArrow.Document, PdfArrow.Document] =
    import PdfArrow.*
    choose(hasPageNumbers)(
      id[Document],
      number().named("number-body"),
      stop[Document, Document]("Review the body page numbering before assembling this packet").named("review-numbering")
    ).named("body-numbering")

  private val packetFile: PdfArrow[Unit, PdfArrow.File] =
    import PdfArrow.*
    val sources = (input("cover") &&& (input("body") >>> finishBody)) &&& input("tail")
    sources >>> assemble3(catalog = 1) >>> write.named("packet-pdf")

  val packetArrow: PdfArrow[Unit, (PdfArrow.File, PdfArrow.Receipt)] =
    import PdfArrow.*
    packetFile >>> (id[File] &&& validate("3b").named("packet-validation"))

  val packet: Recipe =
    PdfArrow.recipe("Cover body and tail", packetArrow)
      .fold(errors => throw IllegalStateException(errors.mkString(", ")), identity)

  val archivePacket: Recipe =
    PdfArrow.recipe("Archival packet", packetFile >>> PdfArrow.archivePdfA3b)
      .fold(errors => throw IllegalStateException(errors.mkString(", ")), identity)
