// tacit/library/impl/PdfPipelineOps.scala
package tacit.library

import language.experimental.captureChecking
import caps.*
import zio.pdf.arrow.PipelineFlow
import zio.pdf.tacit.{PdfFlow, PdfPipeline}

object PdfPipelineOps:

  def pdfPlanFused(name: String = "ingest-fused")(using PdfPipelineCapability): PdfPipeline.PipelinePlan =
    PdfFlow.planOf(PdfFlow.ingestFused(name))

  def pdfPlanStaged(name: String = "ingest-staged")(using PdfPipelineCapability): PdfPipeline.PipelinePlan =
    PdfFlow.planOf(PdfFlow.ingestStaged(name))

  def pdfPlanFlow[A, B](flow: PipelineFlow.Flow[A, B])(using PdfPipelineCapability): PdfPipeline.PipelinePlan =
    PdfFlow.planOf(flow)

  def pdfRunFused(bytes: Array[Byte])(using PdfPipelineCapability): PdfPipeline.IngestSummary =
    PdfFlow.runIngest(PdfFlow.ingestFused(), bytes)

  def pdfRunStaged(bytes: Array[Byte])(using PdfPipelineCapability): PdfPipeline.IngestSummary =
    PdfFlow.runIngest(PdfFlow.ingestStaged(), bytes)

  def pdfRunFlow(bytes: Array[Byte], flow: PipelineFlow.Flow[Array[Byte], PdfFlow.IngestResult])(using
      PdfPipelineCapability
  ): PdfPipeline.IngestSummary =
    PdfFlow.runIngest(flow, bytes)

// In InterfaceImpl.scala:
//
//   export PdfPipelineOps.*
//
//   def requestPdfPipeline[T](op: PdfPipelineCapability^ ?=> T)(using IOCapability): T =
//     val cap = new PdfPipelineCapability {}
//     op(using cap)
