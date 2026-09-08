// tacit/library/impl/PdfPipelineOps.scala
package tacit.library

import language.experimental.captureChecking
import caps.*
import zio.pdf.tacit.PdfPipeline

object PdfPipelineOps:

  def pdfPlanFused(name: String = "ingest-fused")(using PdfPipelineCapability): PdfPipeline.PipelinePlan =
    PdfPipeline.planFused(name)

  def pdfPlanStaged(name: String = "ingest-staged")(using PdfPipelineCapability): PdfPipeline.PipelinePlan =
    PdfPipeline.planStaged(name)

  def pdfRunFused(bytes: Array[Byte])(using PdfPipelineCapability): PdfPipeline.IngestSummary =
    PdfPipeline.runFused(bytes)

  def pdfRunStaged(bytes: Array[Byte])(using PdfPipelineCapability): PdfPipeline.IngestSummary =
    PdfPipeline.runStaged(bytes)

// In InterfaceImpl.scala:
//
//   export PdfPipelineOps.*
//
//   def requestPdfPipeline[T](op: PdfPipelineCapability^ ?=> T)(using IOCapability): T =
//     val cap = new PdfPipelineCapability {}
//     op(using cap)
