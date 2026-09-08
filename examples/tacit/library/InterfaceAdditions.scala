// Paste into tacit/library/Interface.scala (package tacit.library)

import zio.pdf.arrow.PipelineFlow
import zio.pdf.tacit.PdfPipeline

/** Capability for PDF pipeline inspection and ingest within a filesystem scope. */
@assumeSafe
abstract class PdfPipelineCapability private[library] () extends caps.SharedCapability

// Add to trait Interface:

def requestPdfPipeline[T](op: PdfPipelineCapability^ ?=> T)(using IOCapability): T

def pdfPlanFused(name: String = "ingest-fused")(using PdfPipelineCapability): PdfPipeline.PipelinePlan

def pdfPlanStaged(name: String = "ingest-staged")(using PdfPipelineCapability): PdfPipeline.PipelinePlan

def pdfPlanFlow[A, B](flow: PipelineFlow.Flow[A, B])(using PdfPipelineCapability): PdfPipeline.PipelinePlan

def pdfRunFused(bytes: Array[Byte])(using PdfPipelineCapability): PdfPipeline.IngestSummary

def pdfRunStaged(bytes: Array[Byte])(using PdfPipelineCapability): PdfPipeline.IngestSummary
