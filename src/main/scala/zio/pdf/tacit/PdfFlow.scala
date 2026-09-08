package zio.pdf.tacit

import zio.Chunk
import zio.pdf.Decoded
import zio.pdf.arrow.*
import zio.pdf.pipe.FusedDecode.Cfg
import zio.pdf.pipe.IngestPipeline.DecodeDigest

/**
 * PDF ingest flows in Kyo Flow / volga style for TACIT agents.
 *
 * Prefer composing with [[PipelineFlow.init]] directly; these are canonical
 * ingest recipes agents can `.andThen`, inspect with `.renderMermaid`, and run
 * via [[Flow.runLocal]] inside `requestPdfPipeline`.
 */
object PdfFlow {

  type IngestResult = DecodeDigest[Chunk[Decoded]]

  /** Fused HyperFuse ingest — production path. */
  def ingestFused(name: String = "ingest-fused", cfg: Cfg = Cfg()): PipelineFlow.Flow[Array[Byte], IngestResult] =
    PipelineFlow
      .init(name)
      .input[Array[Byte]]("bytes")
      .pipe("slice")(PdfPipes.sliceWhole)
      .pipe("hyperfuse-decode-digest")(PdfPipes.fusedDecodeAndDigest(cfg))
      .build

  /** Staged decode+digest — parity / debug path. */
  def ingestStaged(name: String = "ingest-staged", cfg: Cfg = Cfg()): PipelineFlow.Flow[Array[Byte], IngestResult] =
    PipelineFlow
      .init(name)
      .input[Array[Byte]]("bytes")
      .pipe("slice")(PdfPipes.sliceWhole)
      .pipe("staged-decode-digest")(PdfPipes.stagedDecodeAndDigest(cfg))
      .build

  /** Inspect any [[PipelineFlow.Flow]] as a tacit [[PdfPipeline.PipelinePlan]]. */
  def planOf[A, B](flow: PipelineFlow.Flow[A, B]): PdfPipeline.PipelinePlan =
    PdfPipeline.planFromFlow(flow)

  /** Run a flow and return a log-safe summary. */
  def runIngest(flow: PipelineFlow.Flow[Array[Byte], IngestResult], bytes: Array[Byte]): PdfPipeline.IngestSummary = {
    val result = flow.runLocal(bytes)
    PdfPipeline.IngestSummary(
      decodedCount = result.decoded.size,
      digestHex    = result.digest.map(b => f"$b%02x").mkString
    )
  }
}
