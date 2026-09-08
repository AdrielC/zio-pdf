// TACIT agent script — Kyo Flow / volga-style pipeline composition.
// Verified via PdfPipelineIntegrationSuite in workspace/tacit.

import language.experimental.safe
import tacit.library.*
import zio.pdf.arrow.*
import zio.pdf.tacit.*

requestFileSystem("/workspace") {
  requestPdfPipeline {

    // Kyo Flow style — fluent named steps
    val flow = PipelineFlow
      .init("court-ingest")
      .input[Array[Byte]]("bytes")
      .pipe("slice")(PdfPipes.sliceWhole)
      .pipe("hyperfuse-decode-digest")(PdfPipes.fusedDecodeAndDigest())
      .build

    println(flow.renderMermaid)

    val bytes   = access("/workspace/src/test/resources/empty-kids.pdf").readBytes()
    val summary = PdfFlow.runIngest(flow, bytes)
    println(s"decoded=${summary.decodedCount} digest=${summary.digestHex.take(32)}...")

    // Canonical recipe shortcut
    val fused = PdfFlow.ingestFused()
    println(PdfFlow.planOf(fused).nodes.mkString(" → "))
  }
}
