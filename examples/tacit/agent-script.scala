// Paste into TACIT REPL (after wiring zio-pdf into the library — see README.md).
//
// Capability-safe PDF ingest: inspect wiring (pure), then read + run (effects).

import language.experimental.safe
import tacit.library.*
import zio.pdf.tacit.PdfPipeline

requestFileSystem("/workspace") {
  requestPdfPipeline {
    // Pure — no bytes read yet; agent can verify the graph before execution
    val plan = pdfPlanFused("court-ingest")
    println("=== ingest graph (mermaid) ===")
    println(plan.mermaid)
    println(s"nodes: ${plan.nodes.mkString(" → ")}")
    println(s"edges: ${plan.edges.mkString(", ")}")

    // Effect — only under granted filesystem root
    val pdf = access("/workspace/src/test/resources/empty-kids.pdf")
    val summary = pdfRunFused(pdf.readBytes())
    println(s"decoded=${summary.decodedCount} digest=${summary.digestHex.take(32)}...")
  }
}
