// TACIT agent script — Kyo Flow / volga-style pipeline composition.
// Paste into TACIT REPL after wiring zio-pdf (see README.md).

import language.experimental.safe
import tacit.library.*
import zio.pdf.arrow.*
import zio.pdf.tacit.*
import zio.pdf.pipe.*

requestFileSystem("/workspace") {
  requestPdfPipeline {

    // ── Kyo Flow style (fluent named steps) ─────────────────────────────
    val ingest = PipelineFlow
      .init("court-ingest")
      .input[Array[Byte]]("bytes")
      .pipe("slice")(DecodePipeline.sliceWhole)
      .pipe("hyperfuse-decode-digest") {
        Pipe(slice => IngestPipeline.fusedDecodeAndDigest(slice, FusedDecode.Cfg()))
      }
      .build

    println(ingest.renderMermaid)

    val bytes   = access("/workspace/src/test/resources/empty-kids.pdf").readBytes()
    val summary = PdfFlow.runIngest(ingest, bytes)
    println(s"decoded=${summary.decodedCount} digest=${summary.digestHex.take(32)}...")

    // ── Or use canonical PDF recipes ────────────────────────────────────
    val fused = PdfFlow.ingestFused()
    println(PdfFlow.planOf(fused).nodes.mkString(" → "))

    // ── Volga SMC wiring (port graph, single edge) ──────────────────────
    import volga.free.Nat
    import volga.syntax.smc.V
    type V1 = V[Nat.`1`]
    val wiring = PipelineFlow.wiring[Nat.`1`, Nat.`0`]("wiring", Tuple1("bytes")) { prop =>
      val fuse = ArrowSyntax.node("hyperfuse-decode-digest", 1, 0)
      prop.of1((v: V1) => fuse(v))
    }
    println(wiring.mermaid)
  }
}
