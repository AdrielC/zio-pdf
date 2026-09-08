package zio.pdf.arrow

import zio.test.*
import zio.pdf.pipe.Pipe

/** Prints a maximally wired spine graph — `testOnly zio.pdf.arrow.ComplexMermaidDemo`. */
object ComplexMermaidDemo extends ZIOSpecDefault {

  private type B = Array[Byte]
  private def id: Pipe[B, B] = Pipe(identity)

  /** 12-node / 12-edge agent ingest DAG (nested fan-out after sequential prefix). */
  def complexSpine: PipelineSpine.Spine[B, (B, (B, B))] = {
    val validate = PipelineSpine.node("validate-pdf", 1, 1)(id)
    val slice    = PipelineSpine.node("slice", 1, 1)(id)

    val objectArm =
      PipelineSpine.node("xref-scan", 1, 1)(id) >>>
        PipelineSpine.node("decode-objects", 1, 1)(id) >>>
        PipelineSpine.node("dedupe-filter", 1, 1)(id)

    val streamArm =
      PipelineSpine.node("boundary-scan", 1, 1)(id) >>>
        PipelineSpine.node("hyperfuse-decode", 1, 1)(id) >>>
        PipelineSpine.node("evidence-digest", 1, 1)(id)

    val cryptoArm =
      PipelineSpine.node("crypto-detect", 1, 1)(id) >>>
        PipelineSpine.node("decrypt-stream", 1, 1)(id) >>>
        PipelineSpine.node("acl-check", 1, 1)(id)

    validate >>> slice >>> (objectArm &&& (streamArm &&& cryptoArm))
  }

  /** PipelineFlow version: prefix chain `andThen` nested `zip`. */
  def complexFlow: PipelineFlow.Flow[B, (B, (B, B))] = {
    val prefix = PipelineFlow
      .init("prefix")
      .input[B]("bytes")
      .pipe("validate-pdf")(id)
      .pipe("slice")(id)
      .build

    def arm(name: String, n1: String, n2: String, n3: String) =
      PipelineFlow.init(name).input[B]("bytes").pipe(n1)(id).pipe(n2)(id).pipe(n3)(id).build

    val objects = arm("objects", "xref-scan", "decode-objects", "dedupe-filter")
    val streams = arm("streams", "boundary-scan", "hyperfuse-decode", "evidence-digest")
    val crypto  = arm("crypto", "crypto-detect", "decrypt-stream", "acl-check")

    PipelineFlow.andThen(prefix, PipelineFlow.zip(objects, PipelineFlow.zip(streams, crypto)))
  }

  def spec: Spec[Any, Any] = suite("ComplexMermaidDemo")(
    test("print complex agent ingest mermaid (spine AST)") {
      val formats = PipelineSpine.render("agent-ingest-complex", complexSpine, "bytes")
      println("\n===== SPINE MERMAID (12 nodes, nested fan-out) =====\n")
      println(formats.mermaid)
      assertTrue(
        formats.wiring._2.size >= 11,
        formats.mermaid.contains("slice --> xref-scan"),
        formats.mermaid.contains("slice --> boundary-scan"),
        formats.mermaid.contains("slice --> crypto-detect"),
        !formats.mermaid.contains("bytes --> xref-scan"),
        !formats.mermaid.contains("⟨in⟩")
      )
    },
    test("print complex flow mermaid (PipelineFlow DSL)") {
      val flow = complexFlow
      println("\n===== FLOW MERMAID =====\n")
      println(flow.renderMermaid)
      assertTrue(
        flow.renderMermaid.contains("validate-pdf"),
        flow.renderMermaid.contains("hyperfuse-decode"),
        ScanGraph.nodeNames(flow.schema).size >= 11
      )
    },
    test("print real fused ingest mermaid") {
      val m = IngestGraph.renderFused("production-ingest").mermaid
      println("\n===== REAL INGEST (fused HyperFuse) =====\n")
      println(m)
      assertTrue(m.contains("slice"), m.contains("hyperfuse-decode-digest"))
    }
  )
}
