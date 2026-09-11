package zio.pdf.arrow

import zio.test.*
import zio.pdf.pipe.Pipe
import volga.free.Nat
import volga.syntax.smc.V

object PipelineFlowSpec extends ZIOSpecDefault {

  type V1 = V[Nat.`1`]

  def spec: Spec[Any, Any] = suite("PipelineFlow")(
    test("fluent pipe chain matches IngestGraph fused nodes") {
      val flow = PipelineFlow
        .init("agent-ingest")
        .input[Array[Byte]]("bytes")
        .pipe("slice")(zio.pdf.pipe.DecodePipeline.sliceWhole)
        .pipe("hyperfuse-decode-digest") {
          Pipe(slice => zio.pdf.pipe.IngestPipeline.fusedDecodeAndDigest(slice, zio.pdf.pipe.FusedDecode.Cfg()))
        }
        .build

      val names = ScanGraph.nodeNames(flow.schema)
      assertTrue(
        names.contains("slice"),
        names.contains("hyperfuse-decode-digest"),
        flow.renderMermaid.contains("slice")
      )
    },
    test("andThen sequences flows") {
      val inc = PipelineFlow.init("inc").input[Int]("x").pipe("plus-one")(Pipe(_ + 1)).build
      val dbl = PipelineFlow.init("dbl").input[Int]("x").pipe("double")(Pipe(_ * 2)).build
      val both = PipelineFlow.andThen(inc, dbl)
      assertTrue(both.runLocal(3) == 8)
    },
    test("zip runs parallel branches with honest fan-out graph") {
      val left  = PipelineFlow.init("l").input[Int]("x").pipe("a")(Pipe(_ + 1)).build
      val right = PipelineFlow.init("r").input[Int]("x").pipe("b")(Pipe(_ * 10)).build
      val both  = PipelineFlow.zip(left, right)
      val names = ScanGraph.nodeNames(both.schema)
      assertTrue(
        both.runLocal(2) == (3, 20),
        names.contains("a"),
        names.contains("b"),
        !names.contains("zip:l+r")
      )
    },
    test("dispatch first-match branch") {
      val flow = PipelineFlow
        .dispatch[Int, String]("approval")
        .when(_ > 1000, "review")(Pipe(_ => "needs review"))
        .when(_ > 100, "auto")(Pipe(_ => "auto-approved"))
        .otherwise("default")(Pipe(_ => "instant"))

      assertTrue(
        flow.runLocal(2000) == "needs review",
        flow.runLocal(500) == "auto-approved",
        flow.runLocal(10) == "instant"
      )
    },
    test("wiring discovers arity from of1 block (no upfront Nat)") {
      val fuse = ArrowSyntax.node("fuse", 1, 0)
      val formats = PipelineFlow.wiring("wiring-demo", "bytes") {
        PipelineFlow.prop.of1((v: V1) => fuse(v))
      }
      assertTrue(formats.mermaid.contains("bytes --> fuse"))
    },
    test("wiring of0 needs no boundary labels") {
      val a = ArrowSyntax.node("a", 0, 1)
      val c = ArrowSyntax.node("c", 1, 3)
      val b = ArrowSyntax.node("b", 1, 0)
      val formats = PipelineFlow.wiring("of0-demo") {
        PipelineFlow.prop.of0 {
          val x         = a()
          val (u, v, w) = c(x)
          b(v)
          (w, u)
        }
      }
      assertTrue(
        formats.mermaid.contains("a --> c"),
        formats.mermaid.contains("c --> b")
      )
    },
  )
}
