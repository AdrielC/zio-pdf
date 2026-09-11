package zio.pdf.arrow

import zio.test.*
import volga.syntax.smc.V
import volga.free.Nat

object SmcWireChainSpec extends ZIOSpecDefault {
  type V1 = V[Nat.`1`]

  def spec = suite("SmcWireChain")(
    test("produce then consume as final statement") {
      val exp = PipelineFlow.prop.of2 { (s: V1, e: V1) =>
        ArrowSyntax.node("a", 1, 0)(s)
        ArrowSyntax.node("b", 1, 0)(e)
        val merge = ArrowSyntax.node("|||", 0, 1)()
        ArrowSyntax.node("export", 1, 0)(merge)
      }
      val (_, edges) = GraphRender.wiring(exp, ("left", "right"))
      assertTrue(
        edges.contains("left" -> "a"),
        edges.contains("right" -> "b"),
        edges.exists(_._2 == "export")
      )
    },
    test("produce, consume mid-block, return fresh source") {
      val exp = PipelineFlow.prop.of2 { (s: V1, e: V1) =>
        ArrowSyntax.node("a", 1, 0)(s)
        ArrowSyntax.node("b", 1, 0)(e)
        val merge = ArrowSyntax.node("|||", 0, 1)()
        ArrowSyntax.node("export", 1, 0)(merge)
        ArrowSyntax.node("out", 0, 1)()
      }
      val (outs, edges) = GraphRender.wiring(exp, ("left", "right"))
      assertTrue(
        edges.contains("left" -> "a"),
        edges.exists(_._2 == "export"),
        outs.contains("out")
      )
    },
    test("join attestation chain to export sink") {
      val formats = PipelineFlow.wiring("legal-join", "left", "right") {
        PipelineFlow.prop.of2 { (s: V1, e: V1) =>
          ArrowSyntax.node("structure-attest", 1, 0)(s)
          ArrowSyntax.node("entity-attest", 1, 0)(e)
          val merge = ArrowSyntax.node(GraphSummary.MergeOp, 0, 1)()
          ArrowSyntax.node("legal-export-bundle", 1, 0)(merge)
        }
      }
      assertTrue(
        formats.wiring._2.contains("left" -> "structure-attest"),
        formats.wiring._2.contains("right" -> "entity-attest"),
        formats.wiring._2.exists(_._2 == "legal-export-bundle")
      )
    }
  )
}
