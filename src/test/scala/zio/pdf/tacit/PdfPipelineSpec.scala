package zio.pdf.tacit

import zio.blocks.schema.Schema
import zio.test.*
import zio.test.TestAspect.*
import zio.pdf.arrow.*

import java.nio.file.Files

object PdfPipelineSpec extends ZIOSpecDefault {

  private def samplePdfBytes: Array[Byte] =
    Files.readAllBytes(
      java.nio.file.Paths.get("src/test/resources/empty-kids.pdf")
    )

  def spec: Spec[Any, Any] = suite("PdfPipeline (tacit)")(
    test("planFused exposes mermaid, edges, and schema JSON") {
      val plan = PdfPipeline.planFused()
      assertTrue(
        plan.mermaid.contains("flowchart"),
        plan.nodes.contains("slice"),
        plan.nodes.contains("hyperfuse-decode-digest"),
        plan.schemaJson.contains("Node"),
        plan.edges.nonEmpty || plan.nodes.size >= 2
      )
    },
    test("planFromGraph matches prebuilt fused plan nodes") {
      val custom = IngestGraph.fusedDecodedFromBytes()
      val plan   = PdfPipeline.planFromGraph("custom", custom)
      assertTrue(
        plan.nodes.contains("slice"),
        plan.nodes.contains("hyperfuse-decode-digest")
      )
    },
    test("runFused ingests sample PDF bytes") {
      val summary = PdfPipeline.runFused(samplePdfBytes)
      assertTrue(summary.decodedCount > 0, summary.digestHex.nonEmpty)
    },
    test("ScanGraph and PipelinePlan are Schema-serializable (inspectable)") {
      val schema = IngestGraph.schemaFused()
      val plan   = PdfPipeline.planFromFlow(PdfFlow.ingestFused())
      val planSchema = summon[Schema[PdfPipeline.PipelinePlan]]
      val planRt     = planSchema.fromDynamicValue(planSchema.toDynamicValue(plan))
      assertTrue(
        PdfPipeline.schemaRoundTrip(schema) == Right(schema),
        planRt.isRight,
        plan.schemaJson.contains("Node"),
        plan.mermaid.contains("flowchart")
      )
    }
  ) @@ sequential
}
