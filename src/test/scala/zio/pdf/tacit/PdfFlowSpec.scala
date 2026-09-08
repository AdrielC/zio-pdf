package zio.pdf.tacit

import zio.pdf.arrow.*
import zio.pdf.pipe.*
import zio.test.*
import zio.test.TestAspect.*

import java.nio.file.Files

object PdfFlowSpec extends ZIOSpecDefault {

  private def samplePdfBytes: Array[Byte] =
    Files.readAllBytes(java.nio.file.Paths.get("src/test/resources/empty-kids.pdf"))

  def spec: Spec[Any, Any] = suite("PdfFlow (tacit agent DSL)")(
    test("ingestFused flow renders kyo-style plan and runs") {
      val flow    = PdfFlow.ingestFused("agent-demo")
      val plan    = PdfFlow.planOf(flow)
      val summary = PdfFlow.runIngest(flow, samplePdfBytes)
      assertTrue(
        plan.mermaid.contains("flowchart"),
        plan.nodes.contains("slice"),
        summary.decodedCount > 0
      )
    },
    test("agent composes custom flow like kyo-flow pricing example") {
      val pricing = PipelineFlow
        .init("pricing")
        .input[Int]("amount")
        .output("quote") {
          Pipe { amount =>
            val tax = amount * 0.08
            (amount, tax, amount + tax)
          }
        }
        .build

      val ingest = PdfFlow.ingestStaged("custom-ingest")

      assertTrue(
        pricing.runLocal(100) == (100, 8.0, 108.0),
        ScanGraph.nodeNames(ingest.schema).contains("staged-decode-digest")
      )
    }
  ) @@ sequential
}
