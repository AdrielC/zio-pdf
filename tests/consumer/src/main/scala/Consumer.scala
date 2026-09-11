import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.pdf.PdfObjectScanner
import zio.pdf.arrow.PipelineFlow
import zio.pdf.pipe.Pipe

object Consumer:
  def main(args: Array[String]): Unit =
    val input = Chunk.fromArray(
      "%PDF-1.7\n1 0 obj\n<</Length 6>>\nstream\nendobj\nendstream\nendobj\n"
        .getBytes(StandardCharsets.US_ASCII)
    )
    val result = PdfObjectScanner.step(
      PdfObjectScanner.Config(maxCarryBytes = 1024),
      PdfObjectScanner.initial,
      input,
    )
    assert(result.exists { case (_, boundaries) => boundaries.length == 1 })
    val flow = PipelineFlow.init("consumer").input[Int]("input").pipe("double")(Pipe(_ * 2)).build
    assert(flow.runLocal(21) == 42)
    assert(flow.renderMermaid.contains("double"))
