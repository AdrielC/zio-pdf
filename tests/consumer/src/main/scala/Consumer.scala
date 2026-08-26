import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.pdf.PdfObjectScanner

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
