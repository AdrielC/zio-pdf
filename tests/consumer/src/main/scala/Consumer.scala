import java.nio.charset.StandardCharsets

import zio.Chunk
import zio.pdf.PdfObjectScanner
import zio.pdf.arrow.{ArrowSyntax, GraphRender}
import volga.SymmetricCat
import volga.free.{FreeProp, Nat, PropOb}
import volga.syntax.smc
import volga.syntax.smc.V

type ConsumerLabel[A, B] = String
type ConsumerDiag[A, B] = FreeProp[ConsumerLabel, A, B]

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
    given SymmetricCat[ConsumerDiag, PropOb] = FreeProp.propCat[ConsumerLabel]
    val syntax = smc.syntax[ConsumerDiag, PropOb, Nat.Plus]
    val mark = ArrowSyntax.node("mark", 1, 1)
    val graph = syntax.of1((input: V[Nat.`1`]) => mark(input))
    val (outputs, edges) = GraphRender.wiring(graph, Tuple1("source"))
    assert(outputs == Vector("mark"))
    assert(edges == Vector("source" -> "mark"))
