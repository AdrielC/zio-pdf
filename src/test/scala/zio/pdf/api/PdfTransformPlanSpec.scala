package zio.pdf.api

import zio.pdf.PdfTransform
import zio.test.*

/** Verifies that a library consumer can inspect the public transform plan. */
object PdfTransformPlanSpec extends ZIOSpecDefault {

  private final case class Summary(fontRemaps: Int, tokenizers: Int)

  private object Summary {
    val empty: Summary = Summary(0, 0)

    given PdfTransform.Monoid[Summary] with {
      def empty: Summary = Summary.empty

      def combine(left: Summary, right: Summary): Summary =
        Summary(left.fontRemaps + right.fontRemaps, left.tokenizers + right.tokenizers)
    }
  }

  private val analysis: PdfTransform.Analyzer[Summary] = new PdfTransform.Analyzer[Summary] {
    def apply(op: PdfTransform.Op[PdfTransform.Context, PdfTransform.Context]): Summary =
      op match {
        case PdfTransform.Op.RemapExistingFonts(_, _, _) => Summary(1, 0)
        case PdfTransform.Op.TokenizeText(_, _)          => Summary(0, 1)
      }
  }

  def spec: Spec[Any, Throwable] = suite("PdfTransform public plan")(
    test("allows a downstream analysis interpreter over named leaves") {
      val transform =
        PdfTransform.fonts.replaceExisting("SourceFace", "TargetFace") >>>
          PdfTransform.text.tokenize(PdfTransform.text.Tokenizer.characters)

      assertTrue(transform.program.analyze(analysis) == Summary(1, 1))
    }
  )
}
