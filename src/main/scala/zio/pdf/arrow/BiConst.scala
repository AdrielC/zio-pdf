package zio.pdf.arrow

import ArrowObjects.*

/** Constant bifunctor — carries an invariant summary `M` (graph, schema, etc.). */
opaque type BiConst[M, +A, +B] = M

object BiConst {

  def apply[M, A, B](m: M): BiConst[M, A, B] = m

  def getConst[M, A, B](c: BiConst[M, A, B]): M = c

  type ~|[F[_, _], M] = [A, B] => F[A, B] => M

  /** Category interpreting labeled arrows as [[GraphSummary]] fragments. */
  given graphSummaryCategory: Category[[A, B] =>> BiConst[GraphSummary, A, B]] with {
    def id[A]: BiConst[GraphSummary, A, A] =
      BiConst(GraphSummary.empty)

    def compose[A, B, C](
        f: BiConst[GraphSummary, B, C],
        g: BiConst[GraphSummary, A, B]
    ): BiConst[GraphSummary, A, C] =
      BiConst(GraphSummary.seq(BiConst.getConst(g), BiConst.getConst(f)))

    def split[A: Ob, B: Ob, C: Ob, D: Ob](
        f: BiConst[GraphSummary, A, B],
        g: BiConst[GraphSummary, C, D]
    ): BiConst[GraphSummary, Prod[A, C], Prod[B, D]] =
      BiConst(GraphSummary.par(BiConst.getConst(f), BiConst.getConst(g)))

    def fanout[A: Ob, B: Ob, C: Ob](
        f: BiConst[GraphSummary, A, B],
        g: BiConst[GraphSummary, A, C]
    ): BiConst[GraphSummary, A, Prod[B, C]] = {
      val lf = BiConst.getConst(f)
      val rg = BiConst.getConst(g)
      BiConst(GraphSummary.fanout(lf, rg, fork = GraphSummary.InputPort))
    }

    def merge[A: Ob, B: Ob, C: Ob](
        f: BiConst[GraphSummary, A, C],
        g: BiConst[GraphSummary, B, C]
    ): BiConst[GraphSummary, Sum[A, B], C] =
      BiConst(GraphSummary.par(BiConst.getConst(f), BiConst.getConst(g)))

    def choose[A: Ob, B: Ob, C: Ob, D: Ob](
        f: BiConst[GraphSummary, A, C],
        g: BiConst[GraphSummary, B, D]
    ): BiConst[GraphSummary, Sum[A, B], Sum[C, D]] =
      BiConst(GraphSummary.par(BiConst.getConst(f), BiConst.getConst(g)))

    def injectLeft[A: Ob, B: Ob]: BiConst[GraphSummary, A, Sum[A, B]] =
      BiConst(GraphSummary.empty)

    def injectRight[A: Ob, B: Ob]: BiConst[GraphSummary, B, Sum[A, B]] =
      BiConst(GraphSummary.empty)
  }

  given labeledAnalyze: BiFunctionK[LabeledFnArrow, [A, B] =>> BiConst[GraphSummary, A, B]] =
    new BiFunctionK[LabeledFnArrow, [A, B] =>> BiConst[GraphSummary, A, B]] {
      def apply[A, B](node: LabeledFnArrow[A, B]): BiConst[GraphSummary, A, B] =
        BiConst(GraphSummary.fromLabeled(node))
    }
}
