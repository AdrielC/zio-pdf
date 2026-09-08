package zio.pdf.arrow

/** Constant bifunctor — carries an invariant summary `M` (graph, schema, etc.). */
opaque type BiConst[M, +A, +B] = M

object BiConst {

  def apply[M, A, B](m: M): BiConst[M, A, B] = m

  def getConst[M, A, B](c: BiConst[M, A, B]): M = c

  def liftK[M](analyze: ScanGraph => M): BiFunctionK[FnArrow, [A, B] =>> BiConst[M, A, B]] =
    new BiFunctionK[FnArrow, [A, B] =>> BiConst[M, A, B]] {
      def apply[A, B](fa: FnArrow[A, B]): BiConst[M, A, B] =
        BiConst(analyze(ScanGraph.empty))
    }

  type ~|[F[_, _], M] = [A, B] => F[A, B] => M
}
