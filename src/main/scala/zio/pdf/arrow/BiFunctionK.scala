package zio.pdf.arrow

/** Binatural transformation `F ~> G` between bifunctors. */
trait BiFunctionK[F[_, _], G[_, _]] {
  def apply[A, B](fa: F[A, B]): G[A, B]
}

object BiFunctionK {

  def id[F[_, _]]: BiFunctionK[F, F] = new BiFunctionK[F, F] {
    def apply[A, B](fa: F[A, B]): F[A, B] = fa
  }

  def lift[F[_, _]](f: [A, B] => F[A, B] => F[A, B]): BiFunctionK[F, F] =
    new BiFunctionK[F, F] {
      def apply[A, B](fa: F[A, B]): F[A, B] = f[A, B](fa)
    }

  extension [F[_, _], G[_, _]](self: BiFunctionK[F, G]) {
    infix def andThen[H[_, _]](gh: BiFunctionK[G, H]): BiFunctionK[F, H] =
      new BiFunctionK[F, H] {
        def apply[A, B](fa: F[A, B]): H[A, B] = gh(self(fa))
      }
  }

  /** Alias mirroring the free-arrow operator notation. */
  type ~~>[F[_, _], G[_, _]] = BiFunctionK[F, G]
}
