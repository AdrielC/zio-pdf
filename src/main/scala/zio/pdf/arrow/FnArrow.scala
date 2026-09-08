package zio.pdf.arrow

import zio.pdf.pipe.Pipe

/** Variance-modeled arrow — contravariant input, covariant output. */
sealed trait Arrow[-A, +B] extends Product with Serializable

/** Fused `A => B` arrow. Inline at call sites when built with [[FnArrow.apply]]. */
final case class FnArrow[A, B](private val f: A => B) extends Arrow[A, B] {

  inline def run(inline a: A): B = f(a)

  inline infix def >>>[C](inline g: FnArrow[B, C]): FnArrow[A, C] =
    FnArrow(a => g.f(f(a)))

  infix def &&&[C](g: FnArrow[A, C]): FnArrow[A, (B, C)] =
    FnArrow(a => (f(a), g.f(a)))

  inline infix def ***[C, D](inline g: FnArrow[C, D]): FnArrow[(A, C), (B, D)] =
    FnArrow { case (a, c) => (f(a), g.f(c)) }

  /** Invariant wiring label + optional graph metadata for [[ScanGraph.analyze]]. */
  def labeled(name: String): LabeledFnArrow[A, B] = LabeledFnArrow(name, this)
}

/** [[FnArrow]] tagged with a stable node name for wiring / schema graphs. */
final case class LabeledFnArrow[A, B](name: String, arrow: FnArrow[A, B])

object FnArrow {

  inline def apply[A, B](inline f: A => B): FnArrow[A, B] = new FnArrow(f)

  inline def id[A]: FnArrow[A, A] = FnArrow(identity)

  inline def fromPipe[A, B](inline p: Pipe[A, B]): FnArrow[A, B] =
    FnArrow(p.run)

  given selfBiFunctionK: BiFunctionK[FnArrow, FnArrow] = BiFunctionK.id[FnArrow]
}
