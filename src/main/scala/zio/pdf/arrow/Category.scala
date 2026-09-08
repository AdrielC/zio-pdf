package zio.pdf.arrow

import ArrowObjects.*

/** Minimal arrow algebra (compose + identity). */
trait Compose[FF[_, _]] {
  def id[A]: FF[A, A]
  def compose[A, B, C](f: FF[B, C], g: FF[A, B]): FF[A, C]
}

object Compose {
  extension [FF[_, _]](C: Compose[FF]) {
    def >>>[A, B, C](g: FF[B, C], f: FF[A, B]): FF[A, C] = C.compose(g, f)
  }
}

/**
 * Full category with path-dependent product / coproduct — volga Cartesian +
 * Cocartesian over `Ob`.
 */
trait Category[FF[_, _]] extends Compose[FF] {

  /** Monoidal tensor on independent objects (`><` / `***`). */
  def split[A: Ob, B: Ob, C: Ob, D: Ob](f: FF[A, B], g: FF[C, D]): FF[Prod[A, C], Prod[B, D]]

  /** Cartesian fan-out on a shared domain (`&&&` / `<>`). */
  def fanout[A: Ob, B: Ob, C: Ob](f: FF[A, B], g: FF[A, C]): FF[A, Prod[B, C]]

  /** Coproduct merge (`|||` / `sum`). */
  def merge[A: Ob, B: Ob, C: Ob](f: FF[A, C], g: FF[B, C]): FF[Sum[A, B], C]

  /** Either routing preserving branches (`choose` / `+++`). */
  def choose[A: Ob, B: Ob, C: Ob, D: Ob](f: FF[A, C], g: FF[B, D]): FF[Sum[A, B], Sum[C, D]]

  def injectLeft[A: Ob, B: Ob]: FF[A, Sum[A, B]]
  def injectRight[A: Ob, B: Ob]: FF[B, Sum[A, B]]
}
