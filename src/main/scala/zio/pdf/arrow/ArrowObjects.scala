package zio.pdf.arrow

import volga.{MonoidalObjects, ObAliases, ScalaObjects}
import volga.tags.*

/** Runtime interpretation of volga object tags — use `volga.tags`, not a local copy. */
type ArrowU[t] = t match {
  case Obj[a]        => a
  case One           => Unit
  case Tensor[a, b]  => (a, b)
  case Scala[a]      => a
  case Plus[a, b]    => Either[a, b]
  case Zero          => Nothing
  case Closure[a, b] => a => b
  case Dual[a]       => a
}

/**
 * Object universe for path-dependent `x` / `+` over volga tags.
 *
 * '''Important:''' `Ob[A]` is defined as `ArrowU[Obj[A]]`, which erases to the plain
 * Scala type `A` (see [[ArrowU]]). It is compile-time evidence for volga's categorical
 * operators — our interpreters never read these values at runtime.
 *
 * volga's own `FreeU` uses distinct enum witnesses (`FreeObj.One`, `FreeObj.Prod`, …)
 * instead of this erasure; that is the proper long-term direction if we outgrow plain
 * `FnArrow` types. Until then, `()` for `Ob[Unit]` and [[phantomOb]] cover the rest.
 */
object ArrowObjects extends ObAliases[ArrowU] {

  type U[t] = ArrowU[t]

  inline def ob[A]: Ob[A] = phantomOb[A]

  /** Compile-time-only evidence; never read at runtime (would be meaningless for most `A`). */
  given phantomOb[A]: Ob[A] = null.asInstanceOf[Ob[A]]

  given monoidalObjects: MonoidalObjects[ArrowU] with {
    given unitOb: Ob[I]                     = ()
    given tensorOb[A: Ob, B: Ob]: Ob[A x B] = summon[Ob[A x B]]
  }

  given scalaObjects: ScalaObjects[ArrowU] with {
    given scalaOb[A]: Ob[$[A]] = summon[Ob[$[A]]]
  }

  type Prod[A, B] = A x B
  type Sum[A, B]  = A + B
}
