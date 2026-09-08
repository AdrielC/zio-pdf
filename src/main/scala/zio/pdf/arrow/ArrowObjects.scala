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

/** Object universe for path-dependent `x` / `+` over volga tags. */
object ArrowObjects extends ObAliases[ArrowU] {

  type U[t] = ArrowU[t]

  /** Phantom evidence — never read at runtime; avoid routing through a shared `ob` val (JDK 21+). */
  inline def ob[A]: Ob[A] = null.asInstanceOf[Ob[A]]

  /** Fallback phantom evidence for object tags (never read at runtime). */
  given phantomOb[A]: Ob[A] = null.asInstanceOf[Ob[A]]

  given monoidalObjects: MonoidalObjects[ArrowU] with {
    given unitOb: Ob[I]                     = null.asInstanceOf[Ob[I]]
    given tensorOb[A: Ob, B: Ob]: Ob[A x B] = null.asInstanceOf[Ob[A x B]]
  }

  given scalaObjects: ScalaObjects[ArrowU] with {
    given scalaOb[A]: Ob[$[A]] = null.asInstanceOf[Ob[$[A]]]
  }

  type Prod[A, B] = A x B
  type Sum[A, B]  = A + B
}
