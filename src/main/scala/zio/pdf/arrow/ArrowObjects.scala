package zio.pdf.arrow

import volga.{MonoidalObjects, ObAliases, ScalaObjects}
import volga.tags.*

/** Runtime interpretation of volga object tags — must use `volga.tags`, not `zio.pdf.pipe.tags`. */
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

  def ob[A]: Ob[A] = null.asInstanceOf[Ob[A]]

  given monoidalObjects: MonoidalObjects[ArrowU] with {
    given unitOb: Ob[I]                     = ob
    given tensorOb[A: Ob, B: Ob]: Ob[A x B] = ob
  }

  given zeroOb: Ob[O]                     = ob
  given sumOb[A: Ob, B: Ob]: Ob[A + B]   = ob

  given scalaObjects: ScalaObjects[ArrowU] with {
    given scalaOb[A]: Ob[$[A]] = ob
  }

  type Prod[A, B] = A x B
  type Sum[A, B]  = A + B
}
