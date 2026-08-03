/*
 * Scala-object interpreter for [[Cat]] — objects are plain types.
 *
 * Pattern matches volga `FreeU`: `PipeU` at package scope, nested
 * `monoidalObjects` / `scalaObjects` givens under [[PipeObjects]].
 * Reference: modules/volga/.../free/FreeU.scala
 */

package zio.pdf.pipe

import zio.pdf.pipe.tags.*

type PipeU[t] = t match
  case Obj[a]        => a
  case One           => Unit
  case Tensor[a, b]  => (a, b)
  case Scala[a]      => a
  case Plus[a, b]    => Either[a, b]
  case Zero          => Nothing
  case Closure[a, b] => a => b
  case Dual[a]       => a

object PipeObjects extends ObAliases[PipeU]:

  type U[t] = PipeU[t]

  /** Phantom object evidence. Prefer [[ob]] over importing these givens into ZIO specs. */
  def ob[A]: Ob[A] = null.asInstanceOf[Ob[A]]

  given monoidalObjects: MonoidalObjects[PipeU] with
    given unitOb: Ob[I]                              = ob
    given tensorOb[A: Ob, B: Ob]: Ob[A x B]          = ob

  given scalaObjects: ScalaObjects[PipeU] with
    given scalaOb[A]: Ob[$[A]] = ob
