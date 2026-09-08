package zio.pdf.pipe

import volga.tags.*
import scala.util.NotGiven

/**
 * Distinct runtime witnesses for volga object tags — same idea as volga
 * `free.FreeObj` / `FreeU` (see `modules/volga-core/.../free/FreeU.scala`).
 *
 * Hom-types (`Tensor`, `Plus`, …) still erase to plain Scala tuples / `Either`;
 * only `Obj[a]` tags become [[ObjWitness]] values used as `Ob` evidence.
 */
enum ObjWitness[A]:
  case UnitObj extends ObjWitness[Unit]
  case ZeroObj extends ObjWitness[Nothing]
  case Tag[A]() extends ObjWitness[A]
  case Scala[A]() extends ObjWitness[A]
  case Prod[A, B](l: ObjWitness[A], r: ObjWitness[B]) extends ObjWitness[(A, B)]
  case Inl[A, B](v: ObjWitness[A]) extends ObjWitness[Either[A, B]]
  case Inr[A, B](v: ObjWitness[B]) extends ObjWitness[Either[A, B]]

/** Shared volga object interpreter for pipe + arrow categories. */
type WitnessU[t] = t match {
  case Obj[a]        => ObjWitness[a]
  case One           => Unit
  case Tensor[a, b]  => (a, b)
  case Scala[a]      => a
  case Plus[a, b]    => Either[a, b]
  case Zero          => Nothing
  case Closure[a, b] => a => b
  case Dual[a]       => a
}

/** Canonical `Ob` / monoidal / cocartesian / scala givens over [[WitnessU]]. */
object WitnessObjects extends volga.ObAliases[WitnessU] {

  type U[t] = WitnessU[t]

  inline def ob[A]: Ob[A] = ObjWitness.Tag[A]()

  given tagOb[A](using NotGiven[A =:= Unit], NotGiven[A =:= Nothing]): Ob[A] =
    ObjWitness.Tag[A]()

  given monoidalObjects: volga.MonoidalObjects[WitnessU] with {
    given unitOb: Ob[I] = ObjWitness.UnitObj
    given tensorOb[A: Ob, B: Ob]: Ob[A x B] =
      (summon[Ob[A]], summon[Ob[B]]) match {
        case (l: ObjWitness[a], r: ObjWitness[b]) =>
          ObjWitness.Prod[a, b](l, r)
      }
  }

  given zeroOb: Ob[O] = ObjWitness.ZeroObj

  given sumOb[A: Ob, B: Ob]: Ob[A + B] =
    summon[Ob[A]] match {
      case t: ObjWitness[a] => ObjWitness.Inl[a, B](t)
    }

  given scalaObjects: volga.ScalaObjects[WitnessU] with {
    given scalaOb[A]: Ob[$[A]] = ObjWitness.Scala[A]()
  }
}
