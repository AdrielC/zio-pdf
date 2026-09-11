package zio.pdf.pipe

import volga.ObAliases
import volga.tags.*
import scala.util.NotGiven

/**
 * Distinct runtime witnesses for volga object tags — same idea as volga
 * `free.FreeObj` / `FreeU` (see `modules/volga-core/.../free/FreeU.scala`).
 *
 * `One` / `Zero` are proper witness values ([[ObjWitness.One]], [[ObjWitness.Zero]]),
 * not `()` / impossible values smuggled through `Ob`. Hom-sets (`I`, `x`, `+`, …)
 * erase via [[HomU]] / [[EvalU]] to plain Scala so [[Pipe]] stays executable.
 */
enum ObjWitness[A]:
  case One  extends ObjWitness[Unit]
  case Zero extends ObjWitness[Nothing]
  case Tag[A]() extends ObjWitness[A]
  case OfScala[A]() extends ObjWitness[A]
  case Prod[A, B](l: ObjWitness[A], r: ObjWitness[B]) extends ObjWitness[(A, B)]
  case Inl[A, B](v: ObjWitness[A]) extends ObjWitness[Either[A, B]]
  case Inr[A, B](v: ObjWitness[B]) extends ObjWitness[Either[A, B]]

/**
 * volga tag interpreter for categories — hom-sets are plain Scala types;
 * only `Obj[a]` maps to [[ObjWitness]] evidence.
 */
type HomU[t] = t match {
  case Obj[a]        => ObjWitness[a]
  case One           => Unit
  case Tensor[a, b]  => (a, b)
  case Scala[a]      => a
  case Plus[a, b]    => Either[a, b]
  case Zero          => Nothing
  case Closure[a, b] => a => b
  case Dual[a]       => a
}

/** Alias for [[HomU]] when overriding hom-sets in [[EvalObAliases]]. */
type EvalU[t] = HomU[t]

/**
 * volga [[ObAliases]] with hom-sets pinned to [[EvalU]] while `Ob` stays
 * witness-backed (`ObjWitness`, not plain `Unit` / `Nothing`).
 */
transparent trait EvalObAliases[U[_]] {
  type Ob[A]           = U[Obj[A]]
  type I               = EvalU[One]
  infix type x[A, B]   = EvalU[Tensor[A, B]]
  infix type ==>[A, B] = EvalU[Closure[A, B]]
  type ^[A]            = EvalU[Dual[A]]
  infix type +[A, B]   = EvalU[Plus[A, B]]
  type O               = EvalU[Zero]
  type $[A]            = EvalU[Scala[A]]
}

/** Canonical `Ob` / monoidal / cocartesian / scala givens. */
object WitnessObjects extends ObAliases[HomU] {

  type U[t] = HomU[t]

  inline def ob[A]: Ob[A] = ObjWitness.Tag[A]()

  given tagOb[A](using NotGiven[A =:= Unit], NotGiven[A =:= Nothing]): Ob[A] =
    ObjWitness.Tag[A]()

  given monoidalObjects: volga.MonoidalObjects[HomU] with {
    given unitOb: Ob[I] = ObjWitness.One
    given tensorOb[A: Ob, B: Ob]: Ob[A x B] =
      (summon[Ob[A]], summon[Ob[B]]) match {
        case (l: ObjWitness[a], r: ObjWitness[b]) =>
          ObjWitness.Prod[a, b](l, r)
      }
  }

  given zeroOb: Ob[O] = ObjWitness.Zero

  given sumOb[A: Ob, B: Ob]: Ob[A + B] =
    summon[Ob[A]] match {
      case t: ObjWitness[a] => ObjWitness.Inl[a, B](t)
    }

  given scalaObjects: volga.ScalaObjects[HomU] with {
    given scalaOb[A]: Ob[$[A]] = ObjWitness.OfScala[A]()
  }
}
