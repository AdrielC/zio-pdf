package zio.pdf.arrow

import volga.ObAliases
import zio.pdf.pipe.{ObjWitness, WitnessObjects, WitnessU}

/** Object universe for path-dependent `x` / `+` over volga tags (witness-backed `Ob`). */
object ArrowObjects extends ObAliases[WitnessU] {

  type U[t] = WitnessU[t]

  export WitnessObjects.{ob, tagOb, monoidalObjects, zeroOb, sumOb, scalaObjects}

  type Prod[A, B] = A x B
  type Sum[A, B]  = A + B

  /** Alias matching volga naming — witnesses live in [[ObjWitness]]. */
  type ArrowObj[A] = ObjWitness[A]
}
