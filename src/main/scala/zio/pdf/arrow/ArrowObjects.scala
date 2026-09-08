package zio.pdf.arrow

import zio.pdf.pipe.{EvalObAliases, HomU, ObjWitness, WitnessObjects}

/** Object universe for path-dependent `x` / `+` over volga tags (witness-backed `Ob`). */
object ArrowObjects extends EvalObAliases[HomU] {

  type U[t] = HomU[t]

  export WitnessObjects.{ob, tagOb, monoidalObjects, zeroOb, sumOb, scalaObjects}

  type Prod[A, B] = A x B
  type Sum[A, B]  = A + B

  /** Alias matching volga naming — witnesses live in [[ObjWitness]]. */
  type ArrowObj[A] = ObjWitness[A]
}
