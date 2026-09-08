/*
 * volga object witnesses for [[Pipe]] — shared [[WitnessU]] / [[ObjWitness]] kit.
 * Reference: modules/volga-core/.../free/FreeU.scala
 */

package zio.pdf.pipe

import volga.ObAliases

object PipeObjects extends ObAliases[WitnessU] {

  type U[t] = WitnessU[t]

  export WitnessObjects.{ob, tagOb, monoidalObjects, zeroOb, sumOb, scalaObjects}
}
