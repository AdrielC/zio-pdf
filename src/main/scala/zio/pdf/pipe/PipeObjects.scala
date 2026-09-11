/*
 * volga object witnesses for [[Pipe]] — shared [[HomU]] / [[ObjWitness]] kit.
 * Reference: modules/volga-core/.../free/FreeU.scala
 */

package zio.pdf.pipe

object PipeObjects extends EvalObAliases[HomU] {

  type U[t] = HomU[t]

  export WitnessObjects.{ob, tagOb, monoidalObjects, zeroOb, sumOb, scalaObjects}
}
