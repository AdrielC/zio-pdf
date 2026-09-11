package zio.pdf.pipe

import zio.test.*

object ObjWitnessSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("ObjWitness")(
    test("witnesses are distinct real objects (not null)") {
      val one  = ObjWitness.One
      val int  = ObjWitness.Tag[Int]()
      val str  = ObjWitness.Tag[String]()
      val prod = ObjWitness.Prod(int, str)
      assertTrue(
        one ne null,
        int ne null,
        str ne null,
        prod ne null,
        prod.isInstanceOf[ObjWitness.Prod[Int, String]]
      )
    },
    test("One and Zero are witnesses like volga FreeObj (not plain Unit/Nothing)") {
      val unitOb = WitnessObjects.monoidalObjects.unitOb
      val zeroOb = WitnessObjects.zeroOb
      assertTrue(
        unitOb eq ObjWitness.One,
        zeroOb eq ObjWitness.Zero,
        unitOb.isInstanceOf[ObjWitness.One.type]
      )
    },
    test("tensorOb builds Prod witnesses like volga FreeU") {
      import PipeObjects.Ob
      given Ob[Int]    = ObjWitness.Tag[Int]()
      given Ob[String] = ObjWitness.Tag[String]()
      val combined = WitnessObjects.monoidalObjects.tensorOb[Int, String]
      assertTrue(combined match {
        case ObjWitness.Prod(_: ObjWitness.Tag[Int], _: ObjWitness.Tag[String]) => true
        case _                                                                  => false
      })
    },
    test("Ob evidence is a witness, not a plain Int") {
      import PipeObjects.Ob
      val wit: Ob[Int] = ObjWitness.Tag[Int]()
      assertTrue(wit.isInstanceOf[ObjWitness.Tag[Int]], !wit.equals(0))
    },
    test("hom-sets erase to plain Scala via EvalU") {
      val _: EvalU[volga.tags.One]                  = ()
      val _: EvalU[volga.tags.Tensor[Int, String]] = (0, "")
      assertTrue(true)
    }
  )
}
