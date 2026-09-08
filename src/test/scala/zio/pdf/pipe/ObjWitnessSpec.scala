package zio.pdf.pipe

import zio.test.*

object ObjWitnessSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("ObjWitness")(
    test("witnesses are distinct real objects (not null)") {
      val unit = ObjWitness.UnitObj
      val int  = ObjWitness.Tag[Int]()
      val str  = ObjWitness.Tag[String]()
      val prod = ObjWitness.Prod(int, str)
      assertTrue(
        unit ne null,
        int ne null,
        str ne null,
        prod ne null,
        prod.isInstanceOf[ObjWitness.Prod[Int, String]]
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
      assertTrue(wit.isInstanceOf[ObjWitness[Int]], !wit.equals(0))
    }
  )
}
