package zio.pdf.arrow

import zio.test.*
import volga.free.{FreeProp, Nat, PropOb}
import volga.SymmetricCat
import volga.syntax.smc
import volga.syntax.smc.V

type WiringLabel[A, B] = String
type WiringDiag[A, B]  = FreeProp[WiringLabel, A, B]
type WiringV1          = V[Nat.`1`]

given wiringCat: SymmetricCat[WiringDiag, PropOb] = FreeProp.propCat[WiringLabel]
val wiringProp                                      = smc.syntax[WiringDiag, PropOb, Nat.Plus]

val wiringBNode = ArrowSyntax.node("b", 1, 0)

object ArrowSyntaxSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("ArrowSyntax")(
    test("SMC of1 identity wiring") {
      val exp            = wiringProp.of1((x: WiringV1) => x)
      val (out, edges)   = GraphRender.wiring(exp, Tuple1("x"))
      assertTrue(edges.isEmpty, out == Vector("x"))
    },
    test("SMC apply produces edges") {
      val exp          = wiringProp.of1((v: WiringV1) => wiringBNode(v))
      val (out, edges) = GraphRender.wiring(exp, Tuple1("x"))
      assertTrue(edges == Vector("x" -> "b"), out.isEmpty)
    },
    test("SMC swap rewires ports") {
      val exp          = wiringProp.of2((a: WiringV1, b: WiringV1) => (b, a))
      val (out, edges) = GraphRender.wiring(exp, ("x", "y"))
      assertTrue(edges.isEmpty, out == Vector("y", "x"))
    },
    test("sequential FreeProp wiring chains nodes") {
      val one = Nat.Succ(Nat.Zero())
      val a   = FreeProp.Embed[WiringLabel, Nat.Zero, Nat.`1`]("a")
      val c   = FreeProp.Embed[WiringLabel, Nat.`1`, Nat.OfInt[3]]("c")
      val exp = FreeProp.AndThen(a, c, one)

      val (out, edges) = GraphRender.wiring(exp, EmptyTuple)
      assertTrue(edges == Vector("a" -> "c"), out == Vector("c", "c", "c"))
    },
    test("GraphRender produces mermaid and dot") {
      val exp     = wiringProp.of1((v: WiringV1) => wiringBNode(v))
      val formats = ArrowSyntax.render("ingest", exp, Tuple1("bytes"))
      assertTrue(
        formats.mermaid.contains("bytes --> b"),
        formats.dot.contains("\"bytes\" -> \"b\""),
        formats.plantUml.contains("@startuml")
      )
    },
    test("FreeArrow folds FnArrow spines") {
      given Category[FnArrow] = FnArrowCat.fnCategory
      val f                   = FnArrow[Int, Int](_ + 1)
      val g                   = FnArrow[Int, Int](_ * 2)
      val free                = FreeArrow.embed(f) >>> FreeArrow.embed(g)
      val h                   = free.fold[FnArrow]
      assertTrue(h.run(3) == 8)
    }
  )
}
