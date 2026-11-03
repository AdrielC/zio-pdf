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

val wiringANode = ArrowSyntax.node("a", 0, 1)
val wiringBNode = ArrowSyntax.node("b", 1, 0)
val wiringCNode = ArrowSyntax.node("c", 1, 3)

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
    test("unary operations compose into a two-input join") {
      val mark = ArrowSyntax.node("mark", 1, 1)
      val join = ArrowSyntax.node("join", 2, 1)
      val exp = wiringProp.of2((a: WiringV1, b: WiringV1) => {
        val marked = mark(a)
        val joined = join(marked, b)
        joined
      })
      val (out, edges) = GraphRender.wiring(exp, ("a", "b"))
      assertTrue(out == Vector("join"), edges == Vector("a" -> "mark", "mark" -> "join", "b" -> "join"))
    },
    test("three and four input joins preserve port order") {
      val join3 = ArrowSyntax.node("join3", 3, 1)
      val join4 = ArrowSyntax.node("join4", 4, 1)
      val three = wiringProp.of3((a: WiringV1, b: WiringV1, c: WiringV1) => {
        val joined = join3(c, a, b)
        joined
      })
      val four = wiringProp.of4((a: WiringV1, b: WiringV1, c: WiringV1, d: WiringV1) => {
        val joined = join4(d, b, a, c)
        joined
      })
      val (_, edges3) = GraphRender.wiring(three, ("a", "b", "c"))
      val (_, edges4) = GraphRender.wiring(four, ("a", "b", "c", "d"))
      assertTrue(edges3 == Vector("c" -> "join3", "a" -> "join3", "b" -> "join3"),
        edges4 == Vector("d" -> "join4", "b" -> "join4", "a" -> "join4", "c" -> "join4"))
    },
    test("complex pipeline wiring matches volga SyntaxTest") {
      val exp = wiringProp.of0:
        val x         = wiringANode()
        val (u, v, w) = wiringCNode(x)
        wiringBNode(v)
        (w, u)

      val (out, edges) = GraphRender.wiring(exp, EmptyTuple)
      assertTrue(
        edges == Vector("a" -> "c", "c" -> "b"),
        out == Vector("c", "c")
      )
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
