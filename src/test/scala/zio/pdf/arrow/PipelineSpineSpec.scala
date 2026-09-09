package zio.pdf.arrow

import zio.test.*
import zio.pdf.pipe.{FreePipe, Pipe}

object PipelineSpineSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("PipelineSpine")(
    test("run / analyze / render on sequential spine") {
      val spine = PipelineSpine.node("inc", 1, 1)(Pipe[Int, Int](_ + 1)) >>>
        PipelineSpine.node("dbl", 1, 1)(Pipe[Int, Int](_ * 2))
      val formats = PipelineSpine.render("seq", spine, "x")
      assertTrue(
        PipelineSpine.run(spine).run(3) == 8,
        PipelineSpine.analyze(spine) != ScanGraph.Empty,
        formats.mermaid.contains("inc")
      )
    },
    test("mermaid is syntactically valid and wires input to nodes") {
      val spine = PipelineSpine.node("inc", 1, 1)(Pipe[Int, Int](_ + 1)) >>>
        PipelineSpine.node("dbl", 1, 1)(Pipe[Int, Int](_ * 2))
      val mermaid = PipelineSpine.render("seq", spine, "x").mermaid
      assertTrue(
        mermaid.startsWith("flowchart LR"),
        mermaid.contains("x --> inc"),
        mermaid.contains("inc --> dbl")
      )
    },
    test("fanout zip preserves branch nodes in analyze") {
      val left  = PipelineSpine.node("left", 1, 1)(Pipe[Int, Int](_ + 1))
      val right = PipelineSpine.node("right", 1, 1)(Pipe[Int, Int](_ * 10))
      val spine = left &&& right
      val names = ScanGraph.nodeNames(PipelineSpine.analyze(spine))
      assertTrue(
        PipelineSpine.run(spine).run(2) == (3, 20),
        names.contains("left"),
        names.contains("right")
      )
    },
    test("fanout mermaid fans input to both branches") {
      val left  = PipelineSpine.node("left", 1, 1)(Pipe[Int, Int](_ + 1))
      val right = PipelineSpine.node("right", 1, 1)(Pipe[Int, Int](_ * 10))
      val formats = PipelineSpine.render("fan", left &&& right, "bytes")
      val edges   = formats.wiring._2
      assertTrue(
        formats.mermaid.startsWith("flowchart LR"),
        edges.contains("bytes" -> GraphSummary.FanOp),
        edges.contains(GraphSummary.FanOp -> "left"),
        edges.contains(GraphSummary.FanOp -> "right")
      )
    },
    test("fromFreePipe structural round-trip matches fold") {
      val fp = FreePipe.arr[Int, Int](_ + 1) &&& FreePipe.arr[Int, String](i => s"$i")
      val spine = PipelineSpine.fromFreePipe(fp)
      val direct = FreePipe.fold(fp)
      assertTrue(PipelineSpine.run(spine).run(4) == direct.run(4))
    },
    test("nested fanout mermaid fans from upstream node not global input") {
      type B = Array[Byte]
      val id = Pipe[B, B](identity)
      val slice = PipelineSpine.node("slice", 1, 1)(id)
      val left  = PipelineSpine.node("left", 1, 1)(id)
      val inner = PipelineSpine.node("inner-a", 1, 1)(id) &&& PipelineSpine.node("inner-b", 1, 1)(id)
      val spine = slice >>> (left &&& inner)
      val formats = PipelineSpine.render("nested", spine, "bytes")
      val edges   = formats.wiring._2
      assertTrue(
        edges.contains("slice" -> GraphSummary.FanOp),
        edges.contains(GraphSummary.FanOp -> "left"),
        edges.contains(GraphSummary.FanOp -> "inner-a"),
        edges.contains(GraphSummary.FanOp -> "inner-b"),
        !edges.contains("bytes" -> "inner-a")
      )
    },
    test("planFromFlow matches PipelineFlow schema") {
      val flow = PipelineFlow.init("demo").input[Int]("x").pipe("inc")(Pipe(_ + 1)).build
      val plan = PipelineSpine.planFromFlow(flow)
      assertTrue(
        plan.name == "demo",
        plan.nodes.contains("inc"),
        plan.schemaJson.contains("inc")
      )
    }
  )
}
