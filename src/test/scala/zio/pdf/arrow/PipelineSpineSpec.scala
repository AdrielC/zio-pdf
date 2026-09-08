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
    test("fromFreePipe structural round-trip matches fold") {
      val fp = FreePipe.arr[Int, Int](_ + 1) &&& FreePipe.arr[Int, String](i => s"$i")
      val spine = PipelineSpine.fromFreePipe(fp)
      val direct = FreePipe.fold(fp)
      assertTrue(PipelineSpine.run(spine).run(4) == direct.run(4))
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
