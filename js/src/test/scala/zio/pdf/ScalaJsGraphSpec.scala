package zio.pdf

import zio.pdf.arrow.{GraphRender, PipelineFlow}
import zio.pdf.pipe.Pipe
import zio.test.*

object ScalaJsGraphSpec extends ZIOSpecDefault:
  def spec: Spec[Any, Any] = suite("vendored graph runtime in Scala.js")(
    test("composed graph execution agrees with direct function composition") {
      val increment = PipelineFlow.init("increment").input[Int]("input").pipe("add-one")(Pipe(_ + 1)).build
      val double = PipelineFlow.init("double").input[Int]("input").pipe("double")(Pipe(_ * 2)).build
      val composed = PipelineFlow.andThen(increment, double)
      check(Gen.int(-100000, 100000)) { input =>
        assertTrue(composed.runLocal(input) == (input + 1) * 2,
          composed.renderMermaid.contains("double"))
      }
    },
    test("diagram formats preserve isolated output nodes") {
      check(Gen.int(1, 1000)) { id =>
        val name = s"node$id"
        val wiring = (Vector(name), Vector.empty[(String, String)])
        assertTrue(GraphRender.mermaid("graph", wiring).contains(name),
          GraphRender.dot("graph", wiring).contains(name))
      }
    }
  )
