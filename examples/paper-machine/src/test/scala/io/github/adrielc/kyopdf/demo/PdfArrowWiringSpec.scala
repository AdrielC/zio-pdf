package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.pipeline.{PdfArrow, PdfPipeline}
import PdfArrow.*
import PipelineWiring.{given, *}
import volga.syntax.smc.V
import scala.compiletime.testing.typeCheckErrors
import zio.test.{check as _, *}

object PdfArrowWiringSpec extends ZIOSpecDefault:
  def spec = suite("Volga over typed PDF arrows")(
    test("the studio example uses the actual typed SMC category") {
      val example = PipelineWiring.example
      assertTrue(example.version == 1, example.nodes.map(_.id) == Vector("body", "cut", "stamp", "numbers", "pdf", "checks"),
        example.outputs == Vector("pdf", "checks"), PdfPipeline.check(example).isRight)
    },
    test("typed of1 identity preserves the object rather than an untyped port count") {
      val arrow: PdfArrow[Document, Document] = syntax.of1((document: V[Document]) => document)
      val result = PdfArrow.recipe("Identity", input("body") >>> arrow >>> write).toOption.get
      assertTrue(result.nodes.length == 2, result.nodes.last.inputs == Vector("body"))
    },
    test("typed of2 composes unary arrows into a PDF join") {
      val mark = stamp("MARK")
      val join = assemble()
      val arrow: PdfArrow[(Document, Document), Document] = syntax.of2((front: V[Document], body: V[Document]) => {
        val marked = mark(body)
        val packet = join(front, marked)
        packet
      })
      val result = PdfArrow.recipe("Joined", (input("front") &&& input("body")) >>> arrow >>> write).toOption.get
      assertTrue(result.nodes.find(_.op.isInstanceOf[PdfPipeline.Op.Assemble]).get.inputs == Vector("front", "stamp"),
        PdfPipeline.check(result).isRight)
    },
    test("three and four typed inputs retain reordered product ports") {
      val join3 = assemble3(); val join4 = assemble4()
      val three: PdfArrow[((Document, Document), Document), Document] = syntax.of3((a: V[Document], b: V[Document], c: V[Document]) => {
        val packet = join3(c, a, b)
        packet
      })
      val four: PdfArrow[(((Document, Document), Document), Document), Document] = syntax.of4((a: V[Document], b: V[Document], c: V[Document], d: V[Document]) => {
        val packet = join4(d, b, a, c)
        packet
      })
      val source3 = (input("a") &&& input("b")) &&& input("c")
      val source4 = source3 &&& input("d")
      val recipe3 = PdfArrow.recipe("Three", source3 >>> three >>> write).toOption.get
      val recipe4 = PdfArrow.recipe("Four", source4 >>> four >>> write).toOption.get
      assertTrue(recipe3.nodes.find(_.op.isInstanceOf[PdfPipeline.Op.Assemble]).get.inputs == Vector("c", "a", "b"),
        recipe4.nodes.find(_.op.isInstanceOf[PdfPipeline.Op.Assemble]).get.inputs == Vector("d", "b", "a", "c"))
    },
    test("tensor results support tuple destructuring and heterogeneous ports") {
      val read = input("body")
      val split = fork[Document]
      val mark = stamp("MARK")
      val finish = write &&& (write >>> validate("3b"))
      val keep = keepRight[File, Receipt]
      val arrow: PdfArrow[Unit, (Receipt, File)] = syntax.of0:
        val source = read()
        val (left, right) = split(source)
        val marked = mark(left)
        val (pdf, receipt) = finish(marked)
        val checked = keep(pdf, receipt)
        val other = write(right)
        (checked, other)
      val result = PdfArrow.recipe("Nested", arrow).toOption.get
      assertTrue(result.outputs.length == 2, result.outputs.map(id => result.nodes.find(_.id == id).get.op.output) ==
        Vector(PdfPipeline.Kind.Receipt, PdfPipeline.Kind.File))
    },
    test("zero-input arrows can return and destructure a product directly") {
      val sources = input("front") &&& input("body")
      val join = assemble()
      val arrow: PdfArrow[Unit, File] = syntax.of0:
        val (front, body) = sources()
        val packet = join(front, body)
        val output = write(packet)
        output
      assertTrue(PdfArrow.recipe("Sources", arrow).isRight)
    },
    test("conditional branches are reusable arrows inside ordinary SMC application") {
      val recipe = PipelineWiring.conditionalExample
      val route = recipe.nodes.find(_.id == "route").get
      assertTrue(recipe.version == 3, route.inputs == Vector("body", "short", "long", "unknown"),
        recipe.nodes.find(_.id == "unknown").get.inputs == Vector("body"), PdfPipeline.check(recipe).isRight)
    },
    test("wrong PDF types, implicit copying and unused wires fail during SMC construction") {
      val wrong = typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.PdfArrow.*
        import io.github.adrielc.kyopdf.demo.PipelineWiring.{given, *}
        import volga.syntax.smc.V
        val mark = stamp("bad")
        val bad = syntax.of1((bytes: V[File]) => mark(bytes))""")
      val copied = typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.PdfArrow.*
        import io.github.adrielc.kyopdf.demo.PipelineWiring.{given, *}
        import volga.syntax.smc.V
        val bad = syntax.of1((doc: V[Document]) => { val a = write(doc); val b = write(doc); (a, b) })""")
      val unused = typeCheckErrors("""import io.github.adrielc.kyopdf.pipeline.PdfArrow.*
        import io.github.adrielc.kyopdf.demo.PipelineWiring.{given, *}
        import volga.syntax.smc.V
        val bad = syntax.of2((a: V[Document], b: V[Document]) => { val pdf = write(a); pdf })""")
      assertTrue(wrong.nonEmpty, copied.nonEmpty, unused.nonEmpty)
    }
  )
