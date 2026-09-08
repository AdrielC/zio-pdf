package zio.pdf.arrow

import zio.*
import zio.pdf.pipe.{FreePipe, IngestPipeline, Pipe}
import zio.test.*
import volga.free.Nat
import volga.syntax.smc.V

object PipelineGraphSpec extends ZIOSpecDefault {

  private def loadFixture(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing from test resources")
      val buf = is.readAllBytes()
      is.close()
      buf
    }

  def spec: Spec[Any, Throwable] = suite("PipelineGraph")(
    test("Pipe ↔ FnArrow ↔ FreeArrow round-trip") {
      val p   = Pipe[Int, Int](_ + 1)
      val fa  = PipelineGraph.nodeFn("inc", 1, 1)(FnArrow.fromPipe(p))
      val run = PipelineGraph.run(fa)
      assertTrue(run.run(3) == 4)
    },
    test("FreePipe embeds into FreeArrow and folds to same behavior") {
      val fp  = FreePipe.arr[Int, Int](_ * 2)
      val fa  = PipelineGraph.fromFreePipe(fp)
      val p   = PipelineGraph.toPipe(fa)
      assertTrue(p.run(5) == 10, FreePipe.fold(fp).run(5) == p.run(5))
    },
    test("FreeArrow flatCompile + analyze on labeled spine") {
      val graph = PipelineGraph.nodeFn("a", 1, 1)(FnArrow[Int, Int](_ + 1)) >>>
        PipelineGraph.nodeFn("b", 1, 1)(FnArrow[Int, Int](_ * 2))
      val flat   = graph.flatCompile
      val schema = PipelineGraph.analyze(flat)
      assertTrue(
        PipelineGraph.run(flat).run(2) == 6,
        schema != ScanGraph.Empty
      )
    },
    test("staged ingest graph analyze + render") {
      val analyzed = IngestGraph.schemaStaged()
      val render   = IngestGraph.renderStaged()
      assertTrue(
        ScanGraph.containsNode(analyzed, "slice"),
        ScanGraph.containsNode(analyzed, "staged-decode-digest"),
        render.mermaid.contains("-->")
      )
    },
    test("fused ingest graph runs same as IngestPipeline") {
      for {
        bytes  <- loadFixture("xref-stream.pdf")
        cfg     = zio.pdf.pipe.FusedDecode.Cfg()
        run     = IngestGraph.runFused(cfg)
        direct  = IngestPipeline.decodeAndDigest.fromBytes(cfg)
      } yield assertTrue(run.run(bytes).digest.sameElements(direct.run(bytes).digest))
    },
    test("ScanGraph.fromWiring round-trips volga link") {
      val exp          = PipelineGraph.wiringProp.of1((v: V[Nat.`1`]) => ArrowSyntax.node("decode", 1, 0)(v))
      val wiring       = GraphRender.wiring(exp, Tuple1("x"))
      val schema       = ScanGraph.fromWiring(wiring)
      val (_, edges)   = wiring
      assertTrue(schema != ScanGraph.Empty, edges == Vector("x" -> "decode"))
    }
  )
}
