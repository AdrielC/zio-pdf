package zio.pdf.arrow

import zio.pdf.pipe.{FreePipe, Pipe}
import zio.pdf.tacit.PdfPipeline

/**
 * Unified pipeline spine — one [[FreeArrow]] AST, multiple interpreters:
 *
 *  - '''run''' — fused [[Pipe]] execution
 *  - '''analyze''' — durable [[ScanGraph]]
 *  - '''render''' / '''plan''' — Mermaid/DOT + TACIT [[PdfPipeline.PipelinePlan]]
 *
 * [[PipelineGraph]] and [[PipelineFlow]] delegate here; prefer this entry point
 * for new agent / tacit code.
 */
object PipelineSpine {

  type Node[A, B] = LabeledFnArrow[A, B]

  /** Labeled free-arrow pipeline (the canonical AST). */
  type Spine[In, Out] = FreeArrow[LabeledFnArrow, In, Out]

  def node[A, B](name: String, inPorts: Int, outPorts: Int)(pipe: Pipe[A, B]): Spine[A, B] =
    FreeArrow.embedLabeled(FnArrow.fromPipe(pipe).labeled(name, inPorts, outPorts))

  def nodeFn[A, B](name: String, inPorts: Int, outPorts: Int)(arrow: FnArrow[A, B]): Spine[A, B] =
    FreeArrow.embedLabeled(arrow.labeled(name, inPorts, outPorts))

  def ident[A]: Spine[A, A] =
    FreeArrow.ident

  // ── interpreters ───────────────────────────────────────────────────────────

  def run[A, B](spine: Spine[A, B]): Pipe[A, B] =
    Pipe(foldNodes(spine).run)

  def foldNodes[A, B](spine: Spine[A, B]): FnArrow[A, B] =
    spine.flatCompile.foldMap(nodeToFnArrow)(using FnArrowCat.fnCategory)

  def analyze[A, B](spine: Spine[A, B]): ScanGraph =
    spine.analyze

  def render[A, B](title: String, spine: Spine[A, B], inputLabel: String = "in"): GraphFormats = {
    val schema = analyze(spine)
    val wiring = schemaToWiring(schema, inputLabel)
    GraphFormats(
      wiring   = wiring,
      plantUml = GraphRender.plantUml(title, wiring),
      mermaid  = GraphRender.mermaid(title, wiring),
      dot      = GraphRender.dot(title, wiring)
    )
  }

  def plan(name: String, spine: Spine[?, ?], inputLabel: String = "in"): PdfPipeline.PipelinePlan =
    PdfPipeline.planFromSchema(name, analyze(spine), inputLabel)

  def planFromFlow[A, B](flow: PipelineFlow.Flow[A, B]): PdfPipeline.PipelinePlan =
    plan(flow.name, flow.graph, flow.inputLabel)

  // ── bridges ────────────────────────────────────────────────────────────────

  def fromFreePipe[A, B](fp: FreePipe[A, B]): Spine[A, B] = {
    def go[X, Y](fp: FreePipe[X, Y]): FreeArrow[LabeledFnArrow, X, Y] =
      (fp match {
        case FreePipe.Id()        => FreeArrow.ident[LabeledFnArrow, X]
        case FreePipe.Embed(p)    => node("embed", 1, 1)(p)
        case FreePipe.Seq(l, r)   => go(l) >>> go(r)
        case FreePipe.Par(l, r)   => FreeArrow.parallel(go(l), go(r))
        case FreePipe.Fan(l, r)   => go(l) &&& go(r)
        case FreePipe.Ch(l, r)    => go(l) +++ go(r)
        case FreePipe.Merge(l, r) => go(l) ||| go(r)
      }).asInstanceOf[FreeArrow[LabeledFnArrow, X, Y]]
    go(fp)
  }

  def fromFlow[A, B](flow: PipelineFlow.Flow[A, B]): Spine[A, B] =
    flow.graph

  def toFnArrow[A, B](spine: Spine[A, B]): FnArrow[A, B] =
    foldNodes(spine)

  def toPipe[A, B](spine: Spine[A, B]): Pipe[A, B] =
    run(spine)

  def liftExecution[A, B](spine: Spine[A, B]): FreeArrow[FnArrow, A, B] =
    FreeArrow.embed(foldNodes(spine))

  /** Best-effort wiring view from an analyzed [[ScanGraph]]. */
  def schemaToWiring(schema: ScanGraph, inputLabel: String): GraphRender.Wiring = {
    val edges = collectEdges(schema).distinct
    val outs  = (if edges.exists(_._1 == inputLabel) then Vector.empty else Vector(inputLabel)) ++
      collectOutputs(schema, edges).distinct
    (outs, edges)
  }

  private val nodeToFnArrow: BiFunctionK[Node, FnArrow] = new BiFunctionK[Node, FnArrow] {
    def apply[A, B](node: Node[A, B]): FnArrow[A, B] = node.arrow
  }

  private def collectEdges(schema: ScanGraph): Vector[(String, String)] = schema match {
    case ScanGraph.Edge(from, to)          => Vector(from -> to)
    case ScanGraph.Node(_, _, _, children) => children.flatMap(collectEdges).toVector
    case ScanGraph.Graph(nodes, edges)     => (nodes ++ edges).flatMap(collectEdges).toVector
    case ScanGraph.Empty                   => Vector.empty
  }

  private def collectOutputs(schema: ScanGraph, edges: Vector[(String, String)]): Vector[String] = {
    val targets = edges.map(_._2).toSet
    def names(g: ScanGraph): List[String] = g match {
      case ScanGraph.Node(name, _, out, _) if out > 0 => List(name)
      case ScanGraph.Node(_, _, _, children)          => children.flatMap(names)
      case ScanGraph.Graph(nodes, es)                 => (nodes ++ es).flatMap(names)
      case _                                          => Nil
    }
    names(schema).filterNot(targets.contains).toVector
  }
}
