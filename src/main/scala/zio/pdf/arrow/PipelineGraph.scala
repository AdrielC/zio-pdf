package zio.pdf.arrow

import volga.free.{FreeProp, Nat, PropOb}
import volga.SymmetricCat
import zio.pdf.pipe.{FreePipe, Pipe}

/**
 * Unified pipeline graph — one spine for execution ([[Pipe]] / [[FnArrow]]),
 * analysis ([[ScanGraph]]), and volga wiring ([[ArrowSyntax.Diag]]).
 */
object PipelineGraph {

  type Node[A, B] = LabeledFnArrow[A, B]

  def node[A, B](name: String, inPorts: Int, outPorts: Int)(pipe: Pipe[A, B]): FreeArrow[Node, A, B] =
    FreeArrow.embedLabeled(FnArrow.fromPipe(pipe).labeled(name, inPorts, outPorts))

  def nodeFn[A, B](name: String, inPorts: Int, outPorts: Int)(arrow: FnArrow[A, B]): FreeArrow[Node, A, B] =
    FreeArrow.embedLabeled(arrow.labeled(name, inPorts, outPorts))

  def toFnArrow[A, B](graph: FreeArrow[FnArrow, A, B]): FnArrow[A, B] =
    graph.flatCompile.foldMap(BiFunctionK.id[FnArrow])(using FnArrowCat.fnCategory)

  def toPipe[A, B](graph: FreeArrow[FnArrow, A, B]): Pipe[A, B] =
    Pipe(toFnArrow(graph).run)

  def run[A, B](graph: FreeArrow[Node, A, B]): Pipe[A, B] =
    Pipe(foldNodes(graph).run)

  def foldNodes[A, B](graph: FreeArrow[Node, A, B]): FnArrow[A, B] =
    graph.flatCompile.foldMap(nodeToFnArrow)(using FnArrowCat.fnCategory)

  def analyze[A, B](graph: FreeArrow[Node, A, B]): ScanGraph =
    graph.analyze

  /** Lift labeled nodes to plain [[FnArrow]] for execution. */
  def liftNodes[A, B](graph: FreeArrow[Node, A, B]): FreeArrow[FnArrow, A, B] =
    FreeArrow.embed(foldNodes(graph))

  private val nodeToFnArrow: BiFunctionK[Node, FnArrow] = new BiFunctionK[Node, FnArrow] {
    def apply[A, B](node: Node[A, B]): FnArrow[A, B] = node.arrow
  }

  /** [[FreePipe]] → [[FreeArrow]] (fused leaf — structure lives in [[FreePipe]] until folded). */
  def fromFreePipe[A, B](fp: FreePipe[A, B]): FreeArrow[FnArrow, A, B] =
    FreeArrow.embed(FnArrow.fromPipe(FreePipe.fold(fp)))

  def toFreePipe[A, B](graph: FreeArrow[FnArrow, A, B]): FreePipe[A, B] =
    FreePipe.Embed(toPipe(graph))

  /** Render a volga wiring diagram built from SMC syntax. */
  def renderProp[I: Nat, J: Nat](
      title:  String,
      graph:  FreeProp[ArrowSyntax.Label, I, J],
      inputs: Nat.Vec[I, String]
  ): GraphFormats =
    ArrowSyntax.render(title, graph, inputs)

  /** Durable schema + diagram formats from a labeled free arrow. */
  def renderGraph[A, B](title: String, graph: FreeArrow[Node, A, B], inputLabel: String): GraphFormats = {
    val schema = analyze(graph)
    val wiring = schemaToWiring(schema, inputLabel)
    GraphFormats(
      wiring   = wiring,
      plantUml = GraphRender.plantUml(title, wiring),
      mermaid  = GraphRender.mermaid(title, wiring),
      dot      = GraphRender.dot(title, wiring)
    )
  }

  /** Best-effort wiring view from an analyzed [[ScanGraph]]. */
  def schemaToWiring(schema: ScanGraph, inputLabel: String): GraphRender.Wiring = {
    val edges = collectEdges(schema).distinct
    val outs  = (if edges.exists(_._1 == inputLabel) then Vector.empty else Vector(inputLabel)) ++
      collectOutputs(schema, edges).distinct
    (outs, edges)
  }

  private def collectEdges(schema: ScanGraph): Vector[(String, String)] = schema match {
    case ScanGraph.Edge(from, to)                => Vector(from -> to)
    case ScanGraph.Node(_, _, _, children)       => children.flatMap(collectEdges).toVector
    case ScanGraph.Graph(nodes, edges)           => (nodes ++ edges).flatMap(collectEdges).toVector
    case ScanGraph.Empty                         => Vector.empty
  }

  private def collectOutputs(schema: ScanGraph, edges: Vector[(String, String)]): Vector[String] = {
    val targets = edges.map(_._2).toSet
    def names(g: ScanGraph): List[String] = g match {
      case ScanGraph.Node(name, _, out, _) if out > 0 => List(name)
      case ScanGraph.Node(_, _, _, children)          => children.flatMap(names)
      case ScanGraph.Graph(nodes, edges)              => (nodes ++ edges).flatMap(names)
      case _                                          => Nil
    }
    names(schema).filterNot(targets.contains).toVector
  }

  given wiringCat: SymmetricCat[ArrowSyntax.Diag, PropOb] = FreeProp.propCat[ArrowSyntax.Label]
  val wiringProp: volga.syntax.smc.Syntax[ArrowSyntax.Diag, PropOb, Nat.Plus] =
    ArrowSyntax.prop(using wiringCat)
}
