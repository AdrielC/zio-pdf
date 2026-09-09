package zio.pdf.arrow

/** Analyze fragment — nodes, edges, and open port names for sequential / parallel glue. */
final case class GraphSummary(
    graph:   ScanGraph = ScanGraph.Empty,
    inputs:  List[String] = Nil,
    outputs: List[String] = Nil
)

object GraphSummary {

  /** Synthetic port for shared fan-out input (remapped to upstream at render). */
  val InputPort: String = "⟨in⟩"

  /** Synthetic port for shared fan-in output (remapped to downstream at render). */
  val JoinPort: String = "⟨join⟩"

  /** Visible arrow combinator nodes (Hughes / volga FreeCat naming). */
  val FanOp: String   = "&&&"
  val MergeOp: String = "|||"
  val TestOp: String  = "?"

  def empty: GraphSummary = GraphSummary()

  def fromLabeled(node: LabeledFnArrow[?, ?]): GraphSummary = {
    val n = ScanGraph.node(node.name, node.inPorts, node.outPorts)
    GraphSummary(n, List(node.name), List(node.name))
  }

  /** Combinator-only fragment (zero ports — wiring carries the semantics). */
  def combinator(name: String): GraphSummary =
    GraphSummary(ScanGraph.node(name, 0, 0), List(name), List(name))

  /** Sequential composition — wire upstream outputs into downstream inputs. */
  def seq(left: GraphSummary, right: GraphSummary): GraphSummary = {
    val edges = for {
      from <- left.outputs
      to   <- right.inputs
    } yield ScanGraph.edge(from, to)
    val edgeGraph = edges.foldLeft(ScanGraph.Empty: ScanGraph)(ScanGraph.combine)
    GraphSummary(
      graph   = ScanGraph.combine(ScanGraph.combine(left.graph, edgeGraph), right.graph),
      inputs  = if left.inputs.nonEmpty then left.inputs else right.inputs,
      outputs = if right.outputs.nonEmpty then right.outputs else left.outputs
    )
  }

  /** Independent parallel composition (tensor / `***`). */
  def par(left: GraphSummary, right: GraphSummary): GraphSummary =
    GraphSummary(
      graph   = ScanGraph.combine(left.graph, right.graph),
      inputs  = left.inputs ++ right.inputs,
      outputs = left.outputs ++ right.outputs
    )

  /** Cartesian fan-out — fork → [[FanOp]] → branch heads (arrows `&&&`). */
  def fanout(left: GraphSummary, right: GraphSummary, fork: String): GraphSummary = {
    def expandInputs(summary: GraphSummary): List[String] =
      summary.inputs.flatMap {
        case `InputPort` => portTargets(summary.graph, InputPort).filterNot(_ == fork)
        case name        => List(name)
      }
    val branches = (expandInputs(left) ++ expandInputs(right)).distinct.filterNot(_ == fork)
    val fanNode  = combinator(FanOp)
    val forkEdges =
      Vector(ScanGraph.edge(fork, FanOp)) ++ branches.map(b => ScanGraph.edge(FanOp, b))
    val forkGraph = forkEdges.foldLeft(fanNode.graph: ScanGraph)(ScanGraph.combine)
    GraphSummary(
      graph   = ScanGraph.combine(forkGraph, ScanGraph.combine(left.graph, right.graph)),
      inputs  = List(fork),
      outputs = left.outputs ++ right.outputs
    )
  }

  /**
   * Coproduct merge — branch tips → [[MergeOp]] → shared sink (arrows `|||`).
   * Mirrors [[fanout]] but fans in to one downstream input.
   */
  def fanin(left: GraphSummary, right: GraphSummary, sink: String = MergeOp): GraphSummary = {
    val sources = (left.outputs ++ right.outputs).distinct.filterNot(s => s == sink || s == MergeOp)
    val mergeNode = combinator(MergeOp)
    val joinEdges = sources.map(s => ScanGraph.edge(s, MergeOp)) ++
      (if sink != MergeOp then Vector(ScanGraph.edge(MergeOp, sink)) else Vector.empty)
    val joinGraph = joinEdges.foldLeft(mergeNode.graph: ScanGraph)(ScanGraph.combine)
    GraphSummary(
      graph   = ScanGraph.combine(joinGraph, ScanGraph.combine(left.graph, right.graph)),
      inputs  = left.inputs ++ right.inputs,
      outputs = if sink != MergeOp then List(sink) else List(MergeOp)
    )
  }

  def toScanGraph(summary: GraphSummary): ScanGraph = summary.graph

  private def portTargets(graph: ScanGraph, port: String): List[String] = graph match {
    case ScanGraph.Edge(from, to) if from == port => List(to)
    case ScanGraph.Edge(_, _)                     => Nil
    case ScanGraph.Node(_, _, _, children)        => children.flatMap(portTargets(_, port))
    case ScanGraph.Graph(nodes, edges)            => (nodes ++ edges).flatMap(portTargets(_, port))
    case ScanGraph.Empty                          => Nil
  }
}
