package zio.pdf.arrow

/** Analyze fragment — nodes, edges, and open port names for sequential / parallel glue. */
final case class GraphSummary(
    graph:   ScanGraph = ScanGraph.Empty,
    inputs:  List[String] = Nil,
    outputs: List[String] = Nil
)

object GraphSummary {

  def empty: GraphSummary = GraphSummary()

  def fromLabeled(node: LabeledFnArrow[?, ?]): GraphSummary = {
    val n = ScanGraph.node(node.name, node.inPorts, node.outPorts)
    GraphSummary(n, List(node.name), List(node.name))
  }

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

  /** Cartesian fan-out — shared upstream feeds both arms. */
  def fanout(left: GraphSummary, right: GraphSummary, fork: String): GraphSummary = {
    val forkEdges = (left.inputs ++ right.inputs).distinct.map(to => ScanGraph.edge(fork, to))
    val forkGraph = forkEdges.foldLeft(ScanGraph.Empty: ScanGraph)(ScanGraph.combine)
    GraphSummary(
      graph   = ScanGraph.combine(forkGraph, ScanGraph.combine(left.graph, right.graph)),
      inputs  = List(fork),
      outputs = left.outputs ++ right.outputs
    )
  }

  def toScanGraph(summary: GraphSummary): ScanGraph = summary.graph
}
