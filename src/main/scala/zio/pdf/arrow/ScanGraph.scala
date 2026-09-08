package zio.pdf.arrow

import zio.blocks.schema.Schema

/** Invariant graph summary for `FreeArrow.analyze` / durable scan schemas. */
enum ScanGraph derives CanEqual, Schema {
  case Empty
  case Node(name: String, inPorts: Int, outPorts: Int, children: List[ScanGraph])
  case Edge(from: String, to: String)
  case Graph(nodes: List[ScanGraph], edges: List[ScanGraph])
}

object ScanGraph {

  def empty: ScanGraph = Empty

  def node(name: String, inPorts: Int, outPorts: Int): ScanGraph =
    Node(name, inPorts, outPorts, Nil)

  def edge(from: String, to: String): ScanGraph = Edge(from, to)

  def fromWiring(wiring: GraphRender.Wiring): ScanGraph = {
    val (outs, edges) = wiring
    val edgeNodes     = edges.map { case (from, to) => edge(from, to) }
    val outNodes      = outs.map(out => node(out, 0, 1))
    edgeNodes.foldLeft(combine(outNodes.foldLeft(Empty)(combine), Empty))(combine)
  }

  /** Monoid-ish combine for analyze folds. */
  def combine(left: ScanGraph, right: ScanGraph): ScanGraph = (left, right) match {
    case (Empty, r)                         => r
    case (l, Empty)                         => l
    case (Graph(ns1, es1), Graph(ns2, es2)) => Graph(ns1 ++ ns2, es1 ++ es2)
    case (l, r)                             => Graph(List(l, r), Nil)
  }

  def containsNode(schema: ScanGraph, name: String): Boolean = schema match {
    case Node(n, _, _, _) if n == name     => true
    case Node(_, _, _, children)           => children.exists(containsNode(_, name))
    case Graph(nodes, edges)               => (nodes ++ edges).exists(containsNode(_, name))
    case Empty | Edge(_, _)                => false
  }

  /** All node names appearing in a schema (for agent / tacit summaries). */
  def nodeNames(schema: ScanGraph): Vector[String] = {
    def collect(g: ScanGraph): Vector[String] = g match {
      case Node(name, _, _, children) => Vector(name) ++ children.flatMap(collect)
      case Graph(nodes, edges)        => (nodes ++ edges).flatMap(collect).toVector
      case Empty | Edge(_, _)         => Vector.empty
    }
    collect(schema).distinct
  }

  given scanGraphMonoid: ScanGraphMonoid with {
    def empty: ScanGraph                               = ScanGraph.empty
    def combine(a: ScanGraph, b: ScanGraph): ScanGraph = ScanGraph.combine(a, b)
  }
}

trait ScanGraphMonoid {
  def empty: ScanGraph
  def combine(a: ScanGraph, b: ScanGraph): ScanGraph
}
