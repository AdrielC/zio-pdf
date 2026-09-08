package zio.pdf.arrow

/** Invariant graph summary for `FreeArrow.analyze` / durable scan schemas. */
enum ScanGraph derives CanEqual {
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

  /** Monoid-ish combine for analyze folds. */
  def combine(left: ScanGraph, right: ScanGraph): ScanGraph = (left, right) match {
    case (Empty, r)                  => r
    case (l, Empty)                  => l
    case (Graph(ns1, es1), Graph(ns2, es2)) => Graph(ns1 ++ ns2, es1 ++ es2)
    case (l, r)                      => Graph(List(l, r), Nil)
  }

  given scanGraphMonoid: ScanGraphMonoid with {
    def empty: ScanGraph                       = ScanGraph.empty
    def combine(a: ScanGraph, b: ScanGraph): ScanGraph = ScanGraph.combine(a, b)
  }
}

trait ScanGraphMonoid {
  def empty: ScanGraph
  def combine(a: ScanGraph, b: ScanGraph): ScanGraph
}
