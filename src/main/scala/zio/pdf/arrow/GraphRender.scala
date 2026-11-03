package zio.pdf.arrow

import volga.free.{FreeProp, Nat, PropOb}
import volga.SymmetricCat

/** Render wiring extracted via volga [[FreeProp.link]]. */
object GraphRender {

  type Wiring = (Vector[String], Vector[(String, String)])

  /** Extract inbound→outbound port wiring from a free prop diagram. */
  def wiring[I: Nat, J: Nat](
      graph:  FreeProp[ArrowSyntax.Label, I, J],
      inputs: Nat.Vec[I, String]
  ): Wiring = {
    val (outs, edges) = graph.link[String, ArrowSyntax.Label](inputs)
    (outs.toVector, edges)
  }

  /** PlantUML component diagram (volga `DiagTest` format). */
  def plantUml(title: String, wiring: Wiring): String = {
    val (outs, edges) = wiring
    val edgeLines = edges.map { case (from, to) => s"[$from] --> [$to]" }
    val outLines  = outs.zipWithIndex.map { case (out, i) => s"[$out] --> (out$i)" }
    List(
      "@startuml",
      s"title $title",
      "!pragma layout smetana",
      (edgeLines ++ outLines).mkString("\n"),
      "@enduml"
    ).mkString("\n")
  }

  /** Mermaid flowchart (GitHub-renderable). */
  def mermaid(title: String, wiring: Wiring): String = {
    val (outs, edges) = wiring
    val connected = edges.iterator.flatMap { case (from, to) => Iterator(from, to) }.toSet
    val isolated = outs.distinct.filterNot(connected.contains).map(name => s"  $name")
    val body = (isolated ++ edges.map { case (from, to) => s"  $from --> $to" }).mkString("\n")
    s"""flowchart LR
       |%% $title
       |$body""".stripMargin
  }

  /** Dot/graphviz digraph. */
  def dot(title: String, wiring: Wiring): String = {
    val (outs, edges) = wiring
    val connected = edges.iterator.flatMap { case (from, to) => Iterator(from, to) }.toSet
    val isolated = outs.distinct.filterNot(connected.contains).map(name => s"""  "$name";""")
    val body = (isolated ++ edges.map { case (from, to) => s"""  "$from" -> "$to";""" }).mkString("\n")
    s"""digraph "${title.replace("\"", "\\\"")}" {
       |$body
       |}""".stripMargin
  }
}

/** volga SMC syntax + wiring helpers for zio-pdf arrows. */
object ArrowSyntax {

  /** Wiring label quiver — `Label[A, B] = String` (volga diag convention). */
  type Label[A, B] = String

  type Diag[A, B] = FreeProp[Label, A, B]

  type DAG[N <: Int, M <: Int] = FreeProp[Label, Nat.OfInt[N], Nat.OfInt[M]]

  def node(name: String, inPorts: Int, outPorts: Int): DAG[inPorts.type, outPorts.type] =
    FreeProp.Embed(name)

  /** volga SMC syntax — write pipelines as `prop.ofN { ... }` blocks. */
  def prop(using U: SymmetricCat[Diag, PropOb]) =
    volga.syntax.smc.syntax[Diag, PropOb, Nat.Plus]

  def render[I: Nat, J: Nat](
      title:  String,
      graph:  FreeProp[Label, I, J],
      inputs: Nat.Vec[I, String]
  ): GraphFormats = {
    val w = GraphRender.wiring(graph, inputs)
    GraphFormats(
      wiring   = w,
      plantUml = GraphRender.plantUml(title, w),
      mermaid  = GraphRender.mermaid(title, w),
      dot      = GraphRender.dot(title, w)
    )
  }
}

final case class GraphFormats(
    wiring:   GraphRender.Wiring,
    plantUml: String,
    mermaid:  String,
    dot:      String
)
