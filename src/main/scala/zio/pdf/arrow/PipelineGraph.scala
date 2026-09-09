package zio.pdf.arrow

import volga.free.{FreeProp, Nat, PropOb}
import volga.SymmetricCat
import zio.pdf.pipe.{FreePipe, Pipe}

/**
 * Unified pipeline graph — delegates to [[PipelineSpine]] for the single free-arrow
 * AST; keeps volga SMC wiring helpers here for backward compatibility.
 */
object PipelineGraph {

  type Node[A, B] = PipelineSpine.Node[A, B]

  def node[A, B](name: String, inPorts: Int, outPorts: Int)(pipe: Pipe[A, B]): FreeArrow[Node, A, B] =
    PipelineSpine.node(name, inPorts, outPorts)(pipe)

  def nodeFn[A, B](name: String, inPorts: Int, outPorts: Int)(arrow: FnArrow[A, B]): FreeArrow[Node, A, B] =
    PipelineSpine.nodeFn(name, inPorts, outPorts)(arrow)

  def toFnArrow[A, B](graph: FreeArrow[FnArrow, A, B]): FnArrow[A, B] =
    graph.flatCompile.foldMap(BiFunctionK.id[FnArrow])(using FnArrowCat.fnCategory)

  def toPipe[A, B](graph: FreeArrow[FnArrow, A, B]): Pipe[A, B] =
    Pipe(toFnArrow(graph).run)

  def run[A, B](graph: FreeArrow[Node, A, B]): Pipe[A, B] =
    PipelineSpine.run(graph)

  def foldNodes[A, B](graph: FreeArrow[Node, A, B]): FnArrow[A, B] =
    PipelineSpine.foldNodes(graph)

  def analyze[A, B](graph: FreeArrow[Node, A, B]): ScanGraph =
    PipelineSpine.analyze(graph)

  def liftNodes[A, B](graph: FreeArrow[Node, A, B]): FreeArrow[FnArrow, A, B] =
    PipelineSpine.liftExecution(graph)

  /** [[FreePipe]] → labeled [[FreeArrow]] (structure preserved for analyze). */
  def fromFreePipe[A, B](fp: FreePipe[A, B]): FreeArrow[Node, A, B] =
    PipelineSpine.fromFreePipe(fp)

  def toFreePipe[A, B](graph: FreeArrow[FnArrow, A, B]): FreePipe[A, B] =
    FreePipe.Embed(toPipe(graph))

  def renderProp[I: Nat, J: Nat](
      title:  String,
      graph:  FreeProp[ArrowSyntax.Label, I, J],
      inputs: Nat.Vec[I, String]
  ): GraphFormats =
    ArrowSyntax.render(title, graph, inputs.toVector*)

  def renderGraph[A, B](title: String, graph: FreeArrow[Node, A, B], inputLabel: String): GraphFormats =
    PipelineSpine.render(title, graph, inputLabel)

  def schemaToWiring(schema: ScanGraph, inputLabel: String): GraphRender.Wiring =
    PipelineSpine.schemaToWiring(schema, inputLabel)

  given wiringCat: SymmetricCat[ArrowSyntax.Diag, PropOb] = FreeProp.propCat[ArrowSyntax.Label]
  val wiringProp: volga.syntax.smc.Syntax[ArrowSyntax.Diag, PropOb, Nat.Plus] =
    ArrowSyntax.prop(using wiringCat)
}
