package zio.pdf.arrow

import volga.free.{FreeProp, Nat, PropOb}
import zio.pdf.pipe.Pipe

/**
 * Kyo Flow–style fluent pipeline plans over [[PipelineGraph]].
 *
 * Agents describe *what* happens (named `.pipe` / `.output` / `.step` chains,
 * `.andThen`, `.zip`, `.dispatch`) as a pure plan; [[Flow.runLocal]] executes it.
 * Structure is analyzed to [[ScanGraph]] and rendered as Mermaid/DOT like
 * [kyo-flow](https://getkyo.io/latest/kyo-flow/) diagrams.
 *
 * Volga SMC wiring (`PipelineFlow.wiring(...)(prop.ofN { ... })`) is for port-level graphs.
 */
object PipelineFlow {

  final case class Flow[In, Out](
      name:       String,
      graph:      FreeArrow[PipelineGraph.Node, In, Out],
      inputLabel: String = "in"
  ) {
    def plan: GraphFormats =
      PipelineGraph.renderGraph(name, graph, inputLabel)

    def schema: ScanGraph =
      PipelineGraph.analyze(graph)

    def runLocal(in: In): Out =
      PipelineGraph.run(graph).run(in)

    def renderMermaid: String = plan.mermaid
    def renderDot: String     = plan.dot

    infix def andThen[Out2](that: Flow[Out, Out2]): Flow[In, Out2] =
      Flow(
        name       = s"$name>>>${that.name}",
        graph      = graph >>> that.graph,
        inputLabel = inputLabel
      )
  }

  def init(name: String): Init =
    Init(name)

  def andThen[In, Mid, Out](left: Flow[In, Mid], right: Flow[Mid, Out]): Flow[In, Out] =
    left.andThen(right)

  /** Parallel on shared input (Kyo Flow `.zip`) — honest fan-out in the AST. */
  def zip[In, A, B](left: Flow[In, A], right: Flow[In, B]): Flow[In, (A, B)] =
    Flow(
      name       = s"${left.name}+${right.name}",
      graph      = FreeArrow.fanout(left.graph, right.graph),
      inputLabel = left.inputLabel
    )

  def fanout[In, A, B](left: Flow[In, A], right: Flow[In, B]): Flow[In, (A, B)] =
    zip(left, right)

  def choice[In, L, R, Out](
      name:       String,
      inputLabel: String = "in",
      split:      Pipe[In, Either[L, R]],
      onLeft:     Pipe[L, Out],
      onRight:    Pipe[R, Out]
  ): Flow[In, Out] = {
    val branch = Pipe[Either[L, R], Out](_.fold(onLeft.run, onRight.run))
    Flow(
      name       = name,
      graph      = PipelineGraph.node(s"$name-split", 1, 1)(split) >>>
        PipelineGraph.node(s"$name-branch", 1, 1)(branch),
      inputLabel = inputLabel
    )
  }

  /** Kyo Flow `.dispatch` — first matching predicate wins; `otherwise` is required. */
  def dispatch[In, Out](name: String, inputLabel: String = "in"): DispatchBuilder[In, Out] =
    new DispatchBuilder(name, inputLabel, Nil)

  final class DispatchBuilder[In, Out] private[PipelineFlow] (
      name:       String,
      inputLabel: String,
      whens:      List[(String, In => Boolean, Pipe[In, Out])]
  ) {
    def when(test: In => Boolean, branchName: String)(pipe: Pipe[In, Out]): DispatchBuilder[In, Out] =
      new DispatchBuilder(name, inputLabel, whens :+ (branchName, test, pipe))

    def otherwise(branchName: String)(fallback: Pipe[In, Out]): Flow[In, Out] = {
      val merged = Pipe[In, Out] { in =>
        whens.find(_._2(in)) match {
          case Some((_, _, p)) => p.run(in)
          case None            => fallback.run(in)
        }
      }
      Flow(
        name       = name,
        graph      = PipelineGraph.node(s"$name-$branchName", 1, 1)(merged),
        inputLabel = inputLabel
      )
    }
  }

  /** Volga SMC wiring (port graphs). */
  def wiring[I: Nat, J: Nat](title: String, inputs: Nat.Vec[I, String])(
      build: volga.syntax.smc.Syntax[ArrowSyntax.Diag, PropOb, Nat.Plus] => FreeProp[ArrowSyntax.Label, I, J]
  ): GraphFormats =
    PipelineGraph.renderProp(title, build(PipelineGraph.wiringProp), inputs)

  final class Init private[PipelineFlow] (val name: String) {
    def input[In](label: String = "in"): Start[In] =
      Start(name, label)
  }

  final class Start[In] private[PipelineFlow] (name: String, inputLabel: String) {
    def pipe[Out](stepName: String, inPorts: Int = 1, outPorts: Int = 1)(p: Pipe[In, Out]): Builder[In, Out] =
      Builder(name, inputLabel, PipelineGraph.node(stepName, inPorts, outPorts)(p))

    def output[Out](stepName: String, inPorts: Int = 1, outPorts: Int = 1)(p: Pipe[In, Out]): Builder[In, Out] =
      pipe(stepName, inPorts, outPorts)(p)

    def step(stepName: String)(p: Pipe[In, In]): Builder[In, In] =
      pipe(stepName)(p)
  }

  final class Builder[In, Cur] private[PipelineFlow] (
      name:       String,
      inputLabel: String,
      graph:      FreeArrow[PipelineGraph.Node, In, Cur]
  ) {
    def pipe[Out](stepName: String, inPorts: Int = 1, outPorts: Int = 1)(p: Pipe[Cur, Out]): Builder[In, Out] =
      Builder(name, inputLabel, graph >>> PipelineGraph.node(stepName, inPorts, outPorts)(p))

    def output[Out](stepName: String, inPorts: Int = 1, outPorts: Int = 1)(p: Pipe[Cur, Out]): Builder[In, Out] =
      pipe(stepName, inPorts, outPorts)(p)

    def step(stepName: String)(p: Pipe[Cur, Cur]): Builder[In, Cur] =
      pipe(stepName)(p)

    def build: Flow[In, Cur] =
      Flow(name, graph, inputLabel)
  }
}
