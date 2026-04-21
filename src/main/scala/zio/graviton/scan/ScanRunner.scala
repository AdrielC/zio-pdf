package zio.graviton.scan

import kyo.*
import kyo.Result.*

object ScanRunner:

  def run[I, O, E, S](inputs: Iterable[I])(prog: ScanProg[I, O, E, S])(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      doneTag: Tag[Abort[ScanTerminal]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): (ScanDone[O, E], Seq[O]) < S =
    val chunk = Chunk.from(inputs)
    val inner =
      Emit.run:
        Abort.run:
          prog
    Poll.run(chunk)(inner).map { case (emitted, res) =>
      res match
        case Success(_) =>
          (ScanDone.success[O], emitted.toSeq)
        case Failure(term: ScanTerminal) =>
          val done = ScanTerminal.toTyped[O, E](term)
          val leftover = term match
            case ScanTerminal.Success(l)    => l
            case ScanTerminal.Stop(l)       => l
            case ScanTerminal.Failure(_, l) => l
          (done, emitted.toSeq ++ leftover.asInstanceOf[Seq[O]])
        case Panic(t) => throw t
    }

  def runStateful[I, O, E, VS, S](inputs: Iterable[I], seed: VS)(
      prog: ScanProg[I, O, E, Var[VS] & S]
  )(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      doneTag: Tag[Abort[ScanTerminal]],
      varTag: Tag[Var[VS]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): (ScanDone[O, E], Seq[O]) < S =
    run(inputs)(Var.run(seed)(prog))
