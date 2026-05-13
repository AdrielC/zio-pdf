package zio.graviton.scan

import kyo.*
import kyo.Result as KResult

object ScanRunner {

  import ScanPrograms.ScanProg

  def run[I, O, S](inputs: kyo.Chunk[I])(
    prog: ScanProg[I, O, S]
  )(using
    pollTag: Tag[Poll[I]],
    emitTag: Tag[Emit[O]],
    doneTag: Tag[Abort[ScanAbort]],
    ct: ConcreteTag[ScanAbort],
    frame: Frame
  ): (ScanDone[O, ?], Seq[O]) < S = {
    val inner: (kyo.Chunk[O], KResult[ScanAbort, Unit]) < (Poll[I] & S) =
      Emit.run(Abort.run(prog))
    Poll.run(inputs)(inner).map { case (emitted, result) =>
      result match {
        case KResult.Success(_) =>
          (ScanDone.success[O], emitted)
        case KResult.Failure(box) =>
          val done = ScanAbort.unwrap[O, Any](box)
          val leftover = done match {
            case ScanDone.Success(l)    => l
            case ScanDone.Stop(l)       => l
            case ScanDone.Failure(_, l) => l
          }
          (done, emitted.toSeq ++ leftover)
        case KResult.Panic(t) =>
          throw t
      }
    }
  }

  def runStateful[I, O, VS, S](inputs: kyo.Chunk[I], seed: VS)(
    prog: ScanProg[I, O, Var[VS] & S]
  )(using
    pollTag: Tag[Poll[I]],
    emitTag: Tag[Emit[O]],
    doneTag: Tag[Abort[ScanAbort]],
    varTag: Tag[Var[VS]],
    ct: ConcreteTag[ScanAbort],
    frame: Frame
  ): (ScanDone[O, ?], Seq[O]) < S =
    Var.run(seed)(run(inputs)(prog))
}
