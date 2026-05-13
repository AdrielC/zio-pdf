package zio.graviton.scan

import kyo.*
import kyo.kernel.Loop

/** Hand-written [[ScanProg]] building blocks (mirror the design doc). */
object ScanPrograms {

  /**
   * Kyo [[Abort]] failure uses the concrete [[ScanAbort]] wrapper (see its doc);
   * decode with [[ScanAbort.unwrap]] after [[ScanRunner.run]].
   */
  type ScanProg[I, O, S] =
    Unit < (Poll[I] & Emit[O] & Abort[ScanAbort] & S)

  def mapProg[I, O](f: I => O)(using
    pollTag: Tag[Poll[I]],
    emitTag: Tag[Emit[O]],
    doneTag: Tag[Abort[ScanAbort]],
    frame: Frame
  ): ScanProg[I, O, Any] =
    Loop.foreach {
      Poll.andMap[I] {
        case Absent =>
          Abort.fail(ScanAbort(ScanDone.success[O])).map(_ => Loop.done(()))
        case Present(i) =>
          Emit.value(f(i)).map(_ => Loop.continue)
      }
    }

  def filterProg[A](pred: A => Boolean)(using
    pollTag: Tag[Poll[A]],
    emitTag: Tag[Emit[A]],
    doneTag: Tag[Abort[ScanAbort]],
    frame: Frame
  ): ScanProg[A, A, Any] =
    Loop.foreach {
      Poll.andMap[A] {
        case Absent =>
          Abort.fail(ScanAbort(ScanDone.success[A])).map(_ => Loop.done(()))
        case Present(a) =>
          if pred(a) then Emit.value(a).map(_ => Loop.continue)
          else Loop.continue
      }
    }

  def foldProg[I, S](step: (S, I) => S)(using
    pollTag: Tag[Poll[I]],
    doneTag: Tag[Abort[ScanAbort]],
    varTag: Tag[Var[S]],
    frame: Frame
  ): ScanProg[I, S, Var[S]] =
    Loop.foreach {
      Poll.andMap[I] {
        case Absent =>
          Var.get[S].map { s =>
            Abort.fail(ScanAbort(ScanDone.successWith[S](Seq(s)))).map(_ => Loop.done(()))
          }
        case Present(i) =>
          Var.update[S](step(_, i)).map(_ => Loop.continue)
      }
    }

  def bombGuardProg(maxBytes: Long)(using
    pollTag: Tag[Poll[Byte]],
    emitTag: Tag[Emit[Byte]],
    doneTag: Tag[Abort[ScanAbort]],
    varTag: Tag[Var[Long]],
    frame: Frame
  ): ScanProg[Byte, Byte, Var[Long]] =
    Loop.foreach {
      Poll.andMap[Byte] {
        case Absent =>
          Abort.fail(ScanAbort(ScanDone.success[Byte])).map(_ => Loop.done(()))
        case Present(b) =>
          Var.update[Long](_ + 1).map { n =>
            if n > maxBytes then
              Abort.fail(ScanAbort(ScanDone.fail[Byte, BombError](BombError(n, maxBytes))))
                .map(_ => Loop.done(()))
            else Emit.value(b).map(_ => Loop.continue)
          }
      }
    }
}
