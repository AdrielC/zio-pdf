package zio.graviton.scan

import java.security.MessageDigest
import kyo.*
import kyo.kernel.Loop

object ScanPrograms:

  def mapProg[I, O](f: I => O)(using
      pollTag: Tag[Poll[I]],
      emitTag: Tag[Emit[O]],
      doneTag: Tag[Abort[ScanTerminal]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): ScanProg[I, O, Nothing, Any] =
    Loop.forever:
      Poll.andMap[I]:
        case Absent     => Abort.fail(ScanTerminal.success)
        case Present(i) => Emit.value(f(i))

  def filterProg[A](pred: A => Boolean)(using
      pollTag: Tag[Poll[A]],
      emitTag: Tag[Emit[A]],
      doneTag: Tag[Abort[ScanTerminal]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): ScanProg[A, A, Nothing, Any] =
    Loop.forever:
      Poll.andMap[A]:
        case Absent => Abort.fail(ScanTerminal.success)
        case Present(a) =>
          if pred(a) then Emit.value(a)
          else ()

  def foldProg[I, S](seed: S)(step: (S, I) => S)(using
      pollTag: Tag[Poll[I]],
      doneTag: Tag[Abort[ScanTerminal]],
      varTag: Tag[Var[S]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): ScanProg[I, S, Nothing, Any] =
    Var.run(seed):
      Loop.forever:
        Poll.andMap[I]:
          case Absent =>
            Var.get[S].map: s =>
              Abort.fail(ScanTerminal.successWith(Seq(s)))
          case Present(i) =>
            Var.updateDiscard[S](s => step(s, i))

  def bombGuardProg(maxBytes: Long)(using
      pollTag: Tag[Poll[Byte]],
      emitTag: Tag[Emit[Byte]],
      doneTag: Tag[Abort[ScanTerminal]],
      varTag: Tag[Var[Long]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): ScanProg[Byte, Byte, BombError, Any] =
    Var.run(0L):
      Loop.forever:
        Poll.andMap[Byte]:
          case Absent => Abort.fail(ScanTerminal.success)
          case Present(b) =>
            Var.update[Long](_ + 1).map: n =>
              if n > maxBytes then
                Abort.fail(ScanTerminal.failWith(BombError(n, maxBytes), Seq.empty))
              else Emit.value(b)

  def hashProg(algo: HashAlgo)(using
      pollTag: Tag[Poll[Byte]],
      doneTag: Tag[Abort[ScanTerminal]],
      varTag: Tag[Var[MessageDigest]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): ScanProg[Byte, Byte, Nothing, Any] =
    Var.run(ScanPrim.newDigest(algo)):
      Loop.forever:
        Poll.andMap[Byte]:
          case Absent =>
            Var.get[MessageDigest].map: md =>
              val digest = md.digest()
              Abort.fail(ScanTerminal.successWith(digest.toSeq))
          case Present(b) =>
            Var.use[MessageDigest](md => { md.update(b); () })

  def takeProg[A](n: Int)(using
      pollTag: Tag[Poll[A]],
      emitTag: Tag[Emit[A]],
      doneTag: Tag[Abort[ScanTerminal]],
      varTag: Tag[Var[Int]],
      ct: ConcreteTag[ScanTerminal],
      frame: Frame
  ): ScanProg[A, A, Nothing, Any] =
    Var.run(n):
      Loop.forever:
        Poll.andMap[A]:
          case Absent => Abort.fail(ScanTerminal.success)
          case Present(a) =>
            Var.get[Int].map: rem =>
              if rem <= 0 then Abort.fail(ScanTerminal.stop)
              else
                Var.updateDiscard[Int](_ - 1).map: _ =>
                  Emit.value(a)
