package zio.graviton.scan

import kyo.*

inline def emitStep[O](out: StepOut[O])(using tag: Tag[Emit[O]], frame: Frame): Unit < Emit[O] =
  out match
    case null => ()
    case c: Chunk[?] =>
      val ch = c.asInstanceOf[Chunk[O]]
      Loop.indexed { i =>
        if i >= ch.length then Loop.done(())
        else Emit.value(ch(i)).map(_ => Loop.continue)
      }
    case o => Emit.value(o.asInstanceOf[O])
