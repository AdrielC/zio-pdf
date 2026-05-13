package zio.graviton.scan

import kyo.{Chunk as KChunk, *}

/** Zero outputs — use the literal `null` value (subtype of every reference type). */
type Null = scala.Null

/** Exactly one output; erases to `O` on the JVM. */
opaque type One[+O] <: NonEmpty[O] = O

object One {
  inline def apply[O](o: O): One[O]                    = o
  extension [O](one: One[O]) inline def value: O = one
}

/** One or more outputs; erases to `O | KChunk[O]` (single value or nonempty chunk). */
opaque type NonEmpty[+O] = O | KChunk[O]

object NonEmpty {
  def apply[O](chunk: KChunk[O]): NonEmpty[O] = {
    require(chunk.nonEmpty)
    chunk
  }

  inline def one[O](o: O): NonEmpty[O] = One(o)

  def fromChunk[O](c: KChunk[O]): NonEmpty[O] | Null =
    if c.isEmpty then null else c

  extension [O](ne: NonEmpty[O]) {
    def toChunk: KChunk[O] =
      ne match {
        case c: KChunk[?] => c.asInstanceOf[KChunk[O]]
        case o            => KChunk(o.asInstanceOf[O])
      }
  }
}

/** Union of zero, one, or many outputs per step. */
type StepOut[+O] = Null | NonEmpty[O]

object StepOut {

  /** Collect a step result into a (possibly empty) Kyo chunk. */
  def toKChunk[O](out: StepOut[O]): KChunk[O] =
    if out == null then KChunk.empty
    else
      out.asInstanceOf[NonEmpty[O]] match {
        case c: KChunk[?] => c.asInstanceOf[KChunk[O]]
        case o            => KChunk(o.asInstanceOf[O])
      }

  /** Emit according to cardinality (Kyo `Emit`). */
  def emitStep[O](out: StepOut[O])(using tag: Tag[Emit[O]], frame: Frame): Unit < Emit[O] =
    out match {
      case null => ()
      case c: KChunk[?] =>
        emitChunkIndex(c.asInstanceOf[KChunk[O]], 0)
      case o =>
        Emit.value(o.asInstanceOf[O])
    }

  private def emitChunkIndex[O](c: KChunk[O], i: Int)(using tag: Tag[Emit[O]], frame: Frame): Unit < Emit[O] =
    if i >= c.length then ()
    else Emit.valueWith(c(i))(emitChunkIndex(c, i + 1))
}

/** Compose output cardinality when wiring `left` into `right`. */
type ComposeStep[OL, OR] <: StepOut[?] = OL match {
  case Null => Null
  case One[?] => OR
  case NonEmpty[?] => OR match {
      case Null        => Null
      case One[r]      => NonEmpty[r]
      case NonEmpty[r] => NonEmpty[r]
    }
}
