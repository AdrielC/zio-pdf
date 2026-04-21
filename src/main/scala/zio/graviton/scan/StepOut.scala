package zio.graviton.scan

import kyo.Chunk

/** Static output cardinality for one scan step (Volga-style wiring). */
type Null = scala.Null

val Null: Null = null

opaque type One[+O] <: NonEmpty[O] = O

object One:
  def apply[O](o: O): One[O]          = o
  extension [O](one: One[O]) def value: O = one

/** One or more outputs; at runtime either a single `O` or a non-empty `Chunk[O]`. */
opaque type NonEmpty[+O] = O | Chunk[O]

object NonEmpty:
  def apply[O](chunk: Chunk[O]): NonEmpty[O] =
    require(chunk.nonEmpty)
    chunk

  def one[O](o: O): NonEmpty[O] = One(o)

  def fromChunk[O](c: Chunk[O]): NonEmpty[O] | Null =
    if c.isEmpty then Null else c

  extension [O](ne: NonEmpty[O])
    def toChunk: Chunk[O] =
      ne match
        case c: Chunk[O @unchecked] => c
        case o                        => Chunk.from(List(o.asInstanceOf[O]))

/** Union of step outputs: nothing, or non-empty emission. */
type StepOut[+O] = Null | NonEmpty[O]

type ComposeStep[OL, OR] = OL match
  case Null        => Null
  case One[?]      => OR
  case NonEmpty[?] => OR match
    case Null        => Null
    case One[r]      => NonEmpty[r]
    case NonEmpty[r] => NonEmpty[r]
