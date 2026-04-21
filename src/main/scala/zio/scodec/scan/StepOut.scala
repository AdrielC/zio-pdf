/*
 * Step output cardinality for scan-style primitives (design: emit `O`,
 * not `Chunk[O]`, at the primitive layer; stream layers batch upstream).
 *
 * Runtime representation:
 *   - zero outputs     → `null`
 *   - one output       → bare `O` ([[One]] erases to `O`)
 *   - multiple outputs → `Chunk[O]` with size ≥ 1
 */

package zio.scodec.scan

import zio.Chunk

/** One or more outputs; erases to `O | Chunk[O]`. */
opaque type NonEmpty[+O] = O | Chunk[O]

/** Exactly one output; erases to `O`. */
opaque type One[+O] <: NonEmpty[O] = O

object One {
  inline def apply[O](o: O): One[O] = o
  inline def unwrap[O](one: One[O]): O = one
}

/** Union of zero ([[scala.Null]]), one, or nonempty many. */
type StepOut[+O] = Null | NonEmpty[O]

object StepOut {

  def toChunk[O](out: StepOut[O]): Chunk[O] =
    if (out == null) Chunk.empty
    else
      out.asInstanceOf[NonEmpty[O]] match {
        case c: Chunk[?] => c.asInstanceOf[Chunk[O]]
        case o           => Chunk.single(o.asInstanceOf[O])
      }
}
