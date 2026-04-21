/*
 * Output cardinality types for a single scan step.
 *
 * The output of a scan step is *not* `Chunk[O]` -- it is a subtype the
 * compiler knows about statically:
 *
 *   - `Null`        -- zero outputs (literally `scala.Null`)
 *   - `One[O]`      -- exactly one output, opaque alias for `O`
 *   - `NonEmpty[O]` -- one or more, opaque alias for `O | Chunk[O]`
 *
 *      StepOut[O]
 *     /     |
 *  Null  NonEmpty[O]
 *             |
 *          One[O]    (One <: NonEmpty by opaque type bound)
 *
 * Why `scala.Null`? It already sits below every `AnyRef` in Scala's type
 * hierarchy, and a step result of `null` already means "no value flowed
 * out of this stage" -- exactly what the SMC wire model calls
 * "no wire on the output side".
 *
 * Runtime representation:
 *   - `Null`        -- the JVM `null` reference
 *   - `One[O]`      -- the bare `O` value (no boxing)
 *   - `NonEmpty[O]` -- either a bare `O` (when delegated through `One`)
 *                      or a non-empty `Chunk[O]`
 *
 * Distinguished at dispatch time by a Chunk class test.
 */

package zio.pdf.scan

import kyo.*

object StepOut {

  /** Zero outputs. Literally `scala.Null` -- already the bottom of `AnyRef`.
    * A step with output type `Null` never calls `Emit`. Zero allocation, zero
    * effect. */
  type Null = scala.Null

  /** The canonical zero-output value. */
  inline def empty: Null = null

  /** Exactly one output of type `O`. Opaque, erases to `O` on the JVM. No
    * boxing. `One[O] <: NonEmpty[O]` -- the `<:` bound makes "one" a special
    * case of "nonempty". */
  opaque type One[+O] <: NonEmpty[O] = O

  object One {
    def apply[O](o: O): One[O] = o

    extension [O](one: One[O]) def value: O = one
  }

  /** One or more outputs. Erases to `O | Chunk[O]` at runtime. Distinguished
    * at dispatch time by a `Chunk` class test. */
  opaque type NonEmpty[+O] = O | Chunk[O]

  object NonEmpty {

    /** Construct from a non-empty Chunk. Throws on empty input -- this is the
      * one place we tolerate a defensive throw because `NonEmpty(emptyChunk)`
      * is a static invariant violation, not a runtime error. */
    def fromChunkUnsafe[O](chunk: Chunk[O]): NonEmpty[O] = {
      require(chunk.nonEmpty, "NonEmpty cannot wrap an empty Chunk")
      chunk
    }

    /** Construct a `NonEmpty[O]` from a single value. */
    def one[O](o: O): NonEmpty[O] = One(o)

    /** Smart constructor: returns `Null` for empty chunks, `NonEmpty[O]`
      * otherwise. The result type sits in `StepOut[O]`. */
    def fromChunk[O](c: Chunk[O]): Null | NonEmpty[O] =
      if c.isEmpty then null else c

    extension [O](ne: NonEmpty[O]) {
      def toChunk: Chunk[O] = ne match {
        case c: Chunk[O @unchecked] => c
        case o                      => Chunk(o.asInstanceOf[O])
      }

      /** Number of outputs in this `NonEmpty`. */
      def size: Int = ne match {
        case c: Chunk[O @unchecked] => c.length
        case _                      => 1
      }
    }
  }

  /** The union type for step output. `One[O] <: NonEmpty[O]` already so this
    * simplifies to `Null | NonEmpty[O]`. */
  type StepOut[+O] = Null | NonEmpty[O]

  /** Convert any step output to a `Chunk[O]` (possibly empty). Useful at
    * boundaries where we hand off to chunk-oriented machinery. */
  def toChunk[O](s: StepOut[O]): Chunk[O] = s match {
    case null                   => Chunk.empty[O]
    case c: Chunk[O @unchecked] => c
    case o                      => Chunk(o.asInstanceOf[O])
  }

  /** Composition algebra at the *type* level. What comes out when you compose
    * a step emitting `OL` into a stage expecting `M`?
    *
    *   - left emits nothing          -> nothing downstream
    *   - left emits one              -> right runs once -> right's output
    *   - left emits many, right one  -> nonempty
    *   - left emits many, right many -> nonempty
    *   - right emits nothing         -> nothing downstream
    *
    * The compiler can use this to reduce a chain of pure `One`-emitting maps
    * to a single `One[O]` and fuse them. */
  type ComposeStep[OL, OR] = OL match {
    case Null    => Null
    case One[?]  => OR
    case NonEmpty[?] => OR match {
      case Null     => Null
      case One[r]   => NonEmpty[r]
      case NonEmpty[r] => NonEmpty[r]
    }
  }

  /** Runtime emit -- no boxing, no ADT allocation. Inlines into a single
    * pattern match at the use-site. */
  inline def emitStep[O](out: StepOut[O])(using inline tag: Tag[Emit[O]], inline frame: Frame): Unit < Emit[O] =
    out match {
      case null                   => Kyo.unit
      case c: Chunk[O @unchecked] =>
        // Many outputs: emit each in order.
        Kyo.foreachDiscard(c)(o => Emit.value(o))
      case o                      =>
        // One output: single emit.
        Emit.value(o.asInstanceOf[O])
    }
}
