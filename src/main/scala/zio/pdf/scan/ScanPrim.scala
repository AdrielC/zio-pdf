/*
 * The primitive algebra. Every constructor is a case class or case object,
 * so the value is fully describable as data. Pure-function fields (`Map.f`,
 * `Filter.p`, `Fold.step`) are *not* serialisable on their own -- in
 * production they live behind a registry of named transforms (a
 * `SchemaExpr`-like layer), and the constructors here represent that
 * compiled view. The serialisable form for "named primitives" (Hash,
 * CountBytes, FastCDC, FixedChunk, BombGuard, Take) is fully captured.
 *
 * A `ScanPrim[I, O]` describes "consume one `I`, produce a `StepOut[O]`"
 * plus optional terminal effects (Var state, Abort completion). The
 * interpreter in `SinglePassInterp` lowers it into an actual Kyo
 * `ScanProg`.
 */

package zio.pdf.scan

import StepOut.*

/** A scan primitive. The `Out` parameter is one of:
  *
  *   - `Null`               -- the step never emits per input
  *   - `One[O]`             -- the step emits exactly one `O` per input
  *   - `NonEmpty[O]`        -- the step emits one or more `O` per input
  *   - `Null | One[O]`      -- zero or one (filter)
  *   - `Null | NonEmpty[O]` -- zero or many (chunker)
  *
  * The interpreter dispatches on the static cardinality to fuse maps with
  * adjacent stages and skip `Emit` entirely for `Null`-typed steps. */
sealed trait ScanPrim[-I, +Out] extends Product with Serializable

object ScanPrim {

  /** Pure, stateless. Fuses with adjacent `Map`s at interpretation time. */
  final case class Map[I, O](f: I => O) extends ScanPrim[I, One[O]]

  /** Zero or one output per input. Never fuses (cardinality is dynamic). */
  final case class Filter[A](p: A => Boolean) extends ScanPrim[A, Null | One[A]]

  /** One output per input while count < n, then signals
    * `Abort[ScanDone.Stop]`. The static step type is `One[A]`; the stop
    * fires via Abort once the budget is exhausted. */
  final case class Take[A](n: Int) extends ScanPrim[A, One[A]]

  /** Drops the first `n` inputs, then becomes identity. */
  final case class Drop[A](n: Int) extends ScanPrim[A, Null | One[A]]

  /** Stateful fold. Emits nothing per input; the final accumulator is
    * delivered as the leftover of `ScanDone.Success`. */
  final case class Fold[I, S](seed: S, step: (S, I) => S) extends ScanPrim[I, Null]

  /** Accumulates into a `MessageDigest`; emits nothing per byte. The digest
    * is delivered as `ScanDone.Success(leftover = digestBytes)`. */
  final case class Hash(algo: HashAlgo) extends ScanPrim[Byte, Null]

  /** Counts bytes; emits nothing per byte. The count, big-endian, is the
    * `ScanDone.Success.leftover`. Caller can decode it as a `Long`. */
  case object CountBytes extends ScanPrim[Byte, Null]

  /** Passes bytes through; aborts with `BombError` once the byte budget is
    * exceeded. Static step type is `One[Byte]`; the abort fires separately. */
  final case class BombGuard(maxBytes: Long) extends ScanPrim[Byte, One[Byte]]

  /** Content-defined chunking. Each emitted unit is a `Chunk[Byte]`.
    * Per-step cardinality:
    *   - `Null`                          -- accumulating, no boundary yet
    *   - `One[Chunk[Byte]]`              -- one boundary hit
    *   - `NonEmpty[Chunk[Byte]]`         -- buffer drained multiple chunks
    *
    * Trailing buffer flushed via `ScanDone.Success.leftover`. */
  final case class FastCDC(min: Int, avg: Int, max: Int)
      extends ScanPrim[Byte, Null | NonEmpty[kyo.Chunk[Byte]]]

  /** Fixed-size chunking. Per-step cardinality is `Null | One[Chunk[Byte]]`
    * since at most one chunk boundary is crossed per input byte. */
  final case class FixedChunk(n: Int)
      extends ScanPrim[Byte, Null | One[kyo.Chunk[Byte]]]

  /** Convenience: identity pass-through. Always `One`. */
  def identity[A]: ScanPrim[A, One[A]] = Map(a => a)
}
