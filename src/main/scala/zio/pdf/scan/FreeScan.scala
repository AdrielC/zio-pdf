/*
 * The free symmetric monoidal category over `ScanPrim`. Standalone ADT
 * shown here for clarity; in production this is `FreeArrow[Arrow,
 * ScanPrim, I, O]` from AdrielC/free-arrow, with the same shape:
 *
 *   - `Prim`     -- lift a primitive (the only stateful node)
 *   - `Arr`      -- lift a pure function (zero-state, fuses freely)
 *   - `AndThen`  -- sequential composition (`>>>`)
 *   - `Fanout`   -- parallel composition into a tuple (`&&&`)
 *   - `Choice`   -- routed composition over `Either` (`|||`)
 *
 * The composition interpreter (`SinglePassInterp`) flattens left-nested
 * `AndThen` spines once before execution -- this is the structural
 * analogue of Volga's `StateCont.compose` reassociation, and it gives
 * O(1) stack depth per step.
 */

package zio.pdf.scan

import StepOut.*

sealed trait FreeScan[-I, +O] extends Product with Serializable

object FreeScan {

  /** A leaf carrying a single primitive. The primitive's `Out` type may be
    * any subtype of `StepOut[O]`. */
  final case class Prim[I, O](op: ScanPrim[I, StepOut[O]]) extends FreeScan[I, O]

  /** A pure function lift -- never reaches `Emit`/`Var`/`Abort`. The fusion
    * pass collapses chains of `Arr`/`Prim(Map)` into a single `I => O`. */
  final case class Arr[I, O](f: I => O) extends FreeScan[I, O]

  /** Sequential composition. The interpreter rewrites left-nested chains
    * into right-nested form before execution. */
  final case class AndThen[I, M, O](left: FreeScan[I, M], right: FreeScan[M, O])
      extends FreeScan[I, O]

  /** Parallel composition into a tuple. */
  final case class Fanout[I, OL, OR](left: FreeScan[I, OL], right: FreeScan[I, OR])
      extends FreeScan[I, (OL, OR)]

  /** Routed composition. */
  final case class Choice[IL, IR, O](left: FreeScan[IL, O], right: FreeScan[IR, O])
      extends FreeScan[Either[IL, IR], O]

  /** Smart constructor for primitive lifts. */
  def lift[I, O](p: ScanPrim[I, StepOut[O]]): FreeScan[I, O] = Prim(p)

  /** Smart constructor for pure function lifts. */
  def arr[I, O](f: I => O): FreeScan[I, O] = Arr(f)

  /** The identity scan. */
  def id[A]: FreeScan[A, A] = Arr(a => a)

  extension [I, O](left: FreeScan[I, O]) {

    /** Sequential composition. Read as "then". */
    def >>>[P](right: FreeScan[O, P]): FreeScan[I, P] = AndThen(left, right)

    /** Parallel composition into a tuple. */
    def &&&[P](right: FreeScan[I, P]): FreeScan[I, (O, P)] = Fanout(left, right)

    /** Routed composition. */
    def |||[I2](right: FreeScan[I2, O]): FreeScan[Either[I, I2], O] = Choice(left, right)

    /** Post-compose a pure function. */
    def map[P](f: O => P): FreeScan[I, P] = AndThen(left, Arr(f))

    /** Pre-compose a pure function. */
    def contramap[J](f: J => I): FreeScan[J, O] = AndThen(Arr(f), left)

    /** Bidirectional pure adaptation. */
    def dimap[J, P](pre: J => I)(post: O => P): FreeScan[J, P] =
      AndThen(Arr(pre), AndThen(left, Arr(post)))
  }
}
