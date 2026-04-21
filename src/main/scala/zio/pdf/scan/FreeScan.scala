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

import StepOut.StepOut

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

  extension [I, O](self: FreeScan[I, O]) {

    // ===================================================================
    // Category: identity, sequential composition
    // ===================================================================

    /** Sequential composition. Read as "then". */
    def >>>[P](right: FreeScan[O, P]): FreeScan[I, P] = AndThen(self, right)

    /** Reverse sequential composition. `g <<< f === f >>> g`. */
    def <<<[H](pre: FreeScan[H, I]): FreeScan[H, O] = AndThen(pre, self)

    /** Alias for `>>>`, the "andThen" verb. */
    def andThen[P](right: FreeScan[O, P]): FreeScan[I, P] = AndThen(self, right)

    /** Alias for `<<<`, the "compose" verb. */
    def compose[H](pre: FreeScan[H, I]): FreeScan[H, O] = AndThen(pre, self)

    // ===================================================================
    // Profunctor: pure pre/post composition
    // ===================================================================

    /** Post-compose a pure function. */
    def map[P](f: O => P): FreeScan[I, P] = AndThen(self, Arr(f))

    /** Pre-compose a pure function. */
    def contramap[J](f: J => I): FreeScan[J, O] = AndThen(Arr(f), self)

    /** Bidirectional pure adaptation. */
    def dimap[J, P](pre: J => I)(post: O => P): FreeScan[J, P] =
      AndThen(Arr(pre), AndThen(self, Arr(post)))

    // ===================================================================
    // Arrow: tuple parallelism
    //
    // `first` / `second` thread one component through unchanged while
    // running `self` on the other. They derive from `&&&` plus pure
    // tuple shuffling so fusion still kicks in when the side-channel
    // arm is purely functional.
    // ===================================================================

    /** Run `self` on the first component, leave the second untouched. */
    def first[C]: FreeScan[(I, C), (O, C)] =
      Fanout(
        AndThen(Arr((p: (I, C)) => p._1), self),
        Arr((p: (I, C)) => p._2)
      )

    /** Run `self` on the second component, leave the first untouched. */
    def second[C]: FreeScan[(C, I), (C, O)] =
      Fanout(
        Arr((p: (C, I)) => p._1),
        AndThen(Arr((p: (C, I)) => p._2), self)
      )

    /** Parallel composition on a tuple input: `self` on `_1`, `right` on
      * `_2`. Equivalent to `self.first[I2] >>> right.second[O]`, but
      * built directly so the fanout sees both arms at once. */
    def ***[I2, O2](right: FreeScan[I2, O2]): FreeScan[(I, I2), (O, O2)] =
      Fanout(
        AndThen(Arr((p: (I, I2)) => p._1), self),
        AndThen(Arr((p: (I, I2)) => p._2), right)
      )

    /** Parallel composition into a tuple. */
    def &&&[P](right: FreeScan[I, P]): FreeScan[I, (O, P)] = Fanout(self, right)

    // ===================================================================
    // ArrowChoice: routing on Either
    // ===================================================================

    /** Run `self` on the `Left` branch, pass `Right` through unchanged.
      * The output type widens to `Either[O, C]` so the right-hand value
      * still flows downstream. */
    def left[C]: FreeScan[Either[I, C], Either[O, C]] =
      Choice(
        AndThen(self, Arr((o: O) => Left(o): Either[O, C])),
        Arr((c: C) => Right(c): Either[O, C])
      )

    /** Run `self` on the `Right` branch, pass `Left` through unchanged. */
    def right[C]: FreeScan[Either[C, I], Either[C, O]] =
      Choice(
        Arr((c: C) => Left(c): Either[C, O]),
        AndThen(self, Arr((o: O) => Right(o): Either[C, O]))
      )

    /** Routed composition: run `self` on `Left` and `right` on `Right`,
      * collapse to a single output type. The Haskell `|||`. */
    def |||[I2](right: FreeScan[I2, O]): FreeScan[Either[I, I2], O] = Choice(self, right)

    /** Disjoint parallel composition: run `self` on `Left`, `right` on
      * `Right`, return whichever branch fired (preserves the Either
      * shape -- the Haskell `+++`). */
    def +++[I2, O2](right: FreeScan[I2, O2]): FreeScan[Either[I, I2], Either[O, O2]] =
      Choice(
        AndThen(self, Arr((o: O) => Left(o): Either[O, O2])),
        AndThen(right, Arr((o2: O2) => Right(o2): Either[O, O2]))
      )

    // ===================================================================
    // Convenience / glue
    // ===================================================================

    /** Drop the right side of a parallel pair. `(self &&& right) >>> drainLeft
      * === self`. */
    def drainLeft[I2]: FreeScan[I, O] = self

    /** Like `drainLeft` but as an extension on a tuple-output scan: keep
      * the left, throw away the right. */
    def keepFirst[L, R](using ev: O <:< (L, R)): FreeScan[I, L] =
      AndThen(self, Arr((o: O) => ev(o)._1))

    /** Mirror of `keepFirst`. */
    def keepSecond[L, R](using ev: O <:< (L, R)): FreeScan[I, R] =
      AndThen(self, Arr((o: O) => ev(o)._2))
  }

  // -----------------------------------------------------------------
  // Standalone arrow helpers -- live on the companion so they read
  // naturally at call sites (e.g. `FreeScan.swap[Int, String]`).
  // -----------------------------------------------------------------

  /** The pure tuple swap. */
  def swap[A, B]: FreeScan[(A, B), (B, A)] =
    Arr((p: (A, B)) => (p._2, p._1))

  /** The pure Either swap. */
  def mirror[A, B]: FreeScan[Either[A, B], Either[B, A]] =
    Arr {
      case Left(a)  => Right(a)
      case Right(b) => Left(b)
    }

  /** Diagonal: `a => (a, a)`. */
  def diag[A]: FreeScan[A, (A, A)] =
    Arr((a: A) => (a, a))

  /** Untag an `Either[A, A]` by collapsing both branches. */
  def merge[A]: FreeScan[Either[A, A], A] =
    Arr {
      case Left(a)  => a
      case Right(a) => a
    }

  /** Inject into the left side of an Either. */
  def injectLeft[A, B]: FreeScan[A, Either[A, B]] =
    Arr((a: A) => Left(a): Either[A, B])

  /** Inject into the right side of an Either. */
  def injectRight[A, B]: FreeScan[B, Either[A, B]] =
    Arr((b: B) => Right(b): Either[A, B])

  /** Boolean test: route by predicate. `test(p)(yes)(no)`. */
  def test[A, B](p: A => Boolean)(yes: FreeScan[A, B])(no: FreeScan[A, B]): FreeScan[A, B] =
    AndThen(
      Arr((a: A) => if p(a) then Left(a) else Right(a): Either[A, A]),
      Choice(yes, no)
    )

  /** Run `self` and discard its output. */
  def void[A, B](self: FreeScan[A, B]): FreeScan[A, Unit] =
    AndThen(self, Arr((_: B) => ()))

  /** Constant scan: ignore the input, always emit `b`. */
  def const[A, B](b: B): FreeScan[A, B] =
    Arr((_: A) => b)

  /** Take the first projection of a tuple. */
  def fst[A, B]: FreeScan[(A, B), A] =
    Arr((p: (A, B)) => p._1)

  /** Take the second projection of a tuple. */
  def snd[A, B]: FreeScan[(A, B), B] =
    Arr((p: (A, B)) => p._2)
}

