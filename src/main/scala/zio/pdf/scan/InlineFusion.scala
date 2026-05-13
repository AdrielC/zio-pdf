/*
 * Compile-time fusion entry points.
 *
 * The runtime `Fusion.tryFuse` walks a `FreeScan` value and collapses
 * any spine of `Arr` / `Prim(Map)` nodes into a single `I => O`. That
 * walk happens once per pipeline compile, which is cheap -- but the
 * spine itself is *already* known statically in essentially every
 * real call site:
 *
 *   val pipe: FreeScan[Byte, Int] =
 *     Scan.map[Byte, Int](_ & 0xff) >>>
 *       Scan.map[Int, Int](_ + 1)
 *
 * The two `Scan.map` calls are visible at compile time. There is no
 * reason for the resulting `FreeScan` to carry two `Arr` nodes that
 * the runtime then re-fuses; the compiler can hand back a single
 * `Arr` directly. This file is the seed of that machinery: an
 * `inline` `>>>` that, when both sides are statically `Arr`-shaped,
 * collapses to a single `Arr` *at the call site*. Pipelines built
 * through this entry point cost zero allocations and zero `tryFuse`
 * walks at runtime; their structural form is `Arr` from the start.
 *
 * For nodes that need runtime visibility (Fanout/Choice/stateful
 * primitives), the inline `>>>` just falls back to the existing
 * `AndThen` constructor -- identical behaviour to the non-inline
 * surface, plus zero cost when the pattern doesn't match.
 *
 * This is the first piece of the compile-time pipeline-shape machinery
 * the larger Flow-style redesign will lean on. The next layer
 * (match-type-driven `RegLayout` summation, named-leftover record
 * accumulation) builds on the same `inline` foundation.
 */

package zio.pdf.scan

object InlineFusion {

  /** Compile-time fused composition. Recognises four cases:
    *
    *   - `Arr(f) >>> Arr(g)`             ==>  `Arr(g compose f)`
    *   - `Arr(f) >>> rhs`                ==>  `AndThen(Arr(f), rhs)`
    *     (left was already a single function, no node to fuse)
    *   - `lhs >>> Arr(g)`                ==>  `AndThen(lhs, Arr(g))`
    *   - default                          ==>  `AndThen(lhs, rhs)`
    *
    * `inline` so the match happens at the call site; the runtime
    * never sees the analysis. Type-unsafe casts are confined to
    * lifting the inline-known shapes, and the resulting
    * `FreeScan[I, P]` is well-typed because composition obeys the
    * usual category law `Arr(g) compose Arr(f) == Arr(g compose f)`. */
  inline def fuse[I, O, P](
      lhs: FreeScan[I, O],
      rhs: FreeScan[O, P]
  ): FreeScan[I, P] =
    inline lhs match {
      case lArr: FreeScan.Arr[I, O] =>
        inline rhs match {
          case rArr: FreeScan.Arr[O, P] =>
            FreeScan.Arr[I, P]((i: I) => rArr.f(lArr.f(i)))
          case _ =>
            FreeScan.AndThen(lhs, rhs)
        }
      case _ =>
        FreeScan.AndThen(lhs, rhs)
    }

  /** Sugar so `Scan.map(f) >>>! Scan.map(g)` reads naturally at call
    * sites. The `!` suffix marks the inline lane explicitly so the
    * normal `>>>` (which keeps `AndThen` nodes for the FreeScan
    * runtime to interpret/inspect) remains the default. */
  extension [I, O](self: FreeScan[I, O])
    inline def >>>![P](rhs: FreeScan[O, P]): FreeScan[I, P] = fuse(self, rhs)

  // -------------------------------------------------------------------
  // Transparent smart constructors.
  //
  // `transparent inline def` returns a *singleton-narrowed* result type
  // so the call site sees `FreeScan.Arr[I, O]` instead of the widened
  // `FreeScan[I, O]`. That narrowness is what lets the `inline match`
  // in `fuse` discriminate at compile time and emit a single fused
  // `Arr` for chains of pure stages -- without it, the parent-trait
  // type erases the case-class identity at the static use site.
  // -------------------------------------------------------------------

  /** Compile-time-friendly `arr`: returns the narrow `Arr` type so the
    * inline fusion match can discriminate it. */
  transparent inline def arrT[I, O](inline f: I => O): FreeScan.Arr[I, O] =
    FreeScan.Arr[I, O](f)

  /** Compile-time-friendly `map`: same as `arrT` but reads natural at
    * call sites where you would have written `Scan.map`. */
  transparent inline def mapT[I, O](inline f: I => O): FreeScan.Arr[I, O] =
    FreeScan.Arr[I, O](f)
}
