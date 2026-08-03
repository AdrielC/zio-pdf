/*
 * Compile-time fusion entry point.
 *
 * The compile-time fusion is *built into* `FreeScan.>>>` now (via a
 * `transparent inline def` with an `inline match` over the operand
 * shapes). Default callers never reach into this object: they just
 * write `Scan.map(f) >>> Scan.map(g)` and the `>>>` itself collapses
 * the chain to a single `Arr` at the call site.
 *
 * The standalone `fuse` here is the same machinery surfaced as a
 * function, useful when you already hold two `FreeScan` values and
 * want to combine them without going through the extension method.
 */

package zio.pdf.scan

object InlineFusion {

  /** Compile-time fused composition. Equivalent to `lhs >>> rhs` but
    * usable in a function-position context. The `inline match`
    * recognises:
    *
    *   - `Arr(f) >>> Arr(g)`             ==>  `Arr(g compose f)`
    *   - `Arr(f) >>> rhs`                ==>  `AndThen(Arr(f), rhs)`
    *   - `lhs >>> Arr(g)`                ==>  `AndThen(lhs, Arr(g))`
    *   - default                          ==>  `AndThen(lhs, rhs)`
    *
    * Type-unsafe casts are absent here: composition obeys
    * `Arr(g) compose Arr(f) == Arr(g compose f)` so the rewrite is
    * sound. */
  transparent inline def fuse[I, O, P](
      inline lhs: FreeScan[I, O],
      inline rhs: FreeScan[O, P]
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
}
