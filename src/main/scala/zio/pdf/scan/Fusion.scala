/*
 * The "pure fast path".
 *
 * A `FreeScan` whose every node is `Arr` or `Prim(ScanPrim.Map)` carries
 * no state, no Emit, no Abort, no Var. It is just a function. The
 * interpreter recognises this case before lowering to a Kyo `ScanProg`
 * and collapses the entire chain to a single `I => O`. A pipeline of
 * N pure maps then runs in O(1) per input: one `Poll`, one function
 * call, one `Emit`.
 *
 * As soon as one stage is a `Filter`, `Fold`, `Hash`, `BombGuard`,
 * `FastCDC`, ... fusion fails and the general interpreter takes over.
 * Same for `Fanout`/`Choice` -- those are not unary -> unary.
 */

package zio.pdf.scan

object Fusion {

  /** Try to collapse a `FreeScan[I, O]` into a single `I => O` function.
    * Returns `Some` when every node along the spine is `Arr` or
    * `Prim(ScanPrim.Map)`; `None` otherwise. */
  def tryFuse[I, O](scan: FreeScan[I, O]): Option[I => O] = scan match {
    case FreeScan.Arr(f) =>
      Some(f.asInstanceOf[I => O])
    case FreeScan.Prim(ScanPrim.Map(f)) =>
      Some(f.asInstanceOf[I => O])
    case FreeScan.AndThen(left, right) =>
      // Both sides need to fuse; we don't know `M` here so we treat both
      // as `Any => Any` and rely on the surrounding type-correctness of
      // `AndThen` to keep the composed function honest.
      val leftFused  = tryFuse(left.asInstanceOf[FreeScan[I, Any]])
      val rightFused = tryFuse(right.asInstanceOf[FreeScan[Any, O]])
      for {
        fl <- leftFused
        fr <- rightFused
      } yield (fl andThen fr).asInstanceOf[I => O]
    case _ =>
      None
  }
}
