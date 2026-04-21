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
    * `Prim(ScanPrim.Map)`; `None` otherwise.
    *
    * Stack-safe: iteratively walks the spine left-to-right using an
    * explicit work-stack rather than recursing through `AndThen`. A
    * spine of N pure nodes reduces in N composition steps with O(1)
    * stack depth. */
  def tryFuse[I, O](scan: FreeScan[I, O]): Option[I => O] = {
    val stages = scala.collection.mutable.ArrayBuffer.empty[Any => Any]
    val work   = new scala.collection.mutable.Stack[FreeScan[Any, Any]]
    work.push(scan.asInstanceOf[FreeScan[Any, Any]])
    while work.nonEmpty do {
      val node = work.pop()
      node match {
        case FreeScan.AndThen(l, r) =>
          // Right-most stage runs last -- push right first so the left
          // pops first. (Stack is LIFO.)
          work.push(r.asInstanceOf[FreeScan[Any, Any]])
          work.push(l.asInstanceOf[FreeScan[Any, Any]])
        case FreeScan.Arr(f) =>
          stages += f.asInstanceOf[Any => Any]
        case FreeScan.Prim(ScanPrim.Map(f)) =>
          stages += f.asInstanceOf[Any => Any]
        case _ =>
          // Non-pure node -- whole spine is unfuseable. Bail out.
          return None
      }
    }
    if stages.isEmpty then Some(((a: I) => a.asInstanceOf[O]))
    else {
      val arr      = stages.toArray
      val composed = (a: Any) => {
        var v = a
        var i = 0
        while i < arr.length do {
          v = arr(i)(v)
          i += 1
        }
        v
      }
      Some(composed.asInstanceOf[I => O])
    }
  }
}
