package zio.graviton.scan

/**
 * Right-associate an [[FreeScan.AndThen]] spine: `(a >>> b) >>> c` becomes
 * `(a, List(b, c))` so downstream interpretation is O(1) stack per structural step.
 */
object Spine {

  def flattenSpine[I, O](scan: FreeScan[I, O]): (FreeScan[I, ?], List[FreeScan[Any, O]]) =
    scan match {
      case FreeScan.AndThen(left, right) =>
        val (prim, spine) = flattenSpine(left.asInstanceOf[FreeScan[I, Any]])
        (prim, (spine :+ right).asInstanceOf[List[FreeScan[Any, O]]])
      case other =>
        (other, Nil)
    }
}
