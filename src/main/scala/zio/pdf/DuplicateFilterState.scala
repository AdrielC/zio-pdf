/*
 * Duplicate indirect-object detection before the first xref: exact
 * membership for object numbers in `[0, Int.MaxValue]` using a
 * [[java.util.BitSet]] (grows with the highest number seen, not a
 * [[Set]] of all ids). Numbers outside that range are never suppressed.
 *
 * Same semantics as the original fs2-pdf filter: no false positives
 * for typical PDFs; memory is bounded by the largest object number
 * observed in the pre-xref prologue (often tiny).
 */

package zio.pdf

import java.util as ju

private[pdf] object DuplicateFilterState {

  final class Mutable(
    var seen: ju.BitSet,
    var updateMode: Boolean,
    var duplicateCount: Int
  )

  def initial: Mutable = new Mutable(new ju.BitSet, updateMode = false, duplicateCount = 0)

  /** @return true if this indirect object should be suppressed */
  def shouldSuppress(m: Mutable, objNum: Long): Boolean =
    if (m.updateMode) false
    else if (objNum < 0L || objNum > Int.MaxValue) false
    else {
      val i = objNum.toInt
      if (m.seen.get(i)) {
        m.duplicateCount += 1
        true
      } else {
        m.seen.set(i)
        false
      }
    }

  def enterUpdateMode(m: Mutable): Unit =
    m.updateMode = true
}
