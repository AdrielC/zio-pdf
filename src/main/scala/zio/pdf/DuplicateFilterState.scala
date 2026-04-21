/*
 * Approximate duplicate detection for PDF object numbers before the
 * first xref. Uses a fixed-size bit table (Bloom-style single hash);
 * no unbounded [[Set]] growth.
 *
 * False positives: we may suppress an object whose number was not
 * actually seen (rare for typical PDF sizes vs 1M slots). False
 * negatives are not possible for repeats that collide on the reduced
 * hash — same slot means we still treat as duplicate.
 */

package zio.pdf

private[pdf] object DuplicateFilterState {

  /** Power-of-two slot count; mask = table.length - 1. */
  private val TableBits = 20
  private val TableLen  = 1 << TableBits

  final class Mutable(
    var table: Array[Long],
    var updateMode: Boolean,
    var duplicateCount: Int
  )

  def initial: Mutable = new Mutable(new Array[Long](TableLen), updateMode = false, duplicateCount = 0)

  @inline private def slot(num: Long): Int =
    (java.lang.Long.rotateLeft(num * 0x9e3779b97f4a7c15L, 15).toInt & (TableLen - 1))

  /** @return true if this indirect object should be suppressed */
  def shouldSuppress(m: Mutable, objNum: Long): Boolean =
    if (m.updateMode) false
    else {
      val s    = slot(objNum)
      val word = m.table(s)
      val bit  = 1L << (objNum & 63L)
      if ((word & bit) != 0L) {
        m.duplicateCount += 1
        true
      } else {
        m.table(s) = word | bit
        false
      }
    }

  def enterUpdateMode(m: Mutable): Unit =
    m.updateMode = true
}
