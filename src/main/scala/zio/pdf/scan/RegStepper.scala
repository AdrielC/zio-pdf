/*
 * Register-based stepper -- the zero-allocation alternative to `Stepper`.
 *
 * The legacy `Stepper` is *persistent*: every `step(i)` call returns a
 * fresh closure (e.g. `loop(n - 1)`) plus a fresh `StepEffect` case class
 * holding a fresh `Vector(o)` of outputs. That is one allocation per
 * stage per input byte, for state that is usually a single `Int` or
 * `Long`. The pattern is essentially the "Tuple2 + boxed Int" problem
 * John De Goes describes for record construction in ZIO Schema 2, but at
 * stream-step granularity.
 *
 * This file ports the technique into the Scan algebra. State lives in a
 * single mutable `RegState` whose layout is the concatenation of every
 * stage's slot. Each stage receives its own offset (`RegOff`) and
 * reads/writes its slot in place. Outputs go into a reusable `RegOutBuffer`
 * rather than a fresh `Vector` per step. The composition driver (`AndThen`)
 * threads outputs from one stage's buffer into the next.
 *
 * The signal channel is encoded as an `Int` (`RegSignal.Continue / Success /
 * Stop / Failure`) rather than an `Option[ScanDone[O, E]]`, so the hot
 * path also avoids the `Some(ScanDone.Success(Seq.empty))` allocation
 * that every step pays today.
 *
 * Scope of this v1: spine composition (`AndThen` over `Map`/`Filter`/
 * `Take`/`Drop`/`Fold`/`Hash`/`CountBytes`/`BombGuard`/`FixedChunk`/
 * `FastCDC`). Fanout/Choice still go through the legacy `Stepper` path
 * because their tuple-of-outputs / Either-routing protocol benefits less
 * from register state and would require its own output-buffer story.
 *
 * The layout description is internally a `RegLayout(longs, objects)` --
 * a tighter version of zio-blocks-schema's `RegisterOffset` for the
 * primitive types Scan actually uses (Int, Long, Boolean → 1 long slot
 * each; everything else → 1 object slot). When this design proves out
 * on the benches we can lift the layout onto
 * `zio.blocks.schema.binding.Registers` proper without changing the
 * shape of `RegStepper`.
 */

package zio.pdf.scan

/** Mutable state arena for a register-based stepper.
  *
  * Two parallel arrays:
  *
  *   - `longs`   -- primitive 8-byte slots. `Int`, `Long`, `Boolean`
  *                  (as 0/1), and any other fixed-width primitive all
  *                  consume one slot. We trade 4 bytes of padding per
  *                  `Int` for branch-free offset arithmetic at every
  *                  step.
  *   - `objects` -- reference slots. Anything that doesn't fit in a long
  *                  (a `MessageDigest`, a mutable byte buffer, a fold's
  *                  accumulator) lives here.
  *
  * Capacity grows on demand. Callers should pre-size via `ensureCapacity`
  * to avoid grows on the hot path.
  */
final class RegState(initialLongs: Int = 8, initialObjects: Int = 8) {

  private[scan] var longs:   Array[Long]   = new Array[Long](math.max(initialLongs, 1))
  private[scan] var objects: Array[AnyRef] = new Array[AnyRef](math.max(initialObjects, 1))

  inline def getLong(off: Int): Long = longs(off)
  inline def setLong(off: Int, v: Long): Unit = longs(off) = v

  inline def getInt(off: Int): Int = longs(off).toInt
  inline def setInt(off: Int, v: Int): Unit = longs(off) = v.toLong

  inline def getBoolean(off: Int): Boolean = longs(off) != 0L
  inline def setBoolean(off: Int, v: Boolean): Unit = longs(off) = if v then 1L else 0L

  inline def getObject(off: Int): AnyRef = objects(off)
  inline def setObject(off: Int, v: AnyRef): Unit = objects(off) = v

  /** Pre-size to fit a layout. Call once per pipeline before `init`. */
  def ensureCapacity(nLongs: Int, nObjects: Int): Unit = {
    if nLongs   > longs.length   then growLongs(nLongs)
    if nObjects > objects.length then growObjects(nObjects)
  }

  /** Zero every slot. Useful when reusing a `RegState` for back-to-back
    * pipeline runs. */
  def clear(): Unit = {
    java.util.Arrays.fill(longs, 0L)
    java.util.Arrays.fill(objects.asInstanceOf[Array[Object]], null)
  }

  private def growLongs(min: Int): Unit = {
    val n   = math.max(longs.length * 2, min)
    val nxt = new Array[Long](n)
    System.arraycopy(longs, 0, nxt, 0, longs.length)
    longs = nxt
  }

  private def growObjects(min: Int): Unit = {
    val n   = math.max(objects.length * 2, min)
    val nxt = new Array[AnyRef](n)
    System.arraycopy(objects, 0, nxt, 0, objects.length)
    objects = nxt
  }
}

/** How much state a stepper (or composite) needs.
  *
  * Concatenation is `+`: an `AndThen(l, r)` has `l.layout + r.layout`,
  * and the right's offset is `off + l.layout`.
  */
final case class RegLayout(longs: Int, objects: Int) {
  def +(that: RegLayout): RegLayout =
    RegLayout(longs + that.longs, objects + that.objects)
}

object RegLayout {
  val Zero: RegLayout = RegLayout(0, 0)
  def long: RegLayout = RegLayout(1, 0)
  def obj:  RegLayout = RegLayout(0, 1)
}

/** Offset into a `RegState`. Two cursors, one for primitives, one for
  * references. Adding a layout advances both cursors by the layout's
  * widths; this is how `AndThen` positions the right stage's slot. */
final case class RegOff(longs: Int, objects: Int) {
  def +(layout: RegLayout): RegOff =
    RegOff(longs + layout.longs, objects + layout.objects)
}

object RegOff {
  val Zero: RegOff = RegOff(0, 0)
}

/** Signal returned by `step` / `end`. An `Int` rather than an enum so the
  * JIT doesn't need to box. */
object RegSignal {
  inline val Continue = 0
  inline val Success  = 1
  inline val Stop     = 2
  inline val Failure  = 3
}

/** A reusable, growable output queue. Replaces the per-step
  * `Vector(emittedValue)` allocation in `StepEffect`. Boxes primitives
  * (because `Array[AnyRef]`), which is no worse than the current
  * `Vector[Int]` -- and on the hot path the elements never escape this
  * buffer, so the JIT can scalarise the values away in the lucky case.
  *
  * Element type is erased to `AnyRef` so a `RegStepper[-I, +O, +E]` can
  * accept a buffer in a contravariant-friendly position; the concrete
  * type is recovered at the use site via unchecked casts. Not thread-safe;
  * each compiled pipeline owns its own buffer(s). */
final class RegOutBuffer(initial: Int = 16) {

  private var arr: Array[AnyRef] = new Array[AnyRef](math.max(initial, 1))
  private var sz:  Int           = 0

  def size: Int = sz
  def isEmpty: Boolean = sz == 0
  def nonEmpty: Boolean = sz > 0

  inline def applyAny(i: Int): AnyRef = arr(i)

  def push(o: Any): Unit = {
    if sz >= arr.length then grow()
    arr(sz) = o.asInstanceOf[AnyRef]
    sz += 1
  }

  /** Reset the cursor and null out previously-used slots so the JVM can
    * collect the old references. */
  def clear(): Unit = {
    var i = 0
    while i < sz do { arr(i) = null; i += 1 }
    sz = 0
  }

  /** Drain into a builder in one pass without copying. */
  def drainInto[O](builder: scala.collection.mutable.Builder[O, ?]): Unit = {
    var i = 0
    while i < sz do {
      builder += arr(i).asInstanceOf[O]
      i += 1
    }
  }

  private def grow(): Unit = {
    val n   = arr.length * 2
    val nxt = new Array[AnyRef](n)
    System.arraycopy(arr, 0, nxt, 0, arr.length)
    arr = nxt
  }
}

/** The register-based stepper.
  *
  * Implementations declare a `layout`, and every method receives the
  * `RegOff` that anchors this stepper's slot inside the shared
  * `RegState`. Outputs go into the caller-provided `out` buffer (never
  * allocated by `step` itself). State updates are in-place via
  * `regs.setLong` / `regs.setObject`.
  */
trait RegStepper[-I, +O, +E] {

  /** State footprint. Composite steppers concatenate child layouts. */
  def layout: RegLayout

  /** Write the initial state into `regs` at `off`. Called once per
    * pipeline invocation, before any `step`. */
  def init(regs: RegState, off: RegOff): Unit

  /** Consume one input. Outputs are pushed onto `out`; the caller is
    * responsible for draining and clearing `out` between calls. */
  def step(regs: RegState, off: RegOff, i: I, out: RegOutBuffer): Int

  /** End-of-stream flush. Any final outputs go on `out`. */
  def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int

  /** Leftover carried by the terminal signal. Default empty. */
  def leftover(regs: RegState, off: RegOff): Seq[O] = Seq.empty

  /** Failure payload when `step`/`end` returns `RegSignal.Failure`. The
    * type parameter is the stepper's `E`; the runner widens to `E | Any`
    * because the legacy `Stepper` family historically used `Any` for the
    * error union. */
  def failure(regs: RegState, off: RegOff): Option[E] = None
}
