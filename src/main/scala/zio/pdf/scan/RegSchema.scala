/*
 * `RegSchema[S]` -- the layout-and-accessors typeclass that lets a
 * stepper read/write its state in a `RegState` *without boxing
 * primitives*.
 *
 * Direct copy of the registry pattern ZIO Blocks uses for record
 * construction in `zio-blocks-schema`:
 *
 *   - `Reflect.Record[F, A]` carries a precomputed `usedRegisters:
 *     RegisterOffset` (total layout) and `registers:
 *     IndexedSeq[RegisterPos[A]]` (per-field accessors with `.get` and
 *     `.set`).
 *   - `Binding.Record[A]`'s `constructor.construct(regs, off)` and
 *     `deconstructor.deconstruct(regs, off, value)` use those
 *     accessors to round-trip `A` through the register arena with
 *     zero allocations and zero boxing.
 *
 * The `Fold[I, S]` stepper in this codebase needs *exactly* the same
 * pattern at the type-class level: given a `RegSchema[S]`, the fold's
 * accumulator can live in a long slot for primitive `S` (no box) or
 * an object slot for reference `S`. The previous implementation always
 * boxed via `setObject(s.asInstanceOf[AnyRef])`, which is the
 * "Tuple2 + boxed Int" picture John De Goes describes for record
 * construction -- but at fold-step granularity.
 *
 * The instances here cover the primitive types the Scan algebra
 * actually folds with. Reference instances fall back to one object
 * slot. A future PR can lift `RegSchema` onto the full
 * `zio.blocks.schema.Schema[A]` derivation so case-class accumulators
 * decompose to one long/object slot per field automatically -- which
 * is the natural extension of this pattern to nested state. The
 * scaffolding here (typeclass + layout + read/write) is shaped so
 * that lift is mechanical when we want it.
 */

package zio.pdf.scan

/** A typeclass that describes how to read and write a value of type
  * `S` into a `RegState`. Mirrors `zio.blocks.schema.Reflect[S]`'s
  * `RegisterPos`-based pattern.
  *
  * Implementations come in two flavours:
  *
  *   - **Primitive** -- `layout == RegLayout.long`. Read and write hit
  *     the long-slot array directly; the value is reinterpreted in
  *     place (`getLong` / `setLong`, `getInt` / `setInt`, etc.). Zero
  *     boxing.
  *   - **Reference** -- `layout == RegLayout.obj`. Read and write hit
  *     the object array directly. No boxing of references (they were
  *     already pointers), but primitives that go through this
  *     instance *would* box -- so primitive instances are preferred
  *     via implicit priority.
  */
trait RegSchema[S] {

  /** Total state footprint of `S` in registers. */
  def layout: RegLayout

  /** Read `S` from the registers at the given offset. */
  def read(regs: RegState, off: RegOff): S

  /** Write `S` into the registers at the given offset. */
  def write(regs: RegState, off: RegOff, value: S): Unit
}

object RegSchema {

  /** Summon a `RegSchema[S]`. */
  inline def apply[S](using rs: RegSchema[S]): RegSchema[S] = rs

  // -------------------------------------------------------------------
  // Primitive instances -- zero boxing.
  //
  // Each consumes one long slot (8 bytes of "primitive arena" in the
  // arena view ZIO Blocks uses; here a single `Array[Long]` entry).
  // We pay 4 bytes of padding for `Int`/`Float`/`Boolean` etc. in
  // exchange for branch-free offset arithmetic and no `VarHandle`
  // gymnastics -- the slots are small enough that the trade is in
  // the noise.
  // -------------------------------------------------------------------

  given longSchema: RegSchema[Long] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Long = regs.getLong(off.longs)
    inline def write(regs: RegState, off: RegOff, value: Long): Unit =
      regs.setLong(off.longs, value)
  }

  given intSchema: RegSchema[Int] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Int = regs.getInt(off.longs)
    inline def write(regs: RegState, off: RegOff, value: Int): Unit =
      regs.setInt(off.longs, value)
  }

  given byteSchema: RegSchema[Byte] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Byte =
      regs.getLong(off.longs).toByte
    inline def write(regs: RegState, off: RegOff, value: Byte): Unit =
      regs.setLong(off.longs, value.toLong)
  }

  given booleanSchema: RegSchema[Boolean] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Boolean = regs.getBoolean(off.longs)
    inline def write(regs: RegState, off: RegOff, value: Boolean): Unit =
      regs.setBoolean(off.longs, value)
  }

  given doubleSchema: RegSchema[Double] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Double =
      java.lang.Double.longBitsToDouble(regs.getLong(off.longs))
    inline def write(regs: RegState, off: RegOff, value: Double): Unit =
      regs.setLong(off.longs, java.lang.Double.doubleToRawLongBits(value))
  }

  given floatSchema: RegSchema[Float] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Float =
      java.lang.Float.intBitsToFloat(regs.getInt(off.longs))
    inline def write(regs: RegState, off: RegOff, value: Float): Unit =
      regs.setInt(off.longs, java.lang.Float.floatToRawIntBits(value))
  }

  given shortSchema: RegSchema[Short] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Short =
      regs.getLong(off.longs).toShort
    inline def write(regs: RegState, off: RegOff, value: Short): Unit =
      regs.setLong(off.longs, value.toLong)
  }

  given charSchema: RegSchema[Char] with {
    val layout: RegLayout = RegLayout.long
    inline def read(regs: RegState, off: RegOff): Char =
      regs.getInt(off.longs).toChar
    inline def write(regs: RegState, off: RegOff, value: Char): Unit =
      regs.setInt(off.longs, value.toInt)
  }

  // -------------------------------------------------------------------
  // Reference fallback.
  //
  // Anything that does not match a primitive instance above goes here:
  // one object slot, written and read as-is. This mirrors how ZIO
  // Blocks treats `String`, custom classes, `Array`, etc. -- pointers
  // are stored as pointers, no further encoding.
  // -------------------------------------------------------------------

  /** Default reference instance. Picked up when no primitive instance
    * is more specific. */
  given anyRefSchema[A <: AnyRef]: RegSchema[A] with {
    val layout: RegLayout = RegLayout.obj
    inline def read(regs: RegState, off: RegOff): A =
      regs.getObject(off.objects).asInstanceOf[A]
    inline def write(regs: RegState, off: RegOff, value: A): Unit =
      regs.setObject(off.objects, value)
  }

  /** A coarse fallback for `Any` -- only used when the type is fully
    * erased at the call site. Boxes primitives the same way the
    * legacy stepper did, so we lose nothing relative to the old
    * behaviour; with a concrete `S` the specific instances above are
    * picked instead. */
  def erasedAnyInstance: RegSchema[Any] = new RegSchema[Any] {
    val layout: RegLayout = RegLayout.obj
    def read(regs: RegState, off: RegOff): Any =
      regs.getObject(off.objects)
    def write(regs: RegState, off: RegOff, value: Any): Unit =
      regs.setObject(off.objects, value.asInstanceOf[AnyRef])
  }
}
