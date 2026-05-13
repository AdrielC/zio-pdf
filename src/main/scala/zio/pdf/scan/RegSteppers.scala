/*
 * RegStepper implementations for the `ScanPrim` algebra and the
 * sequential composition primitive `AndThen`.
 *
 * Notes on per-primitive layouts:
 *
 *   - `Map` / `Filter`        -- stateless, RegLayout.Zero
 *   - `Take` / `Drop`         -- 1 long slot (remaining counter)
 *   - `CountBytes`            -- 1 long slot (count)
 *   - `BombGuard`             -- 1 long slot (count) + 1 object slot
 *                                (latched `BombError` when failure fires)
 *   - `Fold[I, S]`            -- 1 object slot (accumulator). Boxes
 *                                primitive `S` types -- acceptable since
 *                                the fold body itself would have boxed
 *                                in the closure-based version too.
 *   - `Hash`                  -- 1 object slot (the live `MessageDigest`)
 *   - `FixedChunk` / `FastCDC`-- 1 object slot (mutable buffer)
 *
 * `AndThen` lays its right child's offset at `off + left.layout`. The
 * scratch output buffer between stages lives on the `AndThen` instance
 * (not in the `RegState`) so different stage outputs don't tread on
 * each other's reference slots.
 */

package zio.pdf.scan

import scala.collection.immutable.ArraySeq

private[scan] object RegSteppers {

  // ----- stateless ----------------------------------------------------

  /** Pure `I => O`. Zero state. */
  final class Pure[I, O](f: I => O) extends RegStepper[I, O, Nothing] {
    val layout: RegLayout = RegLayout.Zero
    def init(regs: RegState, off: RegOff): Unit = ()
    def step(regs: RegState, off: RegOff, i: I, out: RegOutBuffer): Int = {
      out.push(f(i))
      RegSignal.Continue
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
  }

  /** Predicate filter. Zero state. */
  final class Filter[A](p: A => Boolean) extends RegStepper[A, A, Nothing] {
    val layout: RegLayout = RegLayout.Zero
    def init(regs: RegState, off: RegOff): Unit = ()
    def step(regs: RegState, off: RegOff, i: A, out: RegOutBuffer): Int = {
      if p(i) then out.push(i)
      RegSignal.Continue
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
  }

  // ----- counted ------------------------------------------------------

  /** Take the first `n` inputs, then stop. */
  final class Take[A](n: Int) extends RegStepper[A, A, Nothing] {
    val layout: RegLayout = RegLayout.long
    def init(regs: RegState, off: RegOff): Unit =
      regs.setLong(off.longs, n.toLong)
    def step(regs: RegState, off: RegOff, i: A, out: RegOutBuffer): Int = {
      val remaining = regs.getLong(off.longs)
      if remaining <= 0L then RegSignal.Stop
      else {
        out.push(i)
        val nx = remaining - 1L
        regs.setLong(off.longs, nx)
        if nx == 0L then RegSignal.Stop else RegSignal.Continue
      }
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
  }

  /** Drop the first `n` inputs; then identity. */
  final class Drop[A](n: Int) extends RegStepper[A, A, Nothing] {
    val layout: RegLayout = RegLayout.long
    def init(regs: RegState, off: RegOff): Unit =
      regs.setLong(off.longs, n.toLong)
    def step(regs: RegState, off: RegOff, i: A, out: RegOutBuffer): Int = {
      val remaining = regs.getLong(off.longs)
      if remaining <= 0L then { out.push(i); RegSignal.Continue }
      else { regs.setLong(off.longs, remaining - 1L); RegSignal.Continue }
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
  }

  // ----- byte stateful ------------------------------------------------

  /** Byte count; emits nothing. The count travels in `leftover` as 8
    * big-endian bytes (same wire shape as the legacy Stepper). */
  final class CountBytes extends RegStepper[Byte, Byte, Nothing] {
    val layout: RegLayout = RegLayout.long
    def init(regs: RegState, off: RegOff): Unit =
      regs.setLong(off.longs, 0L)
    def step(regs: RegState, off: RegOff, i: Byte, out: RegOutBuffer): Int = {
      regs.setLong(off.longs, regs.getLong(off.longs) + 1L)
      RegSignal.Continue
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
    override def leftover(regs: RegState, off: RegOff): Seq[Byte] = {
      val n   = regs.getLong(off.longs)
      val arr = java.nio.ByteBuffer.allocate(8).putLong(n).array()
      ArraySeq.unsafeWrapArray(arr)
    }
  }

  /** Pass-through with a byte budget. Long slot = bytes seen so far;
    * object slot = the latched `BombError` on failure. */
  final class BombGuard(maxBytes: Long) extends RegStepper[Byte, Byte, BombError] {
    val layout: RegLayout = RegLayout(longs = 1, objects = 1)
    def init(regs: RegState, off: RegOff): Unit = {
      regs.setLong(off.longs, 0L)
      regs.setObject(off.objects, null)
    }
    def step(regs: RegState, off: RegOff, i: Byte, out: RegOutBuffer): Int = {
      val n1 = regs.getLong(off.longs) + 1L
      if n1 > maxBytes then {
        regs.setObject(off.objects, BombError(n1, maxBytes))
        RegSignal.Failure
      } else {
        out.push(i)
        regs.setLong(off.longs, n1)
        RegSignal.Continue
      }
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
    override def failure(regs: RegState, off: RegOff): Option[BombError] = {
      val e = regs.getObject(off.objects)
      if e == null then None else Some(e.asInstanceOf[BombError])
    }
  }

  // ----- fold / hash --------------------------------------------------

  /** Stateful fold. Emits nothing per input; the final accumulator is
    * delivered as `leftover` of `Success`. The accumulator type `S`
    * lives in the object slot (boxes primitive `S` but that's
    * unavoidable for a generic fold; the closure-based version boxed
    * the same way). */
  final class Fold[I, S](seed: S, f: (S, I) => S) extends RegStepper[I, S, Nothing] {
    val layout: RegLayout = RegLayout.obj
    def init(regs: RegState, off: RegOff): Unit =
      regs.setObject(off.objects, seed.asInstanceOf[AnyRef])
    def step(regs: RegState, off: RegOff, i: I, out: RegOutBuffer): Int = {
      val s = regs.getObject(off.objects).asInstanceOf[S]
      regs.setObject(off.objects, f(s, i).asInstanceOf[AnyRef])
      RegSignal.Continue
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
    override def leftover(regs: RegState, off: RegOff): Seq[S] =
      Seq(regs.getObject(off.objects).asInstanceOf[S])
  }

  /** Bytes go into a `MessageDigest`; digest bytes are the leftover. */
  final class Hash(algo: HashAlgo) extends RegStepper[Byte, Byte, Nothing] {
    val layout: RegLayout = RegLayout.obj
    def init(regs: RegState, off: RegOff): Unit =
      regs.setObject(off.objects, algo.newDigest())
    def step(regs: RegState, off: RegOff, i: Byte, out: RegOutBuffer): Int = {
      regs.getObject(off.objects)
        .asInstanceOf[java.security.MessageDigest]
        .update(i)
      RegSignal.Continue
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
    override def leftover(regs: RegState, off: RegOff): Seq[Byte] = {
      val md = regs.getObject(off.objects).asInstanceOf[java.security.MessageDigest]
      ArraySeq.unsafeWrapArray(md.digest())
    }
  }

  // ----- chunkers -----------------------------------------------------

  /** Fixed-size chunker. The mutable `ByteArrayOutputStream` lives in
    * the object slot and is reused across emits. */
  final class FixedChunk(n: Int) extends RegStepper[Byte, kyo.Chunk[Byte], Nothing] {
    require(n > 0, "fixedChunk size must be positive")
    val layout: RegLayout = RegLayout.obj
    def init(regs: RegState, off: RegOff): Unit =
      regs.setObject(off.objects, new java.io.ByteArrayOutputStream(n))
    def step(regs: RegState, off: RegOff, i: Byte, out: RegOutBuffer): Int = {
      val buf = regs.getObject(off.objects).asInstanceOf[java.io.ByteArrayOutputStream]
      buf.write(i.toInt)
      if buf.size() >= n then {
        val arr = buf.toByteArray()
        buf.reset()
        out.push(kyo.Chunk.from(arr))
      }
      RegSignal.Continue
    }
    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int =
      RegSignal.Success
    override def leftover(regs: RegState, off: RegOff): Seq[kyo.Chunk[Byte]] = {
      val buf = regs.getObject(off.objects).asInstanceOf[java.io.ByteArrayOutputStream]
      if buf.size() > 0 then Seq(kyo.Chunk.from(buf.toByteArray())) else Seq.empty
    }
  }

  /** Content-defined chunker. Same drain logic as the legacy stepper but
    * the running buffer is a mutable `ByteArrayOutputStream` instead of
    * a persistent `Vector[Byte]`. */
  final class FastCDC(min: Int, avg: Int, max: Int) extends RegStepper[Byte, kyo.Chunk[Byte], Nothing] {
    private val cfg = zio.pdf.cdc.FastCdc.Config(min, avg, max)
    val layout: RegLayout = RegLayout.obj

    def init(regs: RegState, off: RegOff): Unit =
      regs.setObject(off.objects, new java.io.ByteArrayOutputStream(max))

    def step(regs: RegState, off: RegOff, i: Byte, out: RegOutBuffer): Int = {
      val buf = regs.getObject(off.objects).asInstanceOf[java.io.ByteArrayOutputStream]
      buf.write(i.toInt)
      while buf.size() >= cfg.maxSize do drainOne(buf, out)
      RegSignal.Continue
    }

    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int = {
      val buf = regs.getObject(off.objects).asInstanceOf[java.io.ByteArrayOutputStream]
      while buf.size() > 0 do drainOne(buf, out)
      RegSignal.Success
    }

    private def drainOne(
        buf: java.io.ByteArrayOutputStream,
        out: RegOutBuffer
    ): Unit = {
      val raw    = buf.toByteArray()
      val window = if raw.length >= cfg.maxSize then raw.take(cfg.maxSize) else raw
      val cut    = zio.pdf.cdc.FastCdc.cutOffsetForScan(window, cfg)
      val chunk  = java.util.Arrays.copyOfRange(raw, 0, cut)
      val rest   = java.util.Arrays.copyOfRange(raw, cut, raw.length)
      buf.reset()
      buf.write(rest, 0, rest.length)
      out.push(kyo.Chunk.from(chunk))
      ()
    }
  }

  // ----- composition: spine `AndThen` --------------------------------

  /** Sequential composition `left >>> right`. The right's slot starts at
    * `off + left.layout`. The intermediate output buffer is allocated
    * once per pipeline instance and reused across every input. */
  final class AndThen[I, M, O, EL, ER](
      left:  RegStepper[I, M, EL],
      right: RegStepper[M, O, ER]
  ) extends RegStepper[I, O, EL | ER] {

    private val mid: RegOutBuffer = new RegOutBuffer(16)

    // The intermediate buffer holds elements typed `M`, but the buffer
    // itself is erased to `RegOutBuffer` (storing `AnyRef`). We recover
    // the type with an unchecked cast when handing each value to `right`.
    private inline def midAt(i: Int): M = mid.applyAny(i).asInstanceOf[M]

    val layout: RegLayout = left.layout + right.layout

    def init(regs: RegState, off: RegOff): Unit = {
      left.init(regs, off)
      right.init(regs, off + left.layout)
    }

    def step(regs: RegState, off: RegOff, i: I, out: RegOutBuffer): Int = {
      mid.clear()
      val lSig = left.step(regs, off, i, mid)

      val rOff = off + left.layout
      var idx  = 0
      var rSig = RegSignal.Continue
      val n    = mid.size
      while idx < n && rSig == RegSignal.Continue do {
        rSig = right.step(regs, rOff, midAt(idx), out)
        idx += 1
      }
      compose(lSig, rSig)
    }

    def end(regs: RegState, off: RegOff, out: RegOutBuffer): Int = {
      mid.clear()
      val lSig = left.end(regs, off, mid)
      val rOff = off + left.layout

      var idx  = 0
      var rSig = RegSignal.Continue
      val n    = mid.size
      while idx < n && rSig == RegSignal.Continue do {
        rSig = right.step(regs, rOff, midAt(idx), out)
        idx += 1
      }

      // Feed left's leftover through the right.
      val leftLeft = left.leftover(regs, off)
      val li       = leftLeft.iterator
      while li.hasNext && rSig == RegSignal.Continue do {
        rSig = right.step(regs, rOff, li.next(), out)
      }

      val rEnd = if rSig == RegSignal.Continue then right.end(regs, rOff, out) else rSig
      // Precedence mirrors the legacy `composeAndThen`:
      //   * Right's failure wins immediately.
      //   * Otherwise Stop > Failure (left) > Success.
      if rEnd == RegSignal.Failure then RegSignal.Failure
      else if rSig == RegSignal.Failure then RegSignal.Failure
      else if lSig == RegSignal.Failure then RegSignal.Failure
      else if rSig == RegSignal.Stop || lSig == RegSignal.Stop then RegSignal.Stop
      else RegSignal.Success
    }

    override def leftover(regs: RegState, off: RegOff): Seq[O] =
      // Right is the "outer" stage: its leftover is the visible one.
      right.leftover(regs, off + left.layout)

    override def failure(regs: RegState, off: RegOff): Option[EL | ER] = {
      right.failure(regs, off + left.layout) match {
        case some @ Some(_) => some
        case None           => left.failure(regs, off).map(identity)
      }
    }

    /** Combine per-step signals into a composite signal. Right wins
      * on failure or stop; otherwise propagate the left's signal so
      * the runner can decide to stop feeding. */
    private inline def compose(lSig: Int, rSig: Int): Int =
      if rSig == RegSignal.Failure then RegSignal.Failure
      else if rSig == RegSignal.Stop then RegSignal.Stop
      else if lSig == RegSignal.Failure then RegSignal.Failure
      else if lSig == RegSignal.Stop then RegSignal.Stop
      else RegSignal.Continue
  }
}
