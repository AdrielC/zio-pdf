/*
 * The single-pass composition interpreter.
 *
 * Compiles a `FreeScan[I, O]` into a `Stepper[I, O, E]` -- an immutable
 * value that consumes one input at a time (or end-of-stream) and emits a
 * `StepEffect[O, E]`: zero or more outputs, optionally followed by a
 * `ScanDone` signal carrying any leftover state.
 *
 * Why steppers and not the Kyo `ScanProg.Of[...]` type directly?
 * Composition of `ScanProg`s would require connecting the left's `Emit[M]`
 * to the right's `Poll[M]` -- exactly the problem Kyo's own `Sink` PR
 * said it could not solve cleanly. The stepper formulation sidesteps
 * this: each stage owns its private state, the driver loop threads
 * outputs from one stage to the next, and the final stage's outputs
 * become the composed scan's outputs. The driver is then mapped onto
 * Kyo's `Poll`/`Emit`/`Abort` row by `ScanRunner` *once* at the top of
 * the call tree -- a single layer rather than N nested layers.
 *
 * Spine flattening: before stepping, left-nested `AndThen` chains are
 * rewritten into a `Vector` of stages by `flattenSpine`. This is the
 * structural analogue of Volga's `StateCont.compose`: O(1) stack depth
 * per step regardless of how the source scan was constructed. */

package zio.pdf.scan

import StepOut.*

import scala.collection.immutable.ArraySeq

/** What a single stepper call produces. */
final case class StepEffect[+O, +E](
    /** Outputs to push downstream (possibly empty). */
    out: Vector[O],
    /** If `Some`, the stepper has terminated with this signal. */
    done: Option[ScanDone[O, E]]
)

object StepEffect {
  def empty[O]: StepEffect[O, Nothing] = StepEffect(Vector.empty, None)
  def emit[O](outs: Iterable[O]): StepEffect[O, Nothing] =
    StepEffect(outs.toVector, None)
  def emit[O](o: O): StepEffect[O, Nothing] =
    StepEffect(Vector(o), None)
  def doneClean[O]: StepEffect[O, Nothing] =
    StepEffect(Vector.empty, Some(ScanDone.success[O]))
  def doneSuccess[O](leftover: Seq[O]): StepEffect[O, Nothing] =
    StepEffect(Vector.empty, Some(ScanDone.successWith[O](leftover)))
  def doneStop[O](leftover: Seq[O]): StepEffect[O, Nothing] =
    StepEffect(Vector.empty, Some(ScanDone.stopWith[O](leftover)))
  def doneFail[O, E](err: E, partial: Seq[O]): StepEffect[O, E] =
    StepEffect(Vector.empty, Some(ScanDone.failWith[O, E](err, partial)))
}

/** A purely-functional stepper: feed it an input or end-of-stream, get back
  * its next emission and a continuation. Equivalent in expressiveness to a
  * lazy unfold from `Option[I]` to `(StepEffect[O, E], Stepper[I, O, E])`,
  * but written as an interface for performance. */
trait Stepper[-I, +O, +E] { self =>

  /** Feed one input. */
  def step(i: I): (StepEffect[O, E], Stepper[I, O, E])

  /** Signal end-of-stream. The returned effect *must* have `done.isDefined`. */
  def end: StepEffect[O, E]

  /** Sequential composition. The driver threads `self`'s outputs into
    * `next` one at a time. */
  def andThen[O2, E2](next: Stepper[O, O2, E2]): Stepper[I, O2, E | E2] =
    Stepper.composeAndThen[I, O, O2, E, E2](self, next)
}

object SinglePassInterp {

  /** Flatten left-nested `AndThen` into a (head primitive, tail) shape. The
    * tail is any stage that follows the head; a chain
    * `((((a >>> b) >>> c) >>> d))` flattens to `(a, [b, c, d])`. */
  def flattenSpine[I, O](scan: FreeScan[I, O]): Vector[FreeScan[Any, Any]] = {
    val out = scala.collection.mutable.ArrayBuffer.empty[FreeScan[Any, Any]]
    def go(s: FreeScan[Any, Any]): Unit = s match {
      case FreeScan.AndThen(l, r) =>
        go(l.asInstanceOf[FreeScan[Any, Any]])
        go(r.asInstanceOf[FreeScan[Any, Any]])
      case other =>
        out += other
    }
    go(scan.asInstanceOf[FreeScan[Any, Any]])
    out.toVector
  }

  /** Compile a `FreeScan` into a `Stepper`. Applies fusion before lowering. */
  def compile[I, O](scan: FreeScan[I, O]): Stepper[I, O, Any] =
    Fusion.tryFuse(scan) match {
      case Some(f) =>
        Stepper.pure(f)
      case None =>
        flattenSpine(scan) match {
          case Vector(only)         =>
            compileNode(only).asInstanceOf[Stepper[I, O, Any]]
          case stages if stages.nonEmpty =>
            stages
              .map(compileNode)
              .reduceLeft[Stepper[Any, Any, Any]] { (acc, next) =>
                acc.andThen(next).asInstanceOf[Stepper[Any, Any, Any]]
              }
              .asInstanceOf[Stepper[I, O, Any]]
          case _ =>
            // Empty spine cannot occur -- `flattenSpine` of any scan
            // produces at least one node.
            Stepper.identity.asInstanceOf[Stepper[I, O, Any]]
        }
    }

  /** Compile a single non-`AndThen` node. The match is widened to
    * `FreeScan[?, ?]` to bypass GADT-driven case elimination -- Scala 3
    * (correctly) notes that `FreeScan[Any, Any]` cannot statically be a
    * `Choice` because `Any` is not `Either[_, _]`, but at runtime that
    * narrowing is too aggressive: the spine contains nodes whose type
    * arguments we have erased on purpose. */
  private def compileNode(node: FreeScan[Any, Any]): Stepper[Any, Any, Any] = {
    val widened: FreeScan[?, ?] = node
    widened match {
      case FreeScan.Arr(f) =>
        Stepper.pure(f).asInstanceOf[Stepper[Any, Any, Any]]
      case FreeScan.Prim(p) =>
        compilePrim(p.asInstanceOf[ScanPrim[Any, StepOut[Any]]])
      case FreeScan.Fanout(l, r) =>
        Stepper
          .fanout(
            compile(l.asInstanceOf[FreeScan[Any, Any]]),
            compile(r.asInstanceOf[FreeScan[Any, Any]])
          )
          .asInstanceOf[Stepper[Any, Any, Any]]
      case FreeScan.Choice(l, r) =>
        Stepper
          .choice(
            compile(l.asInstanceOf[FreeScan[Any, Any]]),
            compile(r.asInstanceOf[FreeScan[Any, Any]])
          )
          .asInstanceOf[Stepper[Any, Any, Any]]
      case FreeScan.AndThen(_, _) =>
        // Should never happen -- `flattenSpine` removed all `AndThen`s.
        sys.error("unreachable: AndThen in compileNode")
    }
  }

  /** Compile a primitive into its dedicated stepper. */
  private def compilePrim[I, O](p: ScanPrim[I, StepOut[O]]): Stepper[I, O, Any] =
    p match {
      case ScanPrim.Map(f)         => Stepper.pure(f.asInstanceOf[I => O])
      case ScanPrim.Filter(pred)   => Stepper.filter(pred.asInstanceOf[I => Boolean]).asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.Take(n)        => Stepper.take[I](n).asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.Drop(n)        => Stepper.drop[I](n).asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.Fold(s, step)  => Stepper.fold[I, O](s.asInstanceOf[O], step.asInstanceOf[(O, I) => O])
      case ScanPrim.Hash(algo)     => Stepper.hash(algo).asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.CountBytes     => Stepper.countBytes.asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.BombGuard(max) => Stepper.bombGuard(max).asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.FastCDC(mn, av, mx)  => Stepper.fastCdc(mn, av, mx).asInstanceOf[Stepper[I, O, Any]]
      case ScanPrim.FixedChunk(n)        => Stepper.fixedChunk(n).asInstanceOf[Stepper[I, O, Any]]
    }

  /** Drive a compiled scan against an iterable of inputs. Pure -- no Kyo
    * effects. Useful in tests and as a building block for the Kyo runner. */
  def runDirect[I, O, E](scan: FreeScan[I, O], inputs: Iterable[I]): (ScanDone[O, E], Vector[O]) = {
    val initial: Stepper[I, O, Any] = compile(scan)
    val emitted = Vector.newBuilder[O]
    var current: Stepper[I, O, Any] = initial
    var done: Option[ScanDone[O, Any]] = None
    val it = inputs.iterator
    while done.isEmpty && it.hasNext do {
      val (eff, next) = current.step(it.next())
      emitted ++= eff.out
      done = eff.done
      current = next
    }
    if done.isEmpty then {
      val eff = current.end
      emitted ++= eff.out
      done = eff.done.orElse(Some(ScanDone.success[O]))
    }
    val signal = done.get
    val all = emitted.result() ++ signal.leftoverSeq.toVector
    (signal.asInstanceOf[ScanDone[O, E]], all)
  }
}

/** Stepper constructors. Each function is a closure-light, allocation-light
  * implementation of one primitive. */
object Stepper {

  /** A stateless map. */
  def pure[I, O](f: I => O): Stepper[I, O, Nothing] = new Stepper[I, O, Nothing] {
    def step(i: I) = (StepEffect.emit[O](f(i)), this)
    def end = StepEffect.doneClean[O]
  }

  /** Pass-through identity, never terminates until `end`. */
  def identity[A]: Stepper[A, A, Nothing] = pure[A, A](a => a)

  /** Filter -- emits each input that satisfies the predicate. */
  def filter[A](p: A => Boolean): Stepper[A, A, Nothing] = new Stepper[A, A, Nothing] {
    def step(a: A) =
      if p(a) then (StepEffect.emit(a), this)
      else (StepEffect.empty[A], this)
    def end = StepEffect.doneClean[A]
  }

  /** Take the first `n`, then stop. */
  def take[A](n: Int): Stepper[A, A, Nothing] = {
    def loop(remaining: Int): Stepper[A, A, Nothing] = new Stepper[A, A, Nothing] {
      def step(a: A) =
        if remaining <= 0 then
          (StepEffect.doneStop[A](Seq.empty), this)
        else if remaining == 1 then
          (StepEffect(Vector(a), Some(ScanDone.stop[A])), this)
        else
          (StepEffect.emit(a), loop(remaining - 1))
      def end = StepEffect.doneClean[A]
    }
    loop(n)
  }

  /** Drop the first `n`, then become identity. */
  def drop[A](n: Int): Stepper[A, A, Nothing] = {
    def loop(remaining: Int): Stepper[A, A, Nothing] = new Stepper[A, A, Nothing] {
      def step(a: A) =
        if remaining <= 0 then (StepEffect.emit(a), this)
        else (StepEffect.empty[A], loop(remaining - 1))
      def end = StepEffect.doneClean[A]
    }
    loop(n)
  }

  /** Stateful fold. Output type matches the accumulator; the final value is
    * carried out as the leftover of `ScanDone.Success`. */
  def fold[I, S](seed: S, f: (S, I) => S): Stepper[I, S, Nothing] = {
    def loop(s: S): Stepper[I, S, Nothing] = new Stepper[I, S, Nothing] {
      def step(i: I) = (StepEffect.empty[S], loop(f(s, i)))
      def end = StepEffect.doneSuccess[S](Seq(s))
    }
    loop(seed)
  }

  /** Hash bytes into a `MessageDigest`. The digest bytes are the leftover
    * of `ScanDone.Success`. */
  def hash(algo: HashAlgo): Stepper[Byte, Byte, Nothing] = {
    val md = algo.newDigest()
    new Stepper[Byte, Byte, Nothing] {
      def step(b: Byte) = {
        md.update(b)
        (StepEffect.empty[Byte], this)
      }
      def end = StepEffect.doneSuccess[Byte](ArraySeq.unsafeWrapArray(md.digest()))
    }
  }

  /** Count bytes; deliver count as 8 big-endian bytes in the leftover. */
  def countBytes: Stepper[Byte, Byte, Nothing] = {
    def loop(n: Long): Stepper[Byte, Byte, Nothing] = new Stepper[Byte, Byte, Nothing] {
      def step(b: Byte) = (StepEffect.empty[Byte], loop(n + 1L))
      def end = {
        val arr = java.nio.ByteBuffer.allocate(8).putLong(n).array()
        StepEffect.doneSuccess[Byte](ArraySeq.unsafeWrapArray(arr))
      }
    }
    loop(0L)
  }

  /** Pass bytes through; abort on overflow. */
  def bombGuard(maxBytes: Long): Stepper[Byte, Byte, BombError] = {
    def loop(n: Long): Stepper[Byte, Byte, BombError] = new Stepper[Byte, Byte, BombError] {
      def step(b: Byte) = {
        val n1 = n + 1L
        if n1 > maxBytes then
          (StepEffect.doneFail[Byte, BombError](BombError(n1, maxBytes), Seq.empty), this)
        else
          (StepEffect.emit(b), loop(n1))
      }
      def end = StepEffect.doneClean[Byte]
    }
    loop(0L)
  }

  /** Fixed-size chunker. Emits a `Chunk[Byte]` whenever the buffer hits `n`
    * bytes. Trailing buffer flushed via the leftover. */
  def fixedChunk(n: Int): Stepper[Byte, kyo.Chunk[Byte], Nothing] = {
    require(n > 0, "fixedChunk size must be positive")
    def loop(buf: Vector[Byte]): Stepper[Byte, kyo.Chunk[Byte], Nothing] =
      new Stepper[Byte, kyo.Chunk[Byte], Nothing] {
        def step(b: Byte) = {
          val next = buf :+ b
          if next.size >= n then
            (StepEffect(Vector(kyo.Chunk.from(next)), None), loop(Vector.empty))
          else
            (StepEffect.empty[kyo.Chunk[Byte]], loop(next))
        }
        def end =
          if buf.isEmpty then StepEffect.doneClean[kyo.Chunk[Byte]]
          else StepEffect.doneSuccess[kyo.Chunk[Byte]](Seq(kyo.Chunk.from(buf)))
      }
    loop(Vector.empty)
  }

  /** Content-defined chunker, using the existing `zio.pdf.cdc.FastCdc`. */
  def fastCdc(min: Int, avg: Int, max: Int): Stepper[Byte, kyo.Chunk[Byte], Nothing] = {
    val cfg = zio.pdf.cdc.FastCdc.Config(min, avg, max)

    // Drain buffer producing as many CDC chunks as can be emitted with the
    // current information; returns chunks and leftover bytes.
    def drain(buf: Vector[Byte], flushTail: Boolean): (Vector[kyo.Chunk[Byte]], Vector[Byte]) = {
      var b   = buf
      val out = Vector.newBuilder[kyo.Chunk[Byte]]
      while b.length >= cfg.maxSize || (flushTail && b.nonEmpty) do {
        val arr =
          if b.length >= cfg.maxSize then b.take(cfg.maxSize).toArray
          else b.toArray
        val cut   = zio.pdf.cdc.FastCdc.cutOffsetForScan(arr, cfg)
        val chunk = b.take(cut)
        b = b.drop(cut)
        out += kyo.Chunk.from(chunk)
      }
      (out.result(), b)
    }

    def loop(buf: Vector[Byte]): Stepper[Byte, kyo.Chunk[Byte], Nothing] =
      new Stepper[Byte, kyo.Chunk[Byte], Nothing] {
        def step(b: Byte) = {
          val next           = buf :+ b
          val (chunks, rest) = drain(next, flushTail = false)
          (StepEffect[kyo.Chunk[Byte], Nothing](chunks, None), loop(rest))
        }
        def end = {
          val (chunks, _) = drain(buf, flushTail = true)
          StepEffect.doneSuccess[kyo.Chunk[Byte]](chunks)
        }
      }
    loop(Vector.empty)
  }

  /** Sequential composition.
    *
    * The driver threads the left's outputs through the right one at a
    * time. If the right signals a `ScanDone`, the composed scan signals
    * the corresponding `ScanDone` immediately (carrying any partial
    * outputs the right managed to emit). If the left signals a
    * `ScanDone`, its leftover is fed through the right -- and then the
    * right's `end` is consulted to drain its own state. */
  def composeAndThen[I, M, O, EL, ER](
      left: Stepper[I, M, EL],
      right: Stepper[M, O, ER]
  ): Stepper[I, O, EL | ER] = new Stepper[I, O, EL | ER] {
    def step(i: I): (StepEffect[O, EL | ER], Stepper[I, O, EL | ER]) = {
      val (le, ln) = left.step(i)
      val (re, rn) = feedThrough(right, le.out)
      // Determine the composed completion: right wins if it signals;
      // otherwise the left's signal is propagated by feeding its
      // leftover through and finalising the right.
      (le.done, re.done) match {
        case (None, None) =>
          (StepEffect[O, EL | ER](re.out, None), composeAndThen(ln, rn))
        case (_, Some(rs)) =>
          // Right signalled -- composition ends here, regardless of left.
          (StepEffect[O, EL | ER](re.out, Some(translateRight[O, ER](rs))), terminated[I, O, EL | ER])
        case (Some(ls), None) =>
          // Left signalled -- feed its leftover through the right, then
          // finalise the right by calling `end`.
          val (le2, rn2) = feedThrough(rn, ls.leftoverAny.asInstanceOf[Seq[M]].toVector)
          val finalEff   = rn2.end
          val combinedOut = re.out ++ le2.out ++ finalEff.out
          val finalSig   = composeFinalSignal[O, EL, ER](ls.asInstanceOf[ScanDone[M, EL]], finalEff.done)
          (StepEffect[O, EL | ER](combinedOut, Some(finalSig)), terminated[I, O, EL | ER])
      }
    }

    def end: StepEffect[O, EL | ER] = {
      val le = left.end
      val (re, rn) = feedThrough(right, le.out)
      val rightLeftover = le.done.map(_.leftoverAny.asInstanceOf[Seq[M]]).getOrElse(Seq.empty)
      val (le2, rn2) = feedThrough(rn, rightLeftover.toVector)
      val finalEff = rn2.end
      val combinedOut = re.out ++ le2.out ++ finalEff.out
      val leftSig: ScanDone[M, EL] = le.done.getOrElse(ScanDone.success[M]).asInstanceOf[ScanDone[M, EL]]
      val finalSig = composeFinalSignal[O, EL, ER](leftSig, finalEff.done)
      StepEffect[O, EL | ER](combinedOut, Some(finalSig))
    }
  }

  /** Once a stepper signals `done`, any further `step` calls return the
    * same empty effect with the same terminal signal. */
  private def terminated[I, O, E]: Stepper[I, O, E] = new Stepper[I, O, E] {
    def step(i: I) = (StepEffect.empty[O], this)
    def end        = StepEffect.empty[O].copy(done = Some(ScanDone.success[O]))
  }

  /** Feed a sequence of inputs through a stepper, collecting outputs and
    * stopping early if the stepper terminates partway through. Returns the
    * accumulated effect and the surviving stepper (or a `terminated` if
    * the stepper finished). */
  private def feedThrough[I, O, E](
      stepper: Stepper[I, O, E],
      inputs: Vector[I]
  ): (StepEffect[O, E], Stepper[I, O, E]) = {
    var st: Stepper[I, O, E] = stepper
    var collected = Vector.empty[O]
    var sig: Option[ScanDone[O, E]] = None
    val it = inputs.iterator
    while sig.isEmpty && it.hasNext do {
      val (eff, next) = st.step(it.next())
      collected = collected ++ eff.out
      sig = eff.done
      st = next
    }
    val survivor: Stepper[I, O, E] =
      if sig.isDefined then terminated[I, O, E] else st
    (StepEffect(collected, sig), survivor)
  }

  /** Reinterpret a right-stage signal as a composition signal. The right's
    * `Failure` widens to `EL | ER`. */
  private def translateRight[O, ER](rs: ScanDone[O, ER]): ScanDone[O, ER] = rs

  /** Combine the left's signal with whatever the right reported when we
    * finalised it. Precedence: `Failure > Stop > Success`, with the right's
    * partial outputs already accumulated by the caller. */
  private def composeFinalSignal[O, EL, ER](
      leftSig: ScanDone[?, EL],
      rightSig: Option[ScanDone[O, ER]]
  ): ScanDone[O, EL | ER] = {
    leftSig match {
      case ScanDone.Failure(e, _) =>
        // Left failed: propagate its error (right's signal, if any, is
        // lost because left's failure short-circuits the composition).
        ScanDone.failWith[O, EL | ER](e, rightSig.toSeq.flatMap(_.leftoverSeq))
      case ScanDone.Stop(_) =>
        rightSig match {
          case Some(rs: ScanDone.Failure[O, ER] @unchecked) =>
            ScanDone.failWith[O, EL | ER](rs.err, rs.partial)
          case _ =>
            ScanDone.stopWith[O](rightSig.fold(Seq.empty[O])(_.leftoverSeq))
        }
      case ScanDone.Success(_) =>
        rightSig match {
          case Some(rs: ScanDone.Failure[O, ER] @unchecked) =>
            ScanDone.failWith[O, EL | ER](rs.err, rs.partial)
          case Some(ScanDone.Stop(l)) =>
            ScanDone.stopWith[O](l)
          case Some(ScanDone.Success(l)) =>
            ScanDone.successWith[O](l)
          case None =>
            ScanDone.success[O]
        }
    }
  }

  /** Fanout: run two scans on the *same* input stream, zip their outputs.
    * The composed scan emits `(OL, OR)` pairs; the zipping is positional --
    * the i-th left output is paired with the i-th right output, and any
    * surplus on either side is buffered until the partner catches up. */
  def fanout[I, OL, OR, EL, ER](
      left: Stepper[I, OL, EL],
      right: Stepper[I, OR, ER]
  ): Stepper[I, (OL, OR), EL | ER] = {
    def loop(
        left: Stepper[I, OL, EL],
        right: Stepper[I, OR, ER],
        leftQ: Vector[OL],
        rightQ: Vector[OR]
    ): Stepper[I, (OL, OR), EL | ER] = new Stepper[I, (OL, OR), EL | ER] {
      def step(i: I) = {
        val (le, ln) = left.step(i)
        val (re, rn) = right.step(i)
        val newLeftQ  = leftQ ++ le.out
        val newRightQ = rightQ ++ re.out
        val pairCount = math.min(newLeftQ.size, newRightQ.size)
        val ldone     = le.done
        val rdone     = re.done
        val sig: Option[ScanDone[(OL, OR), EL | ER]] = (ldone, rdone) match {
          case (Some(ls: ScanDone.Failure[OL, EL] @unchecked), _) =>
            Some(ScanDone.failWith[(OL, OR), EL | ER](ls.err, Seq.empty))
          case (_, Some(rs: ScanDone.Failure[OR, ER] @unchecked)) =>
            Some(ScanDone.failWith[(OL, OR), EL | ER](rs.err, Seq.empty))
          case (Some(_), Some(_)) =>
            Some(ScanDone.success[(OL, OR)])
          case (Some(_), None) | (None, Some(_)) =>
            // One side done -- continue with the other until inputs run
            // out or it also terminates.
            None
          case (None, None) =>
            None
        }
        (
          StepEffect((newLeftQ.take(pairCount) zip newRightQ.take(pairCount))
                       .map(_.asInstanceOf[(OL, OR)]), sig),
          if sig.isDefined then terminated[I, (OL, OR), EL | ER]
          else loop(ln, rn, newLeftQ.drop(pairCount), newRightQ.drop(pairCount))
        )
      }
      def end: StepEffect[(OL, OR), EL | ER] = {
        val le = left.end
        val re = right.end
        val newLeftQ  = leftQ ++ le.out
        val newRightQ = rightQ ++ re.out
        val pairCount = math.min(newLeftQ.size, newRightQ.size)
        val pairs     = (0 until pairCount).map(idx => (newLeftQ(idx), newRightQ(idx))).toVector
        val sig = (le.done, re.done) match {
          case (Some(ls: ScanDone.Failure[OL, EL] @unchecked), _) =>
            ScanDone.failWith[(OL, OR), EL | ER](ls.err, Seq.empty)
          case (_, Some(rs: ScanDone.Failure[OR, ER] @unchecked)) =>
            ScanDone.failWith[(OL, OR), EL | ER](rs.err, Seq.empty)
          case _ =>
            ScanDone.success[(OL, OR)]
        }
        StepEffect(pairs, Some(sig))
      }
    }
    loop(left, right, Vector.empty, Vector.empty)
  }

  /** Choice: route each `Either[IL, IR]` to the matching arm. */
  def choice[IL, IR, O, EL, ER](
      left: Stepper[IL, O, EL],
      right: Stepper[IR, O, ER]
  ): Stepper[Either[IL, IR], O, EL | ER] = {
    def loop(
        left: Stepper[IL, O, EL],
        right: Stepper[IR, O, ER]
    ): Stepper[Either[IL, IR], O, EL | ER] = new Stepper[Either[IL, IR], O, EL | ER] {
      def step(i: Either[IL, IR]) = i match {
        case Left(l) =>
          val (eff, ln) = left.step(l)
          val sig = eff.done.map {
            case ScanDone.Failure(e, p) =>
              ScanDone.failWith[O, EL | ER](e.asInstanceOf[EL | ER], p)
            case ScanDone.Stop(s)       => ScanDone.stopWith[O](s)
            case ScanDone.Success(s)    => ScanDone.successWith[O](s)
          }
          (StepEffect[O, EL | ER](eff.out, sig), if sig.isDefined then terminated[Either[IL, IR], O, EL | ER] else loop(ln, right))
        case Right(r) =>
          val (eff, rn) = right.step(r)
          val sig = eff.done.map {
            case ScanDone.Failure(e, p) =>
              ScanDone.failWith[O, EL | ER](e.asInstanceOf[EL | ER], p)
            case ScanDone.Stop(s)       => ScanDone.stopWith[O](s)
            case ScanDone.Success(s)    => ScanDone.successWith[O](s)
          }
          (StepEffect[O, EL | ER](eff.out, sig), if sig.isDefined then terminated[Either[IL, IR], O, EL | ER] else loop(left, rn))
      }
      def end: StepEffect[O, EL | ER] = {
        val le = left.end
        val re = right.end
        val sig = (le.done, re.done) match {
          case (Some(ScanDone.Failure(e, p)), _) =>
            ScanDone.failWith[O, EL | ER](e.asInstanceOf[EL | ER], p)
          case (_, Some(ScanDone.Failure(e, p))) =>
            ScanDone.failWith[O, EL | ER](e.asInstanceOf[EL | ER], p)
          case _ =>
            ScanDone.successWith[O](le.done.toSeq.flatMap(_.leftoverSeq) ++ re.done.toSeq.flatMap(_.leftoverSeq))
        }
        StepEffect[O, EL | ER](le.out ++ re.out, Some(sig))
      }
    }
    loop(left, right)
  }
}
