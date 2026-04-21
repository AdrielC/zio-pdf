/*
 * Typed completion with leftover.
 *
 * Kyo's own `Sink` PR punted on the leftover problem:
 *
 *   "remaining chunks would have to be re-emitted after being handled by
 *    the poll function ... This would break the symmetry of Stream and
 *    Poll."
 *
 * The fix: model completion as `Abort[ScanDone[O, E]]`. Leftovers travel
 * *out* of the effect stack in the abort payload, collected by `Emit.run`
 * alongside regular emissions. No re-emission. No symmetry breaking.
 */

package zio.pdf.scan

/** Concrete (non-generic) base class used as the `Abort` payload type.
  *
  * Why a separate class? Kyo's `ConcreteTag` -- required by `Abort.run` --
  * cannot represent generic types. So we erase `ScanDone`'s type parameters
  * at the abort boundary to a single concrete class and keep the typed view
  * (`ScanDone[O, E]`) as the API surface. This is purely a representation
  * detail; the runner converts back to the typed enum before returning. */
sealed trait ScanSignal extends Product with Serializable {

  /** The leftover sequence carried by every signal. Erased to `Seq[Any]` at
    * the abort boundary; the runner casts back to `Seq[O]`. */
  def leftoverAny: Seq[Any]
}

enum ScanDone[+O, +E] extends ScanSignal {

  /** Input exhausted cleanly. `leftover` is buffered state to flush
    * downstream when the scan is composed. */
  case Success[+O](leftover: Seq[O]) extends ScanDone[O, Nothing]

  /** Scan stopped early (e.g. `take(n)`). `leftover` is what was accumulated
    * but not yet emitted. The driver may stop feeding this scan but continue
    * with remaining input. */
  case Stop[+O](leftover: Seq[O]) extends ScanDone[O, Nothing]

  /** Scan failed. `partial` is what made it out before failure. */
  case Failure[+O, +E](err: E, partial: Seq[O]) extends ScanDone[O, E]

  /** All three constructors carry a leftover sequence; this projects it
    * uniformly so callers can collect outputs without pattern-matching. */
  def leftoverSeq: Seq[O] = this match {
    case Success(l)    => l
    case Stop(l)       => l
    case Failure(_, l) => l
  }

  override def leftoverAny: Seq[Any] = leftoverSeq
}

object ScanDone {

  /** A clean success carrying no leftover. */
  def success[O]: ScanDone[O, Nothing] = Success(Seq.empty)

  /** A clean success carrying the given leftover. */
  def successWith[O](leftover: Seq[O]): ScanDone[O, Nothing] = Success(leftover)

  /** An early stop carrying no leftover. */
  def stop[O]: ScanDone[O, Nothing] = Stop(Seq.empty)

  /** An early stop carrying the given leftover. */
  def stopWith[O](leftover: Seq[O]): ScanDone[O, Nothing] = Stop(leftover)

  /** A failure carrying no partial output. */
  def fail[O, E](e: E): ScanDone[O, E] = Failure(e, Seq.empty)

  /** A failure carrying partial output emitted before the failure. */
  def failWith[O, E](e: E, partial: Seq[O]): ScanDone[O, E] = Failure(e, partial)
}
