/*
 * The runtime effect type of a scan.
 *
 * `Poll[I]`  — pull-based input with backpressure
 * `Emit[O]`  — push-based output
 * `Abort[ScanDone[O, E]]` — typed completion with leftover
 * `S`        — additional effects (Var, Scope, Sync, ...)
 *
 * A scan program runs forever until either:
 *   - upstream signals end-of-input via `Maybe.Absent`, in which case the
 *     scan flushes its state and aborts with `ScanDone.Success(leftover)`;
 *   - the scan stops early (e.g. `take(n)`) and aborts with
 *     `ScanDone.Stop(leftover)`;
 *   - the scan fails and aborts with `ScanDone.Failure(err, partial)`.
 *
 * The Abort payload carries the leftover so the runner can append it to
 * what `Emit` already collected -- without re-emitting and without breaking
 * the symmetry of the Stream/Poll pair.
 */

package zio.pdf.scan

import kyo.*

object ScanProg {

  /** A scan program: consumes Is, emits Os, can fail with `ScanDone[O, E]`,
    * uses ambient effects `S`.
    *
    * The Abort row carries the non-generic `ScanSignal` (because Kyo's
    * `ConcreteTag` cannot represent generic types) but every signal is
    * actually a `ScanDone[O, E]` -- the `O` and `E` parameters are
    * type-level documentation that the runner reattaches when it converts
    * the runtime signal back into a typed `ScanDone[O, E]`. */
  type Of[I, O, E, S] = Unit < (Poll[I] & Emit[O] & Abort[ScanSignal] & S)
}

