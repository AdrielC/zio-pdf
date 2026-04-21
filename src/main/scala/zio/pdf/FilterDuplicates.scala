/*
 * Port of fs2.pdf.FilterDuplicates to Scala 3 + ZIO.
 *
 * The per-element logic is a pure state transition - "have we seen
 * object N before this xref?" - so it lives in `ZPure` and is
 * lifted into the stream by `StatefulPipe`. The only side effect
 * is the optional debug log at end-of-stream, which is run via
 * `applyEffect`'s `onDone` hook in proper ZIO (not smuggled into
 * ZPure).
 *
 * State has three parts:
 *
 *   - `nums`        : object numbers we've already seen
 *   - `duplicates`  : object numbers we've suppressed (used only
 *                     for the optional end-of-stream log)
 *   - `update`      : whether we've crossed the first xref boundary;
 *                     past that point the PDF is in incremental-
 *                     update mode and overwriting earlier objects
 *                     is legal, so we stop suppressing.
 */

package zio.pdf

import zio.ZIO
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

object FilterDuplicates {

  private[pdf] final case class State(
    nums: Set[Long],
    duplicates: Set[Long],
    update: Boolean
  )

  private val initial: State = State(Set.empty, Set.empty, update = false)

  /** Pure step: emit the input via the log unless it's a
    * duplicate-numbered indirect object. */
  private val step: StatefulPipe.Step[TopLevel, State, TopLevel] = {

    def emit(tl: TopLevel): ZPure[TopLevel, State, State, Any, Throwable, Unit] =
      ZPure.log[State, TopLevel](tl)

    def setUpdate: ZPure[TopLevel, State, State, Any, Throwable, Unit] =
      ZPure.update[State, State](_.copy(update = true))

    {
      case tl @ TopLevel.IndirectObjT(IndirectObj(Obj(Obj.Index(num, _), _), _)) =>
        ZPure.modify[State, State, Boolean] { s =>
          if (!s.update && s.nums.contains(num))
            (true, s.copy(duplicates = s.duplicates + num))
          else
            (false, s.copy(nums = s.nums + num))
        }.flatMap { dupe =>
          if (dupe) ZPure.unit else emit(tl)
        }

      case tl @ TopLevel.XrefT(_)      => setUpdate *> emit(tl)
      case tl @ TopLevel.StartXrefT(_) => setUpdate *> emit(tl)
      case other                        => emit(other)
    }
  }

  /**
   * Pipeline that suppresses duplicate-numbered indirect objects
   * up to (but not including) the first xref. The `log` is invoked
   * once at end-of-stream when duplicates were detected.
   */
  def pipe(log: Log = Log.noop): ZPipeline[Any, Throwable, TopLevel, TopLevel] =
    StatefulPipe.applyEffect[TopLevel, State, TopLevel](
      initial = initial,
      onDone = s =>
        if (s.duplicates.nonEmpty) log.debug(s"duplicate objects in pdf: ${s.duplicates}")
        else ZIO.unit
    )(step)
}
