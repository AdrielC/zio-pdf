/*
 * Port of fs2.pdf.FilterDuplicates to Scala 3 + ZIO.
 *
 * Suppresses duplicate-numbered indirect objects before the first xref.
 * Uses [[DuplicateFilterState]] (fixed bit table, no [[Set]] growth).
 */

package zio.pdf

import zio.ZIO
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

object FilterDuplicates {

  private final case class State(filter: DuplicateFilterState.Mutable)

  private val initial: State = State(DuplicateFilterState.initial)

  private val step: StatefulPipe.Step[TopLevel, State, TopLevel] = {

    def emit(tl: TopLevel): ZPure[TopLevel, State, State, Any, Throwable, Unit] =
      ZPure.log[State, TopLevel](tl)

    def setUpdate: ZPure[TopLevel, State, State, Any, Throwable, Unit] =
      ZPure.get[State].flatMap { s =>
        DuplicateFilterState.enterUpdateMode(s.filter)
        ZPure.unit
      }

    {
      case tl @ TopLevel.IndirectObjT(IndirectObj(Obj(Obj.Index(num, _), _), _)) =>
        ZPure.get[State].flatMap { s =>
          if (DuplicateFilterState.shouldSuppress(s.filter, num)) ZPure.unit
          else emit(tl)
        }

      case tl @ TopLevel.XrefT(_)      => setUpdate *> emit(tl)
      case tl @ TopLevel.StartXrefT(_) => setUpdate *> emit(tl)
      case other                        => emit(other)
    }
  }

  def pipe(log: Log = Log.noop): ZPipeline[Any, Throwable, TopLevel, TopLevel] =
    StatefulPipe.applyEffect[TopLevel, State, TopLevel](
      initial = initial,
      onDone = s =>
        if (s.filter.duplicateCount > 0)
          log.debug(
            s"duplicate indirect objects suppressed before first xref (approximate count: ${s.filter.duplicateCount})"
          )
        else ZIO.unit
    )(step)
}
