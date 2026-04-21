/*
 * Port of fs2.pdf.FilterDuplicates to Scala 3 + ZIO.
 *
 * Suppresses duplicate-numbered IndirectObj outputs (a common
 * symptom of broken authoring tools), but only until the first xref
 * is seen, since incremental updates are allowed to overwrite
 * earlier objects.
 */

package zio.pdf

import zio.{Cause, Chunk}
import zio.stream.{ZChannel, ZPipeline}

object FilterDuplicates {

  private final case class State(nums: Set[Long], duplicates: Set[Long], update: Boolean) {
    def push(num: Long): (Boolean, State) = {
      val dupe = !update && nums.contains(num)
      val next = if (dupe) copy(duplicates = duplicates + num) else copy(nums = nums + num)
      (dupe, next)
    }
  }

  private def step(
    state: State
  ): ZChannel[Any, Throwable, Chunk[TopLevel], Any, Throwable, Chunk[TopLevel], State] =
    ZChannel.readWithCause[Any, Throwable, Chunk[TopLevel], Any, Throwable, Chunk[TopLevel], State](
      (chunk: Chunk[TopLevel]) => {
        var s   = state
        val out = Chunk.newBuilder[TopLevel]
        chunk.foreach {
          case tl @ TopLevel.IndirectObjT(IndirectObj(Obj(Obj.Index(num, _), _), _)) =>
            val (dupe, next) = s.push(num)
            s = next
            if (!dupe) out += tl
          case tl @ TopLevel.XrefT(_) =>
            s = s.copy(update = true); out += tl
          case tl @ TopLevel.StartXrefT(_) =>
            s = s.copy(update = true); out += tl
          case other =>
            out += other
        }
        ZChannel.write(out.result()) *> step(s)
      },
      (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
      (_: Any)                  => ZChannel.succeed(state)
    )

  /** Pipeline that suppresses duplicate-numbered objects.
    * The optional `log` is invoked once at end-of-stream when
    * duplicates were detected. */
  def pipe(log: Log = Log.noop): ZPipeline[Any, Throwable, TopLevel, TopLevel] =
    ZPipeline.fromChannel(
      step(State(Set.empty, Set.empty, false)).flatMap { finalState =>
        if (finalState.duplicates.nonEmpty)
          ZChannel.fromZIO(log.debug(s"duplicate objects in pdf: ${finalState.duplicates}")).unit
        else
          ZChannel.unit
      }
    )
}
