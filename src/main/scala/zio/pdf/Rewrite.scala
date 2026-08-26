/*
 * Port of fs2.pdf.Rewrite to Scala 3 + ZIO.
 *
 * The legacy version was built around fs2.Pull; the ZIO equivalent
 * uses StatefulPipe's synchronous kernel. Per-element work is pure:
 * take the RewriteState, emit zero-or-more Part values, and return
 * the next state. The finalizer can emit any number of parts too.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.Chunk
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

final case class RewriteState[S](state: S, trailer: Option[Trailer], root: Option[Prim.Ref])

object RewriteState {
  def cons[S](state: S): RewriteState[S] = RewriteState(state, None, None)
}

final case class RewriteUpdate[S](state: S, trailer: Trailer)

/**
 * Rewrite an `A`-stream into a `Part[Trailer]`-stream by:
 *   1. running a stateful `collect` step per element
 *   2. running a final `update` once the stream has finished
 */
object Rewrite {

  /** Output and next state from one pure collection step. */
  final case class Emission[S](parts: Chunk[Part[Trailer]], state: RewriteState[S])

  /** Pure, multi-output collection step. This is the ZIO equivalent of fs2's Pull collect callback. */
  type Collect[S, A] = RewriteState[S] => A => Either[Throwable, Emission[S]]

  /** Pure, multi-output final step. This is the ZIO equivalent of fs2's Pull update callback. */
  type Update[S] = RewriteUpdate[S] => Either[Throwable, Chunk[Part[Trailer]]]

  private def finish[S](state: RewriteState[S]): Either[Throwable, RewriteUpdate[S]] =
    state match {
      case RewriteState(value, Some(trailer), _) => Right(RewriteUpdate(value, trailer))
      case RewriteState(value, None, Some(root)) =>
        Right(RewriteUpdate(value, Trailer(BigDecimal(-1), Prim.dict("Root" -> root), Some(root))))
      case RewriteState(_, None, None) =>
        Left(new RuntimeException("no trailer or root in rewrite stream"))
    }

  /**
   * Full streaming rewrite. Both collection and finalization may emit any
   * number of parts; finalization receives a guaranteed trailer.
   */
  def parts[S, A](initial: S)(
    collect: Collect[S, A]
  )(
    update: Update[S]
  ): ZPipeline[Any, Throwable, A, Part[Trailer]] =
    StatefulPipe.fromSync[A, RewriteState[S], Part[Trailer]](
      RewriteState.cons(initial),
      state =>
        finish(state).flatMap { completed =>
          update(completed).map { emitted =>
            Chunk.single(Part.Meta(completed.trailer): Part[Trailer]) ++ emitted
          }
        },
      (state, value) => collect(state)(value).map(emission => (emission.parts, emission.state))
    )

  /** Full rewrite followed by PDF encoding. */
  def apply[S, A](initial: S)(
    collect: Collect[S, A]
  )(
    update: Update[S]
  ): ZPipeline[Any, Throwable, A, ByteVector] =
    parts(initial)(collect)(update) >>> WritePdf.parts

  /**
   * Run the same collection protocol for analysis-only work. Emitted parts are
   * intentionally discarded; the final user state is produced once the input
   * has supplied a trailer or root reference.
   */
  def forState[S, A](initial: S)(
    collect: Collect[S, A]
  ): ZPipeline[Any, Throwable, A, S] =
    StatefulPipe.fromSync[A, RewriteState[S], S](
      RewriteState.cons(initial),
      state => finish(state).map(completed => Chunk.single(completed.state)),
      (state, value) => collect(state)(value).map(emission => (Chunk.empty, emission.state))
    )

  /**
   * Convenience adapter for one where collection is infallible and finalization
   * emits exactly one part. Prefer [[parts]] for non-trivial rewrites.
   */
  def simpleParts[S, A](initial: S)(
    collect: RewriteState[S] => A => (List[Part[Trailer]], RewriteState[S])
  )(
    update: RewriteUpdate[S] => Part[Trailer]
  ): ZPipeline[Any, Throwable, A, Part[Trailer]] = {
    type St = RewriteState[S]

    val step: StatefulPipe.Step[A, St, Part[Trailer]] = value =>
      ZPure.modify[St, St, List[Part[Trailer]]] { state =>
        val (emitted, next) = collect(state)(value)
        (emitted, next)
      }.flatMap { emitted =>
        emitted.foldLeft[ZPure[Part[Trailer], St, St, Any, Throwable, Unit]](ZPure.unit) {
          (acc, part) => acc *> ZPure.log[St, Part[Trailer]](part)
        }
      }

    val finalize: St => ZPure[Part[Trailer], St, St, Any, Throwable, Unit] = state =>
      finish(state) match {
        case Left(error) => ZPure.fail(error)
        case Right(completed) =>
          ZPure.log[St, Part[Trailer]](Part.Meta(completed.trailer): Part[Trailer]) *>
            ZPure.log[St, Part[Trailer]](update(completed))
      }

    StatefulPipe[A, St, Part[Trailer]](RewriteState.cons(initial), finalize)(step)
  }

  /** Convenience: rewrite + encode in one shot. */
  def simple[S, A](initial: S)(
    collect: RewriteState[S] => A => (List[Part[Trailer]], RewriteState[S])
  )(
    update: RewriteUpdate[S] => Part[Trailer]
  ): ZPipeline[Any, Throwable, A, ByteVector] =
    simpleParts(initial)(collect)(update) >>> WritePdf.parts

  /** Default tail behaviour: re-emit the trailer as a Meta. */
  def noUpdate[S]: RewriteUpdate[S] => Part[Trailer] = u => Part.Meta(u.trailer)
}
