/*
 * Port of fs2.pdf.Rewrite to Scala 3 + ZIO.
 *
 * The legacy version was built around fs2.Pull; the ZIO equivalent
 * uses StatefulPipe + ZPure. Per-element work is pure: take the
 * RewriteState, apply the user-supplied collect, emit zero-or-more
 * Part[Trailer] values. The tail end-of-stream emit (the trailing
 * Meta from RewriteUpdate) is also pure.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
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

  /**
   * Stateful collect: each `A` produces a list of `Part[Trailer]`
   * outputs and an updated state. Pure all the way down.
   */
  def simpleParts[S, A](initial: S)(
    collect: RewriteState[S] => A => (List[Part[Trailer]], RewriteState[S])
  )(
    update: RewriteUpdate[S] => Part[Trailer]
  ): ZPipeline[Any, Throwable, A, Part[Trailer]] = {

    type St = RewriteState[S]

    val step: StatefulPipe.Step[A, St, Part[Trailer]] = a =>
      ZPure.modify[St, St, List[Part[Trailer]]] { s =>
        val (parts, next) = collect(s)(a)
        (parts, next)
      }.flatMap { parts =>
        parts.foldLeft[ZPure[Part[Trailer], St, St, Any, Throwable, Unit]](ZPure.unit) {
          (acc, p) => acc *> ZPure.log[St, Part[Trailer]](p)
        }
      }

    val finalize: St => ZPure[Part[Trailer], St, St, Any, Throwable, Unit] = s => s match {
      case RewriteState(state, Some(trailer), _) =>
        // Emit the trailing update Meta, threading the trailer through
        // RewriteUpdate.
        val ru = RewriteUpdate(state, trailer)
        ZPure.log[St, Part[Trailer]](Part.Meta(trailer): Part[Trailer]) *>
          ZPure.log[St, Part[Trailer]](update(ru))
      case RewriteState(state, None, Some(root)) =>
        val trailer = Trailer(BigDecimal(-1), Prim.dict("Root" -> root), Some(root))
        val ru      = RewriteUpdate(state, trailer)
        ZPure.log[St, Part[Trailer]](Part.Meta(trailer): Part[Trailer]) *>
          ZPure.log[St, Part[Trailer]](update(ru))
      case RewriteState(_, None, None) =>
        ZPure.fail(new RuntimeException("no trailer or root in rewrite stream"))
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
