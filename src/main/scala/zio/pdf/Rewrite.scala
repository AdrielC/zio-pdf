/*
 * Port of fs2.pdf.Rewrite to Scala 3 + ZIO.
 *
 * The legacy version was built around fs2.Pull; the ZIO equivalent
 * is ZChannel-based, which composes cleanly with the encoder side
 * (WritePdf.parts).
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.{Cause, Chunk, ZIO}
import zio.stream.{ZChannel, ZPipeline}

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

  /** Build the trailing Meta from the rewrite state, falling back
    * to a synthetic root-only trailer when the stream gave us only
    * a root reference. Fails if neither is present. */
  private def emitUpdate[S](
    state: RewriteState[S]
  ): ZIO[Any, Throwable, (List[Part[Trailer]], RewriteUpdate[S])] = state match {
    case RewriteState(s, Some(trailer), _) =>
      ZIO.succeed((List(Part.Meta(trailer): Part[Trailer]), RewriteUpdate(s, trailer)))
    case RewriteState(s, None, Some(root)) =>
      val trailer = Trailer(BigDecimal(-1), Prim.dict("Root" -> root), Some(root))
      ZIO.succeed((List(Part.Meta(trailer): Part[Trailer]), RewriteUpdate(s, trailer)))
    case RewriteState(_, None, None) =>
      ZIO.fail(new RuntimeException("no trailer or root in rewrite stream"))
  }

  /**
   * Stateful collect: each `A` produces a list of `Part[Trailer]`
   * outputs and an updated state. This is the simple, non-Pull
   * variant the legacy code called `simpleParts`/`simple`.
   */
  def simpleParts[S, A](initial: S)(
    collect: RewriteState[S] => A => (List[Part[Trailer]], RewriteState[S])
  )(
    update: RewriteUpdate[S] => Part[Trailer]
  ): ZPipeline[Any, Throwable, A, Part[Trailer]] = {
    def loop(
      st: RewriteState[S]
    ): ZChannel[Any, Throwable, Chunk[A], Any, Throwable, Chunk[Part[Trailer]], RewriteState[S]] =
      ZChannel.readWithCause[Any, Throwable, Chunk[A], Any, Throwable, Chunk[Part[Trailer]], RewriteState[S]](
        (chunk: Chunk[A]) => {
          var s   = st
          val out = Chunk.newBuilder[Part[Trailer]]
          chunk.foreach { a =>
            val (parts, next) = collect(s)(a)
            parts.foreach(out += _)
            s = next
          }
          ZChannel.write(out.result()) *> loop(s)
        },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => ZChannel.succeed(st)
      )

    val finalize: RewriteState[S] => ZChannel[Any, Throwable, Any, Any, Throwable, Chunk[Part[Trailer]], Unit] =
      st =>
        ZChannel.fromZIO(emitUpdate(st)).flatMap { case (extra, ru) =>
          val tail = update(ru)
          ZChannel.write(Chunk.fromIterable(extra) ++ Chunk.single(tail))
        }

    ZPipeline.fromChannel(loop(RewriteState.cons(initial)).flatMap(finalize))
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
