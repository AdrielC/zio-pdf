package io.github.adrielc.scan.fs2

import cats.{Contravariant, Functor}
import cats.arrow.Strong
import cats.effect.Sync
import fs2.{Chunk, Pipe, Pull, Stream}
import io.github.adrielc.scan.Scan
import scala.collection.mutable.ArrayBuffer

/** Optional FS2 integration. Import ScanFs2.* for pipe syntax and instances. */
object ScanFs2:
  final class ScanException(val issue: Scan.Error) extends RuntimeException(issue.toString)

  extension [I, O](scan: Scan[I, O])
    /** One run-local interpreter per stream evaluation. Sync suspends mutable
      * state allocation and brackets cleanup on EOF, failure and cancellation.
      * Batches process at most limits.pending inputs and retain fewer than
      * 2 * limits.pending output references. Each input/EOF may emit at most
      * limits.pending outputs. Elementwise mode yields after every input.
      */
    def toPipe[F[_]](using F: Sync[F]): Pipe[F, I, O] = toPipeWith[F](Scan.Limits.default)

    def toPipeWith[F[_]](limits: Scan.Limits)(using F: Sync[F]): Pipe[F, I, O] =
      pipe(scan, limits, elementwise = false)

    def toPipeElementwise[F[_]](using F: Sync[F]): Pipe[F, I, O] =
      toPipeElementwiseWith[F](Scan.Limits.default)

    def toPipeElementwiseWith[F[_]](limits: Scan.Limits)(using F: Sync[F]): Pipe[F, I, O] =
      pipe(scan, limits, elementwise = true)

  private def pipe[F[_], I, O](scan: Scan[I, O], limits: Scan.Limits, elementwise: Boolean)(using F: Sync[F]): Pipe[F, I, O] =
      val plan = scan.compile(limits)
      input => plan match
        case Left(issue) => Stream.raiseError[F](new ScanException(issue))
        case Right(compiled) =>
          final class Session:
            private val buffer = ArrayBuffer.empty[O]
            private var overflow = false
            private var emitted = 0
            private val run: Scan.Runner[I, scan.State] = compiled.open { output =>
              if emitted < limits.pending then { emitted += 1; buffer += output; () }
              else { overflow = true; run.close() }
            }
            private def collect(result: Either[Scan.RunIssue, Unit]): F[Chunk[O]] =
              val checked = if overflow then Left(Scan.RunIssue.PendingLimit(limits.pending)) else result
              checked match
                case Left(issue) =>
                  buffer.clear()
                  F.raiseError(new ScanException(issue))
                case Right(_) =>
                  val batch = Chunk.from(buffer.toVector)
                  buffer.clear()
                  F.pure(batch)
            def push(input: Chunk[I], from: Int): F[(Int, Chunk[O])] = F.defer {
              var at = from
              val count = if elementwise then 1 else limits.pending
              val result = run.feed { emit =>
                while at < input.size && at - from < count && buffer.length < limits.pending && !run.isClosed do
                  emitted = 0
                  emit(input(at))
                  at += 1
              }
              F.map(collect(result))(batch => (at, batch))
            }
            def finish: F[Chunk[O]] = F.defer { emitted = 0; collect(run.finish().map(_ => ())) }
            def close: F[Unit] = F.delay { run.close(); buffer.clear() }
          def remaining(input: Chunk[I], from: Int, rest: Stream[F, I], session: Session): Pull[F, O, Unit] =
            Pull.eval(session.push(input, from)).flatMap { (next, batch) =>
              Pull.output(batch) >> (if next < input.size then remaining(input, next, rest, session) else loop(rest, session))
            }
          def loop(source: Stream[F, I], session: Session): Pull[F, O, Unit] =
            source.pull.uncons.flatMap {
              case Some((input, rest)) => remaining(input, 0, rest, session)
              case None => Pull.eval(session.finish).flatMap(batch => Pull.output(batch))
            }
          Stream.bracket(F.delay(new Session))(_.close).flatMap(session => loop(input, session).stream)

  given functor[S, I]: Functor[[O] =>> Scan.Aux[S, I, O]] with
    def map[A, B](scan: Scan.Aux[S, I, A])(f: A => B): Scan.Aux[S, I, B] = scan.map(f)

  given contravariant[S, O]: Contravariant[[I] =>> Scan.Aux[S, I, O]] with
    def contramap[A, B](scan: Scan.Aux[S, A, O])(f: B => A): Scan.Aux[S, B, O] = scan.contramap(f)

  given strong[S]: Strong[[I, O] =>> Scan.Aux[S, I, O]] with
    def first[A, B, C](scan: Scan.Aux[S, A, B]): Scan.Aux[S, (A, C), (B, C)] = scan.first[C]
    def second[A, B, C](scan: Scan.Aux[S, A, B]): Scan.Aux[S, (C, A), (C, B)] = scan.second[C]
    def dimap[A, B, C, D](scan: Scan.Aux[S, A, B])(f: C => A)(g: B => D): Scan.Aux[S, C, D] = scan.dimap(f)(g)
