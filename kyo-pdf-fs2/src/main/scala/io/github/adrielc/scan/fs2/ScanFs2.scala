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
      * Both the interpreter queue and the output batch obey limits.pending.
      * Only one input element is transformed before yielding to the consumer.
      */
    def toPipe[F[_]](using F: Sync[F]): Pipe[F, I, O] = toPipeWith[F](Scan.Limits.default)

    def toPipeWith[F[_]](limits: Scan.Limits)(using F: Sync[F]): Pipe[F, I, O] =
      val plan = scan.compile(limits)
      input => plan match
        case Left(issue) => Stream.raiseError[F](new ScanException(issue))
        case Right(compiled) =>
          final class Session:
            private val buffer = ArrayBuffer.empty[O]
            private var overflow = false
            private val run: Scan.Runner[I, scan.State] = compiled.open { output =>
              if buffer.length < limits.pending then { buffer += output; () }
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
            def push(value: I): F[Chunk[O]] = F.defer(collect(run.push(value)))
            def finish: F[Chunk[O]] = F.defer(collect(run.finish().map(_ => ())))
            def close: F[Unit] = F.delay { run.close(); buffer.clear() }
          def loop(source: Stream[F, I], session: Session): Pull[F, O, Unit] =
            source.pull.uncons1.flatMap {
              case Some((value, rest)) =>
                Pull.eval(session.push(value)).flatMap(batch => Pull.output(batch)) >> loop(rest, session)
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
