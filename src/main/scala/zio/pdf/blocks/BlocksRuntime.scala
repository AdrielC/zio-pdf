package zio.pdf.blocks

import zio.*
import zio.blocks.streams.{Sink, Stream}

object BlocksRuntime {

  def runZIO[E, A, Z](stream: Stream[E, A], sink: Sink[E, A, Z]): ZIO[Any, E | Throwable, Z] =
    ZIO
      .attemptBlocking(stream.run(sink))
      .flatMap {
        case Right(value) => ZIO.succeed(value)
        case Left(error)  => ZIO.fail(error)
      }

  def eitherToZIO[E <: Throwable, A](value: Either[E, A]): ZIO[Any, E, A] =
    ZIO.fromEither(value)
}
