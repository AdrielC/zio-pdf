/*
 * Lift ZIO Blocks pull primitives into ZIO streams.
 *
 * The hot loop stays on `Reader` / `Writer` / `Stream` (synchronous pull).
 * `ZChannel` / `ZStream` sit at the rim so upload, Scala.js, and embeddings
 * keep their existing effect surface.
 *
 * Resource lifetime uses Blocks `Scope` (`open` / `close`). Settings travel
 * in a Blocks `Context` and can be loaded with Blocks `Config`. MPSC
 * mailboxes use the same API on JVM (lock-free) and Scala.js (sequential).
 *
 * JVM-only Blocks APIs (virtual threads, NIO sinks, ProducerStreams) are
 * not used — this module is part of the published JVM and Scala.js jars.
 */

package zio.pdf

import zio.blocks.chunk.{Chunk as BlocksChunk}
import zio.blocks.config.{Config, ConfigSource}
import zio.blocks.context.Context
import zio.blocks.ringbuffer.MpscRingBuffer
import zio.blocks.schema.Schema
import zio.blocks.scope.{Scope as BlocksScope, Unscoped}
import zio.blocks.streams.{Stream as BlocksStream}
import zio.blocks.streams.io.{Reader, Writer}
import zio.stream.{ZChannel, ZSink, ZStream}
import zio.{Chunk, UIO, ZIO}

object BlocksLift {

  final case class Options(mailboxCapacity: Int = 8)

  object Options {
    val default: Options = Options()
    given Schema[Options] = Schema.derived[Options]
    given Unscoped[Options] = Unscoped.derived

    def context(options: Options = default): Context[Options] =
      Context(options)

    def fromContext(ctx: Context[Options]): Options =
      ctx.get[Options]

    def fromMap(entries: Map[String, String]): Either[String, Options] =
      Config.load[Options](ConfigSource.fromMap(entries, "blocks-lift")).left.map { errors =>
        errors.map(_.toString).mkString("; ")
      }
  }

  def toBlocksChunk[A](chunk: Chunk[A]): BlocksChunk[A] =
    BlocksChunk.fromIterable(chunk)

  def toZioChunk[A](chunk: BlocksChunk[A]): Chunk[A] =
    Chunk.fromIterator(chunk.iterator)

  /**
   * Pull a Blocks `Reader` on the current fiber. The reader is closed when
   * the stream exits. `sentinel` must not appear as a live element.
   */
  def fromReader[A](acquire: => Reader[A], sentinel: A): ZStream[Any, Throwable, A] =
    ZStream.unwrapScoped {
      ZIO.acquireRelease(ZIO.attempt(acquire))(reader => ZIO.succeed(reader.close())).map { reader =>
        pullReader(reader, sentinel)
      }
    }

  def channelFromReader[A](
    acquire: => Reader[A],
    sentinel: A
  ): ZChannel[Any, Any, Any, Any, Throwable, Chunk[A], Any] =
    fromReader(acquire, sentinel).toChannel

  /**
   * Compile a Blocks `Stream` to a `Reader` under an unowned Blocks
   * [[zio.blocks.scope.Scope.OpenScope]], then pull it from ZIO. The
   * handle is closed when the ZIO stream exits.
   */
  def fromStream[E, A: Unscoped](stream: BlocksStream[E, A], sentinel: A): ZStream[Any, Throwable, A] =
    ZStream.unwrapScoped {
      ZIO
        .acquireRelease(ZIO.attempt(openStream(stream)))(opened => ZIO.succeed(opened.close()))
        .map { opened =>
          pullOpened(opened, sentinel)
        }
    }

  /** Push ZStream elements into a Blocks `Writer`. The writer is closed on exit. */
  def toWriter[A](writer: Writer[A]): ZSink[Any, Throwable, A, Nothing, Unit] =
    ZSink.unwrapScoped {
      ZIO.acquireRelease(ZIO.succeed(writer))(w => ZIO.succeed(w.close())).map { owned =>
        ZSink.foreach[Any, Throwable, A] { value =>
          ZIO.attempt {
            if !owned.writeable() || !owned.write(value) then
              throw new IllegalStateException("blocks Writer rejected an element")
          }
        }
      }
    }

  /**
   * Bounded MPSC mailbox. Same type on JVM and Scala.js; JS uses the
   * sequential ring-buffer implementation. Capacity must be a power of two.
   *
   * Failed offers yield the fiber instead of spinning, so a full buffer
   * cannot livelock the Scala.js event loop.
   */
  final class MpscMailbox[A <: AnyRef] private (buffer: MpscRingBuffer[A]) {
    def offer(value: A): Boolean = buffer.offer(value)

    def poll(): A | Null = buffer.take()

    def offerZIO(value: A): UIO[Unit] =
      ZIO.suspendSucceed {
        if offer(value) then ZIO.unit
        else ZIO.yieldNow *> offerZIO(value)
      }

    def pollZIO: UIO[Option[A]] =
      ZIO.succeed(Option(poll()))
  }

  object MpscMailbox {
    def apply[A <: AnyRef](capacity: Int): MpscMailbox[A] =
      new MpscMailbox(MpscRingBuffer.apply[A](capacity))

    def apply[A <: AnyRef](options: Options): MpscMailbox[A] =
      apply(options.mailboxCapacity)

    def apply[A <: AnyRef](ctx: Context[Options]): MpscMailbox[A] =
      apply(Options.fromContext(ctx))
  }

  private def pullReader[A](reader: Reader[A], sentinel: A): ZStream[Any, Throwable, A] =
    ZStream.repeatZIOChunkOption {
      ZIO
        .attempt {
          val next = reader.read(sentinel)
          if next == sentinel then Chunk.empty[A]
          else Chunk.single(next)
        }
        .foldZIO(
          error => ZIO.fail(Some(error)),
          chunk => if chunk.isEmpty then ZIO.fail(None) else ZIO.succeed(chunk)
        )
    }

  private trait Opened[A] {
    def read(sentinel: A): A
    def close(): Unit
  }

  private def pullOpened[A](opened: Opened[A], sentinel: A): ZStream[Any, Throwable, A] =
    ZStream.repeatZIOChunkOption {
      ZIO
        .attempt {
          val next = opened.read(sentinel)
          if next == sentinel then Chunk.empty[A]
          else Chunk.single(next)
        }
        .foldZIO(
          error => ZIO.fail(Some(error)),
          chunk => if chunk.isEmpty then ZIO.fail(None) else ZIO.succeed(chunk)
        )
    }

  private def openStream[E, A: Unscoped](stream: BlocksStream[E, A]): Opened[A] = {
    val handle = BlocksScope.global.open()
    val reader = stream.start(using handle.scope)
    new Opened[A] {
      def read(sentinel: A): A =
        handle.scope.$(reader)(_.read(sentinel))
      def close(): Unit = {
        handle.scope.$(reader)(_.close())
        val _ = handle.close()
      }
    }
  }
}
