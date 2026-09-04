package zio.pdf

import scala.util.control.NonFatal

import zio.blocks.chunk.{Chunk as BlocksChunk}
import zio.blocks.streams.{Stream as BlocksStream}
import zio.blocks.streams.io.Reader
import zio.stream.{ZPipeline, ZStream}
import zio.{Chunk, ZIO}

/** Incremental PDF object-boundary scanner with bounded parser carry.
  *
  * It trusts declared stream lengths, consumes complete `endobj` trailers,
  * and skips raw stream payloads without copying or decoding them.
  */
object PdfObjectScanner {

  private val MaxStepBytes = 64 * 1024

  final case class Config(maxCarryBytes: Int = 1024 * 1024) {
    require(maxCarryBytes > 0, "maxCarryBytes must be positive")

    private[pdf] val decoder: StreamingDecode.Config =
      StreamingDecode.Config(
        inlineMaxBytes = 0L,
        emitObjectEnds = true,
        maxCarryBytes = Some(maxCarryBytes),
        emitContentEvents = false
      )
  }

  object Config {
    val default: Config = Config()
  }

  sealed trait Error extends Exception {
    def message: String
    override final def getMessage: String = message
  }

  object Error {
    final case class CarryLimit(maxBytes: Int, observedBytes: Long) extends Error {
      val message: String =
        s"PDF structural carry exceeded $maxBytes bytes (observed $observedBytes)"
    }

    final case class Malformed(message: String, cause0: Throwable) extends Error {
      override def getCause: Throwable = cause0
    }

    final case class IndirectLength(index: Obj.Index, reference: Prim.Ref) extends Error {
      val message: String =
        s"stream object ${index.number} uses indirect /Length ${reference.number} ${reference.generation} R; use an xref-resolving reader"
    }

    final case class UnexpectedEnd(context: String, remainingBytes: Long) extends Error {
      val message: String = s"unexpected end of PDF input while $context ($remainingBytes bytes remain)"
    }
  }

  final case class Boundary(index: Obj.Index, nextByteOffset: Long) {
    require(nextByteOffset >= 0L, "nextByteOffset must be non-negative")
  }

  final class Cursor private[pdf] (private[pdf] val parser: StreamingDecode.FinalState)

  def initial: Cursor = new Cursor(StreamingDecode.initialFinalState)

  def bytesSeen(cursor: Cursor): Long = cursor.parser.bytesSeen

  /** Validate that the source ended at a complete top-level boundary. */
  def finish(cursor: Cursor): Either[Error, Unit] =
    StreamingDecode.validateFinalState(cursor.parser).left.map { error =>
      Error.UnexpectedEnd(error.context, error.remainingBytes)
    }

  def step(
    config: Config,
    cursor: Cursor,
    bytes: Chunk[Byte]
  ): Either[Error, (Cursor, Chunk[Boundary])] =
    try {
      var parser     = cursor.parser
      var offset     = 0
      val boundaries = Chunk.newBuilder[Boundary]

      while (offset < bytes.length) {
        val retained = StreamingDecode.structuralCarryBytes(parser)
        val headroom = math.max(1L, config.maxCarryBytes.toLong - retained)
        val stepSize = math.min(math.min(MaxStepBytes.toLong, headroom), bytes.length.toLong - offset.toLong).toInt
        val nextEnd  = offset + stepSize
        val (events, next) = StreamingDecode.stepChunk(config.decoder, parser, bytes.slice(offset, nextEnd))
        events.foreach {
          case StreamingDecoded.ObjectEnd(index, nextByteOffset) =>
            boundaries += Boundary(index, nextByteOffset)
          case _                                                  => ()
        }
        parser = next
        offset = nextEnd
      }

      Right((new Cursor(parser), boundaries.result()))
    } catch {
      case StreamingDecode.CarryLimitExceeded(maxBytes, observedBytes) =>
        Left(Error.CarryLimit(maxBytes, observedBytes))
      case StreamingDecode.UnresolvedIndirectStreamLength(index, reference) =>
        Left(Error.IndirectLength(index, reference))
      case NonFatal(error) =>
        val detail = Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)
        Left(Error.Malformed(s"Malformed or unsupported PDF structure: $detail", error))
    }

  /** Scan byte windows as they arrive. Does not call [[finish]]; use [[stream]] for a complete source. */
  def pipeline(config: Config = Config.default): ZPipeline[Any, Error, Chunk[Byte], Boundary] =
    ZPipeline.unwrap {
      zio.Ref.make(initial).map { cursorRef =>
        ZPipeline.mapChunksZIO[Any, Error, Chunk[Byte], Boundary] { windows =>
          ZIO.foldLeft(windows)(Chunk.empty[Boundary]) { (acc, window) =>
            cursorRef.get.flatMap { cursor =>
              ZIO.fromEither(step(config, cursor, window)).flatMap { (next, bounds) =>
                cursorRef.set(next).as(acc ++ bounds)
              }
            }
          }
        }
      }
    }

  def stream[R](
    windows: ZStream[R, Error, Chunk[Byte]],
    config: Config = Config.default
  ): ZStream[R, Error, Boundary] =
    ZStream.unwrapScoped {
      zio.Ref.make(initial).map { cursorRef =>
        windows
          .mapZIO { window =>
            cursorRef.get.flatMap { cursor =>
              ZIO.fromEither(step(config, cursor, window)).flatMap { (next, bounds) =>
                cursorRef.set(next).as(bounds)
              }
            }
          }
          .flattenChunks
          .concat(ZStream.fromZIO(cursorRef.get.flatMap(cursor => ZIO.fromEither(finish(cursor)))).drain)
      }
    }

  def stream(
    reader: => Reader[BlocksChunk[Byte]],
    config: Config
  ): ZStream[Any, Error, Boundary] =
    stream(
      BlocksLift.fromReader(reader, null).mapError(asError).map(BlocksLift.toZioChunk),
      config
    )

  def stream(reader: => Reader[BlocksChunk[Byte]]): ZStream[Any, Error, Boundary] =
    stream(reader, Config.default)

  def stream(
    source: BlocksStream[Nothing, BlocksChunk[Byte]],
    config: Config
  ): ZStream[Any, Error, Boundary] =
    stream(
      BlocksLift.fromStream(source, null).mapError(asError).map(BlocksLift.toZioChunk),
      config
    )

  def stream(source: BlocksStream[Nothing, BlocksChunk[Byte]]): ZStream[Any, Error, Boundary] =
    stream(source, Config.default)

  private def asError(error: Throwable): Error =
    error match {
      case err: Error => err
      case other =>
        val detail = Option(other.getMessage).filter(_.nonEmpty).getOrElse(other.getClass.getSimpleName)
        Error.Malformed(s"Malformed or unsupported PDF structure: $detail", other)
    }
}
