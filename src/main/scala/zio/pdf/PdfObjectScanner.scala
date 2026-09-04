package zio.pdf

import scala.util.control.NonFatal

import zio.*
import zio.blocks.chunk.{Chunk as BlocksChunk}
import zio.blocks.streams.{Sink as BlocksSink, Stream as BlocksStream}
import zio.blocks.streams.io.Reader
import zio.stream.{ZPipeline, ZStream}

/** Incremental PDF object-boundary scanner with bounded parser carry.
  *
  * It trusts declared stream lengths, consumes complete `endobj` trailers,
  * and skips raw stream payloads without copying or decoding them.
  */
object PdfObjectScanner {

  private val MaxStepBytes = 64 * 1024
  private val MaxPullBytes = 1024 * 1024

  private def pullBuffer(config: Config): Array[Byte] =
    new Array[Byte](math.min(MaxPullBytes, math.max(MaxStepBytes, config.maxCarryBytes)))

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
    bytes match {
      case Chunk.ByteArray(arr, off, len) =>
        stepBytes(config, cursor, arr, off, len)
      case _ =>
        val arr = bytes.toArray
        stepBytes(config, cursor, arr, 0, arr.length)
    }

  /**
   * Zero-copy window step: decode from an owned `Array[Byte]` via
   * [[StreamingDecode.stepChunkBytes]].
   */
  def stepBytes(
    config: Config,
    cursor: Cursor,
    buf: Array[Byte],
    offset: Int,
    length: Int
  ): Either[Error, (Cursor, Chunk[Boundary])] =
    try {
      var parser     = cursor.parser
      var pos        = offset
      val end        = offset + length
      val boundaries = Chunk.newBuilder[Boundary]

      while pos < end do
        val retained = StreamingDecode.structuralCarryBytes(parser)
        val headroom = math.max(1L, config.maxCarryBytes.toLong - retained)
        val stepSize = math.min(headroom, (end - pos).toLong).toInt
        val (events, next) = StreamingDecode.stepChunkBytes(config.decoder, parser, buf, pos, stepSize)
        events.foreach {
          case StreamingDecoded.ObjectEnd(index, nextByteOffset) =>
            boundaries += Boundary(index, nextByteOffset)
          case _ => ()
        }
        parser = next
        pos += stepSize

      Right((new Cursor(parser), boundaries.result()))
    } catch {
      case StreamingDecode.CarryLimitExceeded(maxBytes, observedBytes) =>
        Left(Error.CarryLimit(maxBytes, observedBytes))
      case StreamingDecode.UnresolvedIndirectStreamLength(index, reference) =>
        Left(Error.IndirectLength(index, reference))
      case NonFatal(error) =>
        Left(asError(error))
    }

  /** In-memory scan — no Reader copy. Same decode as [[step]] plus [[finish]]. */
  def scan(bytes: Chunk[Byte], config: Config): Either[Error, Chunk[Boundary]] =
    step(config, initial, bytes).flatMap { (cursor, found) =>
      finish(cursor).map(_ => found)
    }

  def scan(bytes: Chunk[Byte]): Either[Error, Chunk[Boundary]] =
    scan(bytes, Config.default)

  def scan(bytes: Array[Byte], config: Config): Either[Error, Chunk[Boundary]] =
    stepBytes(config, initial, bytes, 0, bytes.length).flatMap { (cursor, found) =>
      finish(cursor).map(_ => found)
    }

  def scan(bytes: Array[Byte]): Either[Error, Chunk[Boundary]] =
    scan(bytes, Config.default)

  /**
   * Tight pull on a Blocks [[Reader]]: `readBytes` into one reused buffer,
   * decode with [[stepBytes]], no ZIO per object or per byte.
   */
  def scan(reader: Reader[Byte], config: Config = Config.default): Either[Error, Chunk[Boundary]] =
    try {
      val buf    = pullBuffer(config)
      var cursor = initial
      val out    = Chunk.newBuilder[Boundary]
      var n      = reader.readBytes(buf, 0, buf.length)
      var failed = Option.empty[Error]
      while n >= 0 && failed.isEmpty do
        if n > 0 then
          stepBytes(config, cursor, buf, 0, n) match {
            case Left(e) =>
              failed = Some(e)
            case Right((next, found)) =>
              cursor = next
              if found.nonEmpty then out ++= found
          }
        if failed.isEmpty then n = reader.readBytes(buf, 0, buf.length)
      failed match {
        case Some(e) => Left(e)
        case None    => finish(cursor).map(_ => out.result())
      }
    } catch {
      case NonFatal(error) => Left(asError(error))
    }

  /**
   * Drain a Blocks byte [[zio.blocks.streams.Stream]] through [[sink]] —
   * the scan stays inside the Blocks `Reader` / `Sink`, not a ZStream loop.
   */
  def scan(source: BlocksStream[Nothing, Byte], config: Config): Either[Error, Chunk[Boundary]] =
    source.run(sink(config)) match {
      case Right(result) => result
      case Left(_) =>
        Left(Error.Malformed("blocks stream failed", new RuntimeException("blocks stream failed")))
    }

  def scan(source: BlocksStream[Nothing, Byte]): Either[Error, Chunk[Boundary]] =
    scan(source, Config.default)

  def scanZIO(reader: Reader[Byte], config: Config = Config.default): IO[Error, Chunk[Boundary]] =
    ZIO.succeed(scan(reader, config)).flatMap(ZIO.fromEither)

  /**
   * Blocks sink: `readBytes` + [[stepBytes]] on the calling thread.
   * Use `stream.run(sink)` — do not lift each byte into ZIO first.
   */
  def sink(config: Config = Config.default): BlocksSink[Nothing, Byte, Either[Error, Chunk[Boundary]]] =
    BlocksSink.create { (reader: Reader[Byte]) =>
      scan(reader, config)
    }

  /**
   * One ZStream element per pulled window (`Chunk[Boundary]`), not one
   * object per step. Flatten with `.flattenChunks` only at the rim.
   */
  def streamWindows(
    acquire: => Reader[Byte],
    config: Config = Config.default
  ): ZStream[Any, Error, Chunk[Boundary]] =
    ZStream.unwrapScoped {
      ZIO.acquireRelease(ZIO.attempt(acquire).mapError(asError))(reader => ZIO.succeed(reader.close())).map { reader =>
        ZStream
          .unfoldZIO(WindowPull(initial, pullBuffer(config))) { pull =>
            ZIO.attempt(reader.readBytes(pull.buf, 0, pull.buf.length)).mapError(asError).flatMap { n =>
              if n < 0 then
                ZIO.fromEither(finish(pull.cursor)).as(None)
              else if n == 0 then
                ZIO.succeed(Some(Chunk.empty[Boundary] -> pull))
              else
                ZIO.fromEither(stepBytes(config, pull.cursor, pull.buf, 0, n)).map { (next, found) =>
                  Some(found -> pull.copy(cursor = next))
                }
            }
          }
          .filter(_.nonEmpty)
      }
    }

  def streamWindows[R](
    windows: ZStream[R, Error, Chunk[Byte]],
    config: Config
  ): ZStream[R, Error, Chunk[Boundary]] =
    ZStream.unwrapScoped {
      Ref.make(initial).map { cursorRef =>
        windows
          .mapZIO { window =>
            cursorRef.get.flatMap { cursor =>
              ZIO.fromEither(step(config, cursor, window)).flatMap { (next, bounds) =>
                cursorRef.set(next).as(bounds)
              }
            }
          }
          .filter(_.nonEmpty)
          .concat(ZStream.fromZIO(cursorRef.get.flatMap(cursor => ZIO.fromEither(finish(cursor)))).drain)
      }
    }

  def stream(reader: => Reader[Byte], config: Config = Config.default): ZStream[Any, Error, Boundary] =
    streamWindows(reader, config).flattenChunks

  /** Scan byte windows as they arrive. Does not call [[finish]]; use [[stream]] for a complete source. */
  def pipeline(config: Config = Config.default): ZPipeline[Any, Error, Chunk[Byte], Boundary] =
    pipelineWindows(config).flattenChunks

  def pipelineWindows(config: Config = Config.default): ZPipeline[Any, Error, Chunk[Byte], Chunk[Boundary]] =
    ZPipeline.unwrap {
      Ref.make(initial).map { cursorRef =>
        ZPipeline.mapChunksZIO[Any, Error, Chunk[Byte], Chunk[Boundary]] { windows =>
          cursorRef.get.flatMap { start =>
            val folded =
              windows.foldLeft[Either[Error, (Cursor, Chunk[Chunk[Boundary]])]](Right((start, Chunk.empty))) {
                case (Left(e), _) => Left(e)
                case (Right((cursor, acc)), window) =>
                  step(config, cursor, window).map { (next, bounds) =>
                    (next, if bounds.isEmpty then acc else acc :+ bounds)
                  }
              }
            ZIO.fromEither(folded).flatMap { (next, emitted) =>
              cursorRef.set(next).as(emitted)
            }
          }
        }
      }
    }

  def stream[R](
    windows: ZStream[R, Error, Chunk[Byte]],
    config: Config
  ): ZStream[R, Error, Boundary] =
    streamWindows(windows, config).flattenChunks

  def stream[R](windows: ZStream[R, Error, Chunk[Byte]]): ZStream[R, Error, Boundary] =
    stream(windows, Config.default)

  def stream(
    source: BlocksStream[Nothing, BlocksChunk[Byte]],
    config: Config
  ): ZStream[Any, Error, Boundary] =
    streamWindows(
      BlocksLift.fromStream(source, null).mapError(asError).map(BlocksLift.toZioChunk),
      config
    ).flattenChunks

  def stream(source: BlocksStream[Nothing, BlocksChunk[Byte]]): ZStream[Any, Error, Boundary] =
    stream(source, Config.default)

  private def asError(error: Throwable): Error =
    error match {
      case err: Error => err
      case other =>
        val detail = Option(other.getMessage).filter(_.nonEmpty).getOrElse(other.getClass.getSimpleName)
        Error.Malformed(s"Malformed or unsupported PDF structure: $detail", other)
    }

  private final case class WindowPull(cursor: Cursor, buf: Array[Byte])
}
