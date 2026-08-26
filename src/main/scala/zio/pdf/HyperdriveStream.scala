/*
 * Incremental path streams. A scoped, pull-based FileChannel feeds the same
 * fused decoder used for caller-owned ZStreams. There is no producer
 * fiber, queue, or input-sized mmap copy between file I/O and downstream.
 */

package zio.pdf

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}

import zio.{Chunk, Ref, ZIO}
import zio.stream.ZStream

private[pdf] object HyperdriveStream {

  private enum PullState:
    case Reading(decoder: FusedDecoder.State)
    case Complete

  /**
   * Avoid reserving the default 10 MiB decoder buffer for a small file while
   * retaining the parser's 64 KiB lower bound and the caller's upper bound for
   * large files. The reader remains incremental if a file grows after this
   * initial stat: it simply performs another pull with the same buffer.
   */
  private[pdf] def adaptiveChunkSize(fileBytes: Long, requested: Int): Int =
    require(fileBytes >= 0L, "fileBytes must be non-negative")
    val normalized = FusedDecoder.normalizedChunkSize(requested)
    val fileBound = math.max(
      FusedDecoder.MinimumChunkSize.toLong,
      math.min(fileBytes, Int.MaxValue.toLong)
    ).toInt
    math.min(normalized, fileBound)

  private def decodedFromChannel(
    channel: FileChannel,
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config,
    chunkSize: Int
  ): ZStream[Any, Throwable, Decoded] = {
    val buffer = ByteBuffer.allocate(chunkSize)

    def transition(
      current: PullState,
      read: Int
    ): (Either[Throwable, Chunk[Decoded]], PullState) =
      current match {
        case PullState.Complete => (Right(Chunk.empty), PullState.Complete)
        case PullState.Reading(decoder) =>
          read match {
            case -1 =>
              FusedDecoder.run(decoder, FusedDecoder.finish(enableDiagnostics, config)) match {
                case Left(error)   => (Left(error), current)
                case Right(result) => (Right(result.emitted), PullState.Complete)
              }
            case 0  => (Right(Chunk.empty), current)
            case n  =>
              FusedDecoder.run(decoder, FusedDecoder.feedBytes(buffer.array(), 0, n, config)) match {
                case Left(error)   => (Left(error), current)
                case Right(result) => (Right(result.emitted), PullState.Reading(result.next))
              }
          }
      }

    ZStream.unwrap {
      Ref.make[PullState](PullState.Reading(FusedDecoder.initial)).map { state =>
        // A ZStream drives its source pulls linearly. FiberRef would fork this
        // cursor; Ref.Synchronized would add a mutex without a second writer.
        def pull: ZIO[Any, Option[Throwable], Chunk[Decoded]] =
          state.get.flatMap {
            case PullState.Complete => ZIO.fail(None)
            case current =>
              ZIO
                .attemptBlockingInterrupt {
                  buffer.clear()
                  channel.read(buffer)
                }
                .mapError(Some(_))
                .flatMap(read => state.modify(transition(_, read)))
                .flatMap(ZIO.fromEither(_).mapError(Some(_)))
          }

        ZStream.repeatZIOChunkOption(pull)
      }
    }
  }

  private def decodedFromPath(
    path: Path,
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config,
    chunkSize: Int
  ): ZStream[Any, Throwable, Decoded] =
    ZStream.unwrapScoped {
      for
        fileBytes <- ZIO.attemptBlocking(Files.size(path)).refineToOrDie[java.io.IOException]
        channel <- ZIO.acquireRelease(
                     ZIO
                       .attemptBlocking(FileChannel.open(path, StandardOpenOption.READ))
                       .refineToOrDie[java.io.IOException]
                   )(channel => ZIO.attemptBlocking(channel.close()).orDie)
      yield decodedFromChannel(channel, enableDiagnostics, config, adaptiveChunkSize(fileBytes, chunkSize))
    }

  private def classify(decoded: Decoded): Element =
    Elements.classifyOne(decoded) match {
      case Left(error)  => throw error
      case Right(value) => value
    }

  def decoded(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = FusedDecoder.DefaultChunkSize
  ): ZStream[Any, Throwable, Decoded] = {
    val effectiveBatchSize = FusedDecoder.normalizedChunkSize(batchSize)
    decodedFromPath(path, enableDiagnostics, config, effectiveBatchSize)
  }

  def elements(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = FusedDecoder.DefaultChunkSize
  ): ZStream[Any, Throwable, Element] = {
    val effectiveBatchSize = FusedDecoder.normalizedChunkSize(batchSize)
    decodedFromPath(path, enableDiagnostics, config, effectiveBatchSize)
      .mapChunksZIO(chunk => ZIO.attempt(chunk.map(classify)))
  }
}
