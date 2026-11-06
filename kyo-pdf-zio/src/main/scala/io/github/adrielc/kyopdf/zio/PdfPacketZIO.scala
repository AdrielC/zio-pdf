package io.github.adrielc.kyopdf.zio

import io.github.adrielc.kyopdf.{PdfError, PdfPacket, PdfPacketSource, PdfRead}
import java.nio.file.{Files, Path}
import kyo.Async
import _root_.zio.{Chunk, Ref, Scope, ZIO}
import _root_.zio.stream.{ZSink, ZStream}

/** Each input is consumed once into a scoped seekable file, then indexed without
  * retaining its stream payloads. Live output is provisional until EOF. Staged
  * output exposes nothing until the complete packet has been produced.
  */
object PdfPacketZIO:
  final case class Part[-R, +E](title: String, bytes: ZStream[R, E, Byte])

  def stitch[R, E](parts: ZStream[R, E, Part[R, E]], settings: PdfPacket.Settings = PdfPacket.Settings(),
      bounds: PdfPacket.Bounds = PdfPacket.Bounds(), delivery: PdfPacket.Delivery = PdfPacket.Delivery.Staged,
      stagingDirectory: Option[Path] = None): ZStream[R, E | PdfError, Byte] = ZStream.unwrapScoped {
    for
      session <- ZIO.acquireRelease(PdfZIO.run(Async.defer(PdfPacket.open(settings, bounds))))(session => ZIO.succeed(session.close()))
      used <- Ref.make(0L)
    yield
      val quota = new Quota(bounds.spoolBytes, used)
      def output(pull: session.Pull): ZStream[Any, PdfError, Byte] =
        ZStream.repeatZIOOption(ZIO.blocking(PdfZIO.run(Async.defer(pull.read()))).mapError(Some(_)).flatMap {
          case Some(block) => ZIO.succeed(Chunk.fromArray(block.array).take(block.size))
          case None => ZIO.fail(None)
        }).flattenChunks
      val live = parts.flatMap { part =>
        ZStream.unwrapScoped {
          for
            _ <- PdfZIO.run(Async.defer(session.admit(part.title)))
            spool <- temporary(stagingDirectory, quota)
            _ <- headerChecked(spool.meter(part.bytes, bounds.sourceBytes)).run(write(spool.path))
            source <- ZIO.acquireRelease(ZIO.blocking(PdfZIO.run(Async.defer(PdfPacketSource.open(spool.path, bounds)))))(
              source => ZIO.attemptBlockingIO(source.close()).orDie)
            pull <- PdfZIO.run(Async.defer(session.append(part.title, source)))
          yield output(pull)
        }
      } ++ ZStream.unwrap(PdfZIO.run(Async.defer(session.finish())).map(output))
      delivery match
        case PdfPacket.Delivery.Live => live
        case PdfPacket.Delivery.Staged => ZStream.unwrapScoped {
          for
            spool <- temporary(stagingDirectory, quota)
            _ <- spool.meter(live, bounds.outputBytes).run(write(spool.path))
          yield ZStream.fromPath(spool.path, PdfPacket.ChunkBytes).mapError(_ => PdfError.InvalidPdf("Packet spool read failed"))
        }
  }

  private def headerChecked[R, E](bytes: ZStream[R, E, Byte]): ZStream[R, E | PdfError, Byte] = ZStream.unwrapScoped {
    ZIO.acquireRelease(ZIO.succeed(new PdfRead.HeaderReader))(reader => ZIO.succeed(reader.close())).map { reader =>
      bytes.mapChunksZIO(chunk => ZIO.fromEither(reader.offer(chunk.size)(chunk.apply)).as(chunk)) ++
        ZStream.fromZIO(ZIO.fromEither(reader.finish())).drain
    }
  }

  private final class Quota(limit: Long, used: Ref[Long]):
    def reserve(bytes: Int): ZIO[Any, PdfError, Unit] = used.modify { current =>
      if bytes.toLong > limit - current then Left(PdfError.RetentionExceeded(s"Packet spool quota exceeds $limit bytes")) -> current
      else Right(()) -> (current + bytes)
    }.flatMap(ZIO.fromEither(_))
    def release(bytes: Long): ZIO[Any, Nothing, Unit] = used.update(_ - bytes)

  private final class Spool(val path: Path, quota: Quota, held: Ref[Long]):
    def meter[R, E](bytes: ZStream[R, E, Byte], limit: Long): ZStream[R, E | PdfError, Byte] =
      bytes.mapChunksZIO { chunk =>
        for
          current <- held.get
          _ <- ZIO.fail(PdfError.TooLarge(limit, if chunk.size.toLong > Long.MaxValue - current then Long.MaxValue else current + chunk.size))
            .when(chunk.size.toLong > limit - current)
          // Reserve and register atomically with respect to interruption, before
          // the sink receives the chunk. Cleanup releases every reservation.
          _ <- ZIO.uninterruptible(quota.reserve(chunk.size) *> held.update(_ + chunk.size))
        yield chunk
      }
    def release: ZIO[Any, Nothing, Unit] = held.get.flatMap(quota.release)

  private def io[A](body: => A): ZIO[Any, PdfError, A] =
    ZIO.attemptBlockingIO(body).mapError(_ => PdfError.InvalidPdf("Packet spool I/O failed"))
  private def temporary(directory: Option[Path], quota: Quota): ZIO[Scope, PdfError, Spool] =
    for
      held <- Ref.make(0L)
      spool <- ZIO.acquireRelease(io(new Spool(directory.fold(Files.createTempFile("kyo-packet-", ".pdf"))(
        dir => Files.createTempFile(dir, "kyo-packet-", ".pdf")), quota, held)))(spool =>
          ZIO.attemptBlockingIO(Files.deleteIfExists(spool.path)).orDie.ensuring(spool.release))
    yield spool
  private def write(path: Path): ZSink[Any, PdfError, Byte, Byte, Long] =
    ZSink.fromPath(path).mapError(_ => PdfError.InvalidPdf("Packet spool write failed"))
