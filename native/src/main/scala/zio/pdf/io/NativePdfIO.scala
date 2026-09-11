package zio.pdf.io

import java.io.FileInputStream

import zio.*
import zio.stream.ZStream

/**
 * File ingest backends for Scala Native.
 *
 * Phase 1: javalib / POSIX `FileInputStream` reads via blocking ZIO.
 * Phase 2: [[IoUringFileBackend]] batches reads through liburing when linked.
 */
sealed trait NativeFileBackend:
  def readStream(path: String, chunkSize: Int): ZStream[Any, Throwable, Byte]

object NativeFileBackend:

  /** Default backend — portable file input stream. */
  val posix: NativeFileBackend = PosixFileBackend

  /** Linux io_uring backend (currently aliases [[posix]] until liburing is linked). */
  val ioUring: NativeFileBackend = IoUringFileBackend

private object PosixFileBackend extends NativeFileBackend:

  def readStream(path: String, chunkSize: Int): ZStream[Any, Throwable, Byte] =
    ZStream.unwrapScoped {
      ZIO.acquireRelease(ZIO.attemptBlocking(new FileInputStream(path))) { input =>
        ZIO.attemptBlocking(input.close()).ignore
      }.map { input =>
        ZStream.repeatZIOOption {
          ZIO.attemptBlocking {
            val buf = new Array[Byte](chunkSize)
            val n   = input.read(buf)
            if n < 0 then None
            else if n == 0 then Some(Chunk.empty[Byte])
            else Some(Chunk.fromArray(java.util.Arrays.copyOf(buf, n)))
          }.foldZIO(
            error => ZIO.fail(Some(error)),
            {
              case None        => ZIO.fail(None)
              case Some(chunk) => if chunk.isEmpty then ZIO.fail(None) else ZIO.succeed(chunk)
            }
          )
        }.flattenChunks
      }
    }

/**
 * io_uring read path — placeholder for batched async file ingest.
 *
 * Next step: add `@extern` bindings to liburing (`io_uring_queue_init`,
 * `io_uring_prep_read`, `io_uring_submit`, `io_uring_wait_cqe`) and link with
 * `-luring`. Until then this backend delegates to [[PosixFileBackend]].
 */
private object IoUringFileBackend extends NativeFileBackend:

  def readStream(path: String, chunkSize: Int): ZStream[Any, Throwable, Byte] =
    PosixFileBackend.readStream(path, chunkSize)
