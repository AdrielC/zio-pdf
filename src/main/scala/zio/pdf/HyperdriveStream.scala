/*
 * Hyperdrive decode as [[ZStream]] — mmap fused parse into a bounded queue.
 *
 * The fuse loop runs on a blocking fiber and [[ArrayBlockingQueue.put]]s each
 * event. When the queue is full, decode stalls (backpressure). Downstream
 * never sees a materialised timeline [[Chunk]] unless it explicitly
 * `runCollect`s the stream.
 *
 * For fire-and-forget constant-memory sinks, prefer
 * [[zio.pdf.io.PdfIO.warpStreaming]] (no queue).
 */

package zio.pdf

import java.nio.file.Path
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicReference

import zio.*
import zio.stream.ZStream

object HyperdriveStream {

  /** Sentinel: producer finished (success or failure). */
  private val StreamEnd = new AnyRef

  /** mmap fused decode — one [[Decoded]] per stream element. */
  def decoded(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Decoded] =
    fromPathSink(queueCapacity) { emit =>
      PdfHyperdrive.decodeFromPathSink(path, enableDiagnostics, config, batchSize)(emit)
    }

  /** Triple-fused [[Element]] stream from mmap hyperdrive. */
  def elements(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Element] =
    fromPathSink(queueCapacity) { emit =>
      PdfHyperdrive.elementsFromPathSink(path, enableDiagnostics, config, batchSize)(emit)
    }

  /**
   * Drive a sync path sink into a bounded blocking queue so decode stalls
   * when downstream is slow — never builds a timeline [[Chunk]].
   */
  private def fromPathSink[A <: AnyRef](
    queueCapacity: Int
  )(run: (A => Unit) => Long): ZStream[Any, Throwable, A] = {
    val capacity = math.max(1, queueCapacity)
    ZStream.unwrapScoped {
      for {
        queue <- ZIO.succeed(new ArrayBlockingQueue[AnyRef](capacity))
        error <- ZIO.succeed(new AtomicReference[Throwable](null))
        _ <- ZIO
               .attemptBlockingInterrupt {
                 try {
                   val _ = run { a =>
                     queue.put(a)
                   }
                 } catch {
                   case _: InterruptedException =>
                     () // scope interrupt — StreamEnd still published in finally
                   case t: Throwable =>
                     error.set(t)
                 } finally {
                   try {
                     queue.put(StreamEnd)
                   } catch {
                     case _: InterruptedException =>
                       val _ = queue.offer(StreamEnd)
                   }
                 }
               }
               .forkScoped
      } yield ZStream.repeatZIOOption {
        ZIO
          .attemptBlockingInterrupt(queue.take())
          .mapError(Some(_))
          .flatMap {
            case end if end eq StreamEnd =>
              val t = error.get
              if (t ne null) ZIO.fail(Some(t))
              else ZIO.fail(None)
            case a =>
              ZIO.succeed(a.asInstanceOf[A])
          }
      }
    }
  }
}
