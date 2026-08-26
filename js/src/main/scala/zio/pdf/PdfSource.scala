package zio.pdf

import org.scalajs.dom
import scala.scalajs.js.typedarray.Uint8Array
import zio.{Chunk, ZIO}
import zio.stream.ZStream

/** A byte source that the platform-specific `PdfEngine` can consume. */
trait PdfSource:
  def bytes: ZStream[Any, Throwable, Byte]

object PdfSource:

  /** A reusable in-memory source. */
  def fromChunk(input: Chunk[Byte]): PdfSource =
    new PdfSource:
      val bytes: ZStream[Any, Throwable, Byte] = ZStream.fromChunk(input)

  /** A reusable browser / Node `Uint8Array` source. */
  def fromUint8Array(input: Uint8Array): PdfSource =
    fromChunk(Chunk.fromArray(JsBinary.bytes(input)))

  /**
   * A reusable browser `Blob` source. `Blob.stream()` is opened for each run,
   * so callers can decode and then validate the same Blob independently.
   */
  def fromBlob(blob: dom.Blob): PdfSource =
    fromFactory(() => blob.stream())

  /**
   * A one-shot WHATWG stream source. Callers that need to consume it twice
   * should use `fromBlob` or tee the browser stream before wrapping it.
   */
  def fromReadableStream(stream: dom.ReadableStream[Uint8Array]): PdfSource =
    fromFactory(() => stream)

  private def fromFactory(open: () => dom.ReadableStream[Uint8Array]): PdfSource =
    new PdfSource:
      def bytes: ZStream[Any, Throwable, Byte] =
        ZStream.unwrapScoped {
          ZIO
            .acquireRelease(ZIO.succeed(open().getReader()))(reader => ZIO.fromPromiseJS(reader.cancel()).ignore)
            .map { reader =>
              ZStream
                .repeatZIOOption {
                  ZIO
                    .fromPromiseJS(reader.read())
                    .mapError(Some(_))
                    .flatMap { next =>
                      if next.done then ZIO.fail(None)
                      else ZIO.succeed(Chunk.fromArray(JsBinary.bytes(next.value)))
                    }
                }
                .flattenChunks
            }
        }
