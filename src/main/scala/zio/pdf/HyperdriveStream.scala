/*
 * Hyperdrive decode as [[ZStream]] — mmap fused parse.
 *
 * Emits one element at a time from [[PdfHyperdrive.decodeFromPath]] without
 * building a timeline [[zio.Chunk]] in user code. For constant-memory sinks
 * on huge PDFs, prefer [[zio.pdf.io.PdfIO.warpStreaming]].
 *
 * Shared by [[zio.pdf.io.PdfIO]] and [[zio.pdf.PdfStream]] without cyclic imports.
 */

package zio.pdf

import java.nio.file.Path

import zio.ZIO
import zio.stream.ZStream

object HyperdriveStream {

  /** mmap fused decode — one [[Decoded]] per stream element. */
  def decoded(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Decoded] = {
    val _ = queueCapacity
    ZStream.fromIterableZIO(
      ZIO.attemptBlocking(
        PdfHyperdrive.decodeFromPath(path, enableDiagnostics, config, batchSize)
      )
    )
  }

  /** Triple-fused [[Element]] stream from mmap hyperdrive. */
  def elements(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024,
    queueCapacity: Int = 256
  ): ZStream[Any, Throwable, Element] = {
    val _ = queueCapacity
    ZStream.fromIterableZIO(
      ZIO.attemptBlocking(
        PdfHyperdrive.elementsFromPath(path, enableDiagnostics, config, batchSize)
      )
    )
  }
}
