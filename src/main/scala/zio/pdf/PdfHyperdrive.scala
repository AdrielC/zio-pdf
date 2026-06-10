/*
 * Synchronous PDF decode — no `ZStream`, no `ZChannel`, no `Runtime`.
 *
 * The streaming state machine ([[StreamingDecode]]) and expansion bridge
 * ([[DecodedFromStreaming]]) already expose sync stepping; Hyperdrive
 * feeds the entire buffer (or large slices) in one tight loop and folds
 * [[Decoded]] on the spot. Use for in-memory PDFs and as the auto-fast
 * path from [[zio.pdf.io.PdfIO]] when the file fits in RAM.
 *
 * {{{
 *   val decoded = PdfHyperdrive.decodeSync(bytes)
 *   PdfIO.warp(path)  // ZIO wrapper (I/O only)
 * }}}
 */

package zio.pdf

import java.nio.MappedByteBuffer

import zio.Chunk

object PdfHyperdrive {

  /** Auto-route files at or below this size through [[decodeSync]]. */
  val defaultAutoThresholdBytes: Long = 32L * 1024 * 1024

  /**
   * Full [[StreamingDecoded]] timeline in one synchronous pass.
   * Diagnostics (when enabled) go to stderr via [[ZPureLog.drainSync]].
   */
  def decodeStreamingSync(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[StreamingDecoded] =
    decodeStreamingSyncSlice(bytes, 0, bytes.length, enableDiagnostics, config, batchSize)

  def decodeStreamingSyncSlice(
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[StreamingDecoded] = {
    val builder = Chunk.newBuilder[StreamingDecoded]
    var fs      = StreamingDecode.initialFinalState
    var pos     = offset
    val end     = offset + length
    while pos < end do
      val len         = math.min(batchSize, end - pos)
      val (out, next) = StreamingDecode.stepChunkBytes(config, fs, bytes, pos, len)
      builder ++= out
      fs = next
      pos += len
    builder ++= StreamingDecode.finalizeToMetaSync(enableDiagnostics, fs)
    builder.result()
  }

  /**
   * Full [[Decoded]] timeline synchronously — streaming parse plus
   * ObjStm / XRef expansion, identical semantics to
   * `bytes.via(PdfStream.decode())`.
   */
  def decodeSync(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    decodeSyncSlice(bytes, 0, bytes.length, enableDiagnostics, config, batchSize)

  def decodeSyncSlice(
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] = {
    val streaming = decodeStreamingSyncSlice(bytes, offset, length, enableDiagnostics, config, batchSize)
    val (decoded, acc) =
      DecodedFromStreaming.foldSync(DecodedFromStreaming.accInitial, streaming)
    decoded ++ DecodedFromStreaming.finalizeSync(acc)
  }

  /**
   * Decode from a memory-mapped buffer without copying when the mapping
   * is array-backed (typical for file-backed maps on HotSpot).
   */
  def decodeSyncMapped(
    mapped: MappedByteBuffer,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    if (mapped.hasArray) {
      val arr    = mapped.array()
      val offset = mapped.arrayOffset() + mapped.position()
      val length = mapped.remaining()
      decodeSyncSlice(arr, offset, length, enableDiagnostics, config, batchSize)
    } else {
      val dup = mapped.duplicate()
      val arr = new Array[Byte](dup.remaining())
      dup.get(arr)
      decodeSync(arr, enableDiagnostics, config, batchSize)
    }

  /** Full decode plus [[Elements]] classification in one synchronous pass. */
  def elementsSync(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    Elements.foldSync(decodeSync(bytes, enableDiagnostics, config, batchSize))

  /** mmap [[decodeSyncMapped]] + [[Elements.foldSync]] — file sicko in-memory. */
  def elementsSyncMapped(
    mapped: MappedByteBuffer,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    Elements.foldSync(decodeSyncMapped(mapped, enableDiagnostics, config, batchSize))

  /** In-memory full sicko: hyperdrive decode (alias for [[decodeSync]]). */
  def sicko(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    decodeSync(bytes, enableDiagnostics, config, batchSize)

  /** mmap sicko — zero heap copy when the mapping is array-backed. */
  def sickoMapped(
    mapped: MappedByteBuffer,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    decodeSyncMapped(mapped, enableDiagnostics, config, batchSize)

  /** Decode + classify in one synchronous pass (alias for [[elementsSync]]). */
  def sickoElements(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    elementsSync(bytes, enableDiagnostics, config, batchSize)

  def sickoElementsMapped(
    mapped: MappedByteBuffer,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    elementsSyncMapped(mapped, enableDiagnostics, config, batchSize)

  def fitsInHyperdrive(fileSizeBytes: Long, thresholdBytes: Long = defaultAutoThresholdBytes): Boolean =
    fileSizeBytes >= 0 && fileSizeBytes <= thresholdBytes
}
