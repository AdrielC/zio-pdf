/*
 * Synchronous PDF decode — no `ZStream`, no `ZChannel`, no `Runtime`.
 *
 * Hot paths delegate to [[zio.pdf.pipe.FusedDecode]] (fused streaming +
 * expansion) and [[zio.pdf.pipe.DecodePipeline]] (composed I/O morphisms).
 * Callers should prefer [[PdfEngine]]; this object stays package-private.
 */

package zio.pdf

import java.nio.MappedByteBuffer
import java.nio.file.Path

import zio.Chunk
import zio.pdf.pipe.DecodePipeline
import zio.pdf.pipe.FusedDecode
import zio.pdf.pipe.FusedDecode.Cfg

private[pdf] object PdfHyperdrive {

  private def cfg(
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config,
    batchSize: Int
  ): Cfg =
    Cfg(enableDiagnostics, config, batchSize)

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
  ): Chunk[StreamingDecoded] =
    FusedDecode.decodeStreamingSlice(
      FusedDecode.Slice(bytes, offset, length),
      cfg(enableDiagnostics, config, batchSize)
    )

  /**
   * Full [[Decoded]] timeline synchronously — fused streaming parse plus
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
  ): Chunk[Decoded] =
    FusedDecode.decodeSlice(
      FusedDecode.Slice(bytes, offset, length),
      cfg(enableDiagnostics, config, batchSize)
    )

  /**
   * Decode from a memory-mapped buffer without copying when the mapping
   * is array-backed (typical for file-backed maps on HotSpot).
   */
  def decodeSyncMapped(
    mapped: MappedByteBuffer,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] = {
    val slice = DecodePipeline.bytesFromMapped.run(mapped)
    FusedDecode.decodeSlice(slice, cfg(enableDiagnostics, config, batchSize))
  }

  /** Full decode plus [[Elements]] classification in one synchronous pass. */
  def elementsSync(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    elementsFusedSync(bytes, enableDiagnostics, config, batchSize)

  /**
   * Triple-fused elements — parse, expand, and classify without materialising
   * [[StreamingDecoded]] or [[Decoded]] timelines.
   */
  def elementsFusedSync(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    zio.pdf.pipe.FusedElements.decodeBytes(bytes, enableDiagnostics, config, batchSize)

  /** Staged elements (decode then classify) — parity / debug. */
  def elementsStagedSync(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    Elements.foldSync(decodeSync(bytes, enableDiagnostics, config, batchSize))

  /** mmap triple-fused elements. */
  def elementsSyncMapped(
    mapped: MappedByteBuffer,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] = {
    val slice = DecodePipeline.bytesFromMapped.run(mapped)
    zio.pdf.pipe.FusedElements.decodeSlice(slice, cfg(enableDiagnostics, config, batchSize))
  }

  /**
   * File decode with no `ZIO` — mmap, fused decode, close.
   * Hot path for [[PdfEngine]] Live.
   */
  def decodeFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    DecodePipeline.fromPath(cfg(enableDiagnostics, config, batchSize)).run(path)

  /**
   * mmap fused decode with a per-event sink — never builds `Chunk[Decoded]`.
   * Raw bytes come from mmap; parse windows are batched by `batchSize`.
   * Returns the number of [[Decoded]] values delivered to `sink`.
   */
  def decodeFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): Long =
    DecodePipeline.fromPathSink(cfg(enableDiagnostics, config, batchSize))(path, sink)

  /** In-memory [[decodeSyncSlice]] with a sink — no timeline [[Chunk]]. */
  def decodeSyncSink(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): Long =
    decodeSyncSliceSink(bytes, 0, bytes.length, enableDiagnostics, config, batchSize)(sink)

  def decodeSyncSliceSink(
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): Long =
    FusedDecode.decodeSliceSink(
      FusedDecode.Slice(bytes, offset, length),
      cfg(enableDiagnostics, config, batchSize)
    )(sink)

  /** mmap decode — default sync file path. */
  def decodeFromPathMapped(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    DecodePipeline.fromPathMmap(cfg(enableDiagnostics, config, batchSize)).run(path)

  /** File elements — mmap read, triple-fused classify. */
  def elementsFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] =
    DecodePipeline.elementsFromPath(cfg(enableDiagnostics, config, batchSize)).run(path)

  /**
   * mmap triple-fused classify with a per-event sink — never builds timelines.
   * Returns the number of [[Element]] values delivered to `sink`.
   */
  def elementsFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => Unit): Long =
    DecodePipeline.elementsFromPathSink(cfg(enableDiagnostics, config, batchSize))(path, sink)

  /** Decode + SHA-256 in one fused scan (mmap). */
  def decodeAndDigestFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): (Chunk[Decoded], Array[Byte]) = {
    val r = zio.pdf.pipe.IngestPipeline.decodeAndDigest
      .fromPath(cfg(enableDiagnostics, config, batchSize))
      .run(path)
    (r.decoded, r.digest)
  }

  /**
   * mmap decode + SHA-256 with a per-event sink — never materialises `Chunk[Decoded]`.
   * Returns event count and the raw-file digest from the same fused scan.
   */
  def decodeAndDigestFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): (Long, Array[Byte]) = {
    val r = zio.pdf.pipe.IngestPipeline.decodeAndDigest
      .fromPathSink(cfg(enableDiagnostics, config, batchSize))(path, sink)
    (r.count, r.digest)
  }

  /** Triple-fused elements + SHA-256 in one scan. */
  def elementsAndDigestFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): (Chunk[Element], Array[Byte]) = {
    val r = zio.pdf.pipe.IngestPipeline.decodeAndDigest
      .elementsFromPath(cfg(enableDiagnostics, config, batchSize))
      .run(path)
    (r.decoded, r.digest)
  }

  /**
   * mmap triple-fused classify + SHA-256 with a per-event sink.
   * Returns event count and the raw-file digest from the same fused scan.
   */
  def elementsAndDigestFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => Unit): (Long, Array[Byte]) = {
    val r = zio.pdf.pipe.IngestPipeline.decodeAndDigest
      .elementsFromPathSink(cfg(enableDiagnostics, config, batchSize))(path, sink)
    (r.count, r.digest)
  }

  /** SHA-256 over raw file bytes — mmap + batched scan, no decode. */
  def digestFromPath(
    path: Path,
    batchSize: Int = 10 * 1024 * 1024
  ): Array[Byte] = {
    val c = Cfg(batchSize = batchSize)
    zio.pdf.pipe.ByteDigest.digestSlice(DecodePipeline.readSlice(c).run(path), c)
  }

  def digestSync(bytes: Array[Byte], batchSize: Int = 10 * 1024 * 1024): Array[Byte] =
    zio.pdf.pipe.ByteDigest.digestSlice(
      FusedDecode.Slice(bytes, 0, bytes.length),
      Cfg(batchSize = batchSize)
    )

  /** In-memory triple-fused classify with a per-event sink. */
  def elementsSyncSink(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => Unit): Long =
    zio.pdf.pipe.FusedElements.decodeSliceSink(
      FusedDecode.Slice(bytes, 0, bytes.length),
      cfg(enableDiagnostics, config, batchSize)
    )(sink)
}
