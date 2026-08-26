/*
 * Synchronous PDF decode — no `ZStream`, no `ZChannel`, no `Runtime`.
 *
 * Array inputs use [[zio.pdf.pipe.FusedDecode]]. Path inputs use
 * [[FusedDecoder]] over one bounded reusable read buffer.
 * Callers should prefer [[PdfEngine]]; this object stays package-private.
 */

package zio.pdf

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest

import zio.Chunk
import zio.pdf.pipe.FusedDecode
import zio.pdf.pipe.FusedDecode.Cfg

import scala.annotation.tailrec

private[pdf] object PdfHyperdrive {

  private def cfg(
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config,
    batchSize: Int
  ): Cfg =
    Cfg(enableDiagnostics, config, batchSize)

  private def each[A](chunk: Chunk[A])(consume: A => Unit): Unit = {
    val iterator = chunk.iterator
    while iterator.hasNext do consume(iterator.next())
  }

  /** Fold one bounded reusable read buffer; consumers must not retain `bytes`. */
  private def foldPathChunks[A](
    path: Path,
    chunkSize: Int,
    initial: A
  )(step: (A, Array[Byte], Int) => A): A = {
    val effectiveChunkSize = FusedDecoder.normalizedChunkSize(chunkSize)
    val channel            = FileChannel.open(path, StandardOpenOption.READ)
    val buffer  = ByteBuffer.allocate(effectiveChunkSize)
    try {
      @tailrec
      def loop(acc: A): A = {
        buffer.clear()
        channel.read(buffer) match {
          case -1 => acc
          case 0  => loop(acc)
          case n  => loop(step(acc, buffer.array(), n))
        }
      }
      loop(initial)
    } finally channel.close()
  }

  private final case class PathDecodeState(decoder: FusedDecoder.State, count: Long)

  private def orThrow[A](value: Either[Throwable, A]): A =
    value.fold(throw _, identity)

  private def decodePathSinkWithBytes(
    path: Path,
    enableDiagnostics: Boolean,
    config: StreamingDecode.Config,
    batchSize: Int,
    onBytes: (Array[Byte], Int, Int) => Unit
  )(sink: Decoded => Unit): Long = {
    val state = foldPathChunks(path, batchSize, PathDecodeState(FusedDecoder.initial, 0L)) {
      (current, bytes, length) =>
      onBytes(bytes, 0, length)
      val result = orThrow(
        FusedDecoder.run(current.decoder, FusedDecoder.feedBytes(bytes, 0, length, config))
      )
      each(result.emitted)(sink)
      PathDecodeState(result.next, current.count + result.emitted.size.toLong)
    }
    val tail = orThrow(FusedDecoder.run(state.decoder, FusedDecoder.finish(enableDiagnostics, config)))
    each(tail.emitted)(sink)
    state.count + tail.emitted.size.toLong
  }

  private def classify(decoded: Decoded): Element =
    Elements.classifyOne(decoded) match {
      case Left(error)  => throw error
      case Right(value) => value
    }

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

  /**
   * File decode with one bounded reusable buffer. This collects only the
   * requested decoded timeline, never an input-sized byte array.
   */
  def decodeFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] = {
    val builder = Chunk.newBuilder[Decoded]
    val _       = decodeFromPathSink(path, enableDiagnostics, config, batchSize)(value => builder += value)
    builder.result()
  }

  /**
   * Incremental fused decode with a per-event sink — never builds an
   * input-sized byte array or decoded timeline.
   * Returns the number of [[Decoded]] values delivered to `sink`.
   */
  def decodeFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): Long =
    decodePathSinkWithBytes(path, enableDiagnostics, config, batchSize, (_, _, _) => ())(sink)

  /**
   * In-memory [[decodeSyncSlice]] with a sink — no timeline [[Chunk]].
   * Pass a literal/`inline` lambda so the sink beta-reduces into HyperFuse.
   */
  inline def decodeSyncSink(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(inline sink: Decoded => Unit): Long =
    decodeSyncSliceSink(bytes, 0, bytes.length, enableDiagnostics, config, batchSize)(sink)

  /** Non-inline sink entry — JMH / reflective callers (body stays in [[FusedDecode]]). */
  def decodeSyncSinkRuntime(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): Long =
    FusedDecode.decodeSliceSinkRuntime(
      FusedDecode.Slice(bytes, 0, bytes.length),
      cfg(enableDiagnostics, config, batchSize)
    )(sink)

  inline def decodeSyncSliceSink(
    bytes: Array[Byte],
    offset: Int,
    length: Int,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(inline sink: Decoded => Unit): Long =
    FusedDecode.decodeSliceSink(
      FusedDecode.Slice(bytes, offset, length),
      cfg(enableDiagnostics, config, batchSize)
    )(sink)

  /** File elements — incremental decode, expansion, and classification. */
  def elementsFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Element] = {
    val builder = Chunk.newBuilder[Element]
    val _       = elementsFromPathSink(path, enableDiagnostics, config, batchSize)(value => builder += value)
    builder.result()
  }

  /**
   * Incremental decode, expansion, and classify with a per-event sink.
   * Returns the number of [[Element]] values delivered to `sink`.
   */
  def elementsFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => Unit): Long =
    decodeFromPathSink(path, enableDiagnostics, config, batchSize)(decoded => sink(classify(decoded)))

  /** Decode + SHA-256 in one incremental scan. */
  def decodeAndDigestFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): (Chunk[Decoded], Array[Byte]) = {
    val builder = Chunk.newBuilder[Decoded]
    val (_, digest) = decodeAndDigestFromPathSink(path, enableDiagnostics, config, batchSize) { value =>
      builder += value
    }
    (builder.result(), digest)
  }

  /**
   * Incremental decode + SHA-256 with a per-event sink.
   * Returns event count and the raw-file digest from the same fused scan.
   */
  def decodeAndDigestFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): (Long, Array[Byte]) = {
    val digest = MessageDigest.getInstance("SHA-256")
    val count = decodePathSinkWithBytes(
      path,
      enableDiagnostics,
      config,
      batchSize,
      (bytes, offset, length) => digest.update(bytes, offset, length)
    )(sink)
    (count, digest.digest())
  }

  /** In-memory counterpart of [[decodeAndDigestFromPathSink]], without a decoded timeline. */
  def decodeAndDigestSyncSink(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Decoded => Unit): (Long, Array[Byte]) = {
    val result = zio.pdf.pipe.HyperFuse.fuseDecodedWithDigestSink(
      FusedDecode.Slice(bytes, 0, bytes.length),
      cfg(enableDiagnostics, config, batchSize)
    )(sink)
    (result.count, result.digest)
  }

  /** Incremental elements + SHA-256 in one scan. */
  def elementsAndDigestFromPath(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): (Chunk[Element], Array[Byte]) = {
    val builder = Chunk.newBuilder[Element]
    val (_, digest) = elementsAndDigestFromPathSink(path, enableDiagnostics, config, batchSize) { value =>
      builder += value
    }
    (builder.result(), digest)
  }

  /**
   * Incremental decode, classify, and SHA-256 with a per-event sink.
   * Returns event count and the raw-file digest from the same fused scan.
   */
  def elementsAndDigestFromPathSink(
    path: Path,
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )(sink: Element => Unit): (Long, Array[Byte]) = {
    val digest = MessageDigest.getInstance("SHA-256")
    val count = decodePathSinkWithBytes(
      path,
      enableDiagnostics,
      config,
      batchSize,
      (bytes, offset, length) => digest.update(bytes, offset, length)
    )(decoded => sink(classify(decoded)))
    (count, digest.digest())
  }

  /** SHA-256 over raw file bytes with one bounded reusable buffer. */
  def digestFromPath(
    path: Path,
    batchSize: Int = 10 * 1024 * 1024
  ): Array[Byte] = {
    val digest = MessageDigest.getInstance("SHA-256")
    foldPathChunks(path, batchSize, digest) { (current, bytes, length) =>
      current.update(bytes, 0, length)
      current
    }.digest()
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
