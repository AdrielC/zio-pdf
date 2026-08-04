/*
 * Fused hyperdrive decode — streaming parse and ObjStm/XRef expansion in one pass.
 *
 * Volga would materialise an intermediate product (events × state) then link;
 * we fold [[DecodedFromStreaming]] per batch via [[HyperFuse]] so the
 * full streaming timeline never lands in a [[Chunk]].
 */

package zio.pdf.pipe

import zio.Chunk
import zio.pdf.{Decoded, StreamingDecode, StreamingDecoded}

private[pdf] object FusedDecode {

  final case class Slice(bytes: Array[Byte], offset: Int, length: Int)

  final case class Cfg(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  )

  /** Fused [[PdfHyperdrive.decodeSync]] hot path — single builder, no per-batch [[Chunk]]. */
  def decodeSlice(slice: Slice, cfg: Cfg): Chunk[Decoded] = {
    val builder = Chunk.newBuilder[Decoded]
    HyperFuse.fuseDecodedBuild(slice, cfg, d => builder += d)
    builder.result()
  }

  /**
   * Sink each [[Decoded]] as produced — never materialises a timeline [[Chunk]].
   * Returns the number of values delivered to `sink`.
   *
   * `sink` is `inline` so call-site lambdas fuse into the HyperFuse loop.
   */
  inline def decodeSliceSink(slice: Slice, cfg: Cfg)(inline sink: Decoded => Unit): Long = {
    var count = 0L
    HyperFuse.fuseDecodedBuild(slice, cfg, d => { sink(d); count += 1 })
    count
  }

  /** Non-inline sink — safe for JMH forks (no call-site inline into `zio.pdf.pipe`). */
  def decodeSliceSinkRuntime(slice: Slice, cfg: Cfg)(sink: Decoded => Unit): Long = {
    var count = 0L
    HyperFuse.fuseDecodedBuild(slice, cfg, d => { sink(d); count += 1 })
    count
  }

  def decodeBytes(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[Decoded] =
    decodeSlice(Slice(bytes, 0, bytes.length), Cfg(enableDiagnostics, config, batchSize))

  /** Streaming timeline only — parity tests against [[PdfStream.streamingDecode]]. */
  def decodeStreamingSlice(slice: Slice, cfg: Cfg): Chunk[StreamingDecoded] =
    ByteFeed.streamingEvents(slice, cfg)

  def decodeStreamingBytes(
    bytes: Array[Byte],
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default,
    batchSize: Int = 10 * 1024 * 1024
  ): Chunk[StreamingDecoded] =
    decodeStreamingSlice(Slice(bytes, 0, bytes.length), Cfg(enableDiagnostics, config, batchSize))

  val decode: Pipe[(Slice, Cfg), Chunk[Decoded]] =
    Pipe { case (slice, cfg) => decodeSlice(slice, cfg) }
}
