/*
 * Shared hyperdrive fuse — one [[ByteFeed.runBatched]] loop, pluggable sinks.
 *
 * [[FusedDecode]] and [[FusedElements]] differ only in what they do with each
 * [[zio.pdf.Decoded]] emission; ingest digests the same byte windows inline.
 */

package zio.pdf.pipe

import java.security.MessageDigest

import zio.Chunk
import zio.pdf.{Decoded, DecodedFromStreaming, Element, Elements, StreamingDecode, StreamingDecoded}
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object HyperFuse {

  private def streamingStep(cfg: Cfg): ByteFeed.Step[StreamingDecode.FinalState, StreamingDecoded] = {
    val Cfg(_, config, _) = cfg
    (fs, buf, off, len) => StreamingDecode.stepChunkBytes(config, fs, buf, off, len)
  }

  private def streamingFinalize(cfg: Cfg): StreamingDecode.FinalState => Chunk[StreamingDecoded] = {
    val Cfg(diag, _, _) = cfg
    fs => StreamingDecode.finalizeToMetaSync(diag, fs)
  }

  /** Streaming parse + ObjStm/XRef bridge; emits each [[Decoded]] via `emit`. */
  def fuseDecodedBuild(slice: Slice, cfg: Cfg, emit: Decoded => Unit, onBytes: ByteFeed.OnBytes = (_, _, _) => ()): Unit = {
    var bridge = DecodedFromStreaming.accInitial
    val onStreaming: ByteFeed.OnEvents[StreamingDecoded] = events =>
      bridge = DecodedFromStreaming.foldEventsAcc(bridge, events, emit)
    ByteFeed.runBatched(
      slice,
      cfg.batchSize,
      StreamingDecode.initialFinalState,
      streamingStep(cfg),
      streamingFinalize(cfg),
      onBytes = onBytes,
      onEvents = onStreaming
    )
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then
      val it = tail.iterator
      while it.hasNext do emit(it.next())
  }

  /** Streaming parse + ObjStm/XRef bridge; emits [[Decoded]] batches to [[sink]]. */
  def fuseDecoded(slice: Slice, cfg: Cfg, onBytes: ByteFeed.OnBytes = (_, _, _) => ())(
    sink: Chunk[Decoded] => Unit
  ): Unit = {
    var bridge = DecodedFromStreaming.accInitial
    val onStreaming: ByteFeed.OnEvents[StreamingDecoded] = events => {
      val (decoded, next) = DecodedFromStreaming.foldSync(bridge, events)
      if !decoded.isEmpty then sink(decoded)
      bridge = next
    }
    ByteFeed.runBatched(
      slice,
      cfg.batchSize,
      StreamingDecode.initialFinalState,
      streamingStep(cfg),
      streamingFinalize(cfg),
      onBytes = onBytes,
      onEvents = onStreaming
    )
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then sink(tail)
  }

  /** Triple-fuse: parse → expand → classify; emits each [[Element]] via `emit`. */
  def fuseElementsBuild(slice: Slice, cfg: Cfg, emit: Element => Unit, onBytes: ByteFeed.OnBytes = (_, _, _) => ()): Unit =
    fuseDecodedBuild(slice, cfg, d => Elements.classifyOne(d) match {
      case Left(err)      => throw err
      case Right(element) => emit(element)
    }, onBytes)

  /** Triple-fuse: parse → expand → classify; never materialises timelines. */
  def fuseElements(slice: Slice, cfg: Cfg, onBytes: ByteFeed.OnBytes = (_, _, _) => ())(
    sink: Element => Unit
  ): Unit =
    fuseElementsBuild(slice, cfg, sink, onBytes)

  /** Decode + SHA-256 in one scan — digest windows match streaming batches. */
  def fuseDecodedWithDigest(slice: Slice, cfg: Cfg): (Chunk[Decoded], Array[Byte]) = {
    val builder = Chunk.newBuilder[Decoded]
    val r       = fuseDecodedWithDigestSink(slice, cfg)(d => builder += d)
    (builder.result(), r.digest)
  }

  /**
   * Decode + SHA-256 with a per-event sink — never materialises `Chunk[Decoded]`.
   * Digest is computed over the same byte windows as [[fuseDecodedBuild]].
   */
  def fuseDecodedWithDigestSink(slice: Slice, cfg: Cfg)(sink: Decoded => Unit): DigestSink = {
    val md = MessageDigest.getInstance("SHA-256")
    var count = 0L
    fuseDecodedBuild(
      slice,
      cfg,
      d => { sink(d); count += 1 },
      onBytes = (buf, off, len) => md.update(buf, off, len)
    )
    DigestSink(count, md.digest())
  }

  def fuseElementsWithDigest(slice: Slice, cfg: Cfg): (Chunk[Element], Array[Byte]) = {
    val builder = Chunk.newBuilder[Element]
    val r       = fuseElementsWithDigestSink(slice, cfg)(el => builder += el)
    (builder.result(), r.digest)
  }

  /** Elements + SHA-256 with a per-event sink — never materialises timelines. */
  def fuseElementsWithDigestSink(slice: Slice, cfg: Cfg)(sink: Element => Unit): DigestSink = {
    val md = MessageDigest.getInstance("SHA-256")
    var count = 0L
    fuseElementsBuild(
      slice,
      cfg,
      el => { sink(el); count += 1 },
      onBytes = (buf, off, len) => md.update(buf, off, len)
    )
    DigestSink(count, md.digest())
  }

  /** Fused scan summary — event count plus raw-file SHA-256. */
  final case class DigestSink(count: Long, digest: Array[Byte])
}
