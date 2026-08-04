/*
 * Shared hyperdrive fuse — one [[ByteFeed.runDrain]] loop, pluggable sinks.
 *
 * [[FusedDecode]] and [[FusedElements]] differ only in what they do with each
 * [[zio.pdf.Decoded]] emission; ingest digests the same byte windows via
 * [[ByteFeed.tapBytes]].
 */

package zio.pdf.pipe

import java.security.MessageDigest

import zio.Chunk
import zio.pdf.{Decoded, DecodedFromStreaming, Element, Elements, StreamingDecode, StreamingDecoded}
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object HyperFuse {

  private def streamingStep(cfg: Cfg): ByteFeed.Step[StreamingDecode.FinalState, StreamingDecoded] = {
    val Cfg(_, config, _) = cfg
    ByteFeed.fromSync((fs, buf, off, len) => StreamingDecode.stepChunkBytes(config, fs, buf, off, len))
  }

  private def streamingFinalize(cfg: Cfg): ByteFeed.Finalize[StreamingDecode.FinalState, StreamingDecoded] = {
    val Cfg(diag, _, _) = cfg
    fs => ByteFeed.emitBatch(StreamingDecode.finalizeToMetaSync(diag, fs))
  }

  private def runStreaming(
    slice: Slice,
    cfg: Cfg,
    step: ByteFeed.Step[StreamingDecode.FinalState, StreamingDecoded]
  )(drain: Chunk[StreamingDecoded] => Unit): Unit = {
    val _ = ByteFeed.runDrain(
      slice,
      cfg.batchSize,
      StreamingDecode.initialFinalState,
      step,
      streamingFinalize(cfg)
    )(drain)
  }

  /** Streaming parse + ObjStm/XRef bridge; emits each [[Decoded]] via `emit`. */
  def fuseDecodedBuild(slice: Slice, cfg: Cfg, emit: Decoded => Unit): Unit =
    fuseDecodedBuild(slice, cfg, emit, step = streamingStep(cfg))

  private def fuseDecodedBuild(
    slice: Slice,
    cfg: Cfg,
    emit: Decoded => Unit,
    step: ByteFeed.Step[StreamingDecode.FinalState, StreamingDecoded]
  ): Unit = {
    var bridge = DecodedFromStreaming.accInitial
    runStreaming(slice, cfg, step) { events =>
      bridge = DecodedFromStreaming.foldEventsAcc(bridge, events, emit)
    }
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then
      val it = tail.iterator
      while it.hasNext do emit(it.next())
  }

  /** Streaming parse + ObjStm/XRef bridge; emits [[Decoded]] batches to [[sink]]. */
  def fuseDecoded(slice: Slice, cfg: Cfg)(sink: Chunk[Decoded] => Unit): Unit = {
    var bridge = DecodedFromStreaming.accInitial
    runStreaming(slice, cfg, streamingStep(cfg)) { events =>
      val (decoded, next) = DecodedFromStreaming.foldSync(bridge, events)
      if !decoded.isEmpty then sink(decoded)
      bridge = next
    }
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then sink(tail)
  }

  /** Triple-fuse: parse → expand → classify; emits each [[Element]] via `emit`. */
  def fuseElementsBuild(slice: Slice, cfg: Cfg, emit: Element => Unit): Unit =
    fuseDecodedBuild(
      slice,
      cfg,
      d =>
        Elements.classifyOne(d) match {
          case Left(err)      => throw err
          case Right(element) => emit(element)
        }
    )

  /** Triple-fuse: parse → expand → classify; never materialises timelines. */
  def fuseElements(slice: Slice, cfg: Cfg)(sink: Element => Unit): Unit =
    fuseElementsBuild(slice, cfg, sink)

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
    val md    = MessageDigest.getInstance("SHA-256")
    var count = 0L
    fuseDecodedBuild(
      slice,
      cfg,
      d => { sink(d); count += 1 },
      step = ByteFeed.tapBytes((buf, off, len) => md.update(buf, off, len))(streamingStep(cfg))
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
    val md    = MessageDigest.getInstance("SHA-256")
    var count = 0L
    fuseDecodedBuild(
      slice,
      cfg,
      d =>
        Elements.classifyOne(d) match {
          case Left(err) => throw err
          case Right(el) =>
            sink(el)
            count += 1
        },
      step = ByteFeed.tapBytes((buf, off, len) => md.update(buf, off, len))(streamingStep(cfg))
    )
    DigestSink(count, md.digest())
  }

  /** Fused scan summary — event count plus raw-file SHA-256. */
  final case class DigestSink(count: Long, digest: Array[Byte])
}
