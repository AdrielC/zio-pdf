/*
 * Shared hyperdrive fuse — [[ByteFeed]] ZPure steps, StreamDecoder-style
 * [[ByteFeed.runWindows]] so sinks never retain the full event log.
 *
 * Sink spines take `inline` emit/consume lambdas so classify / count /
 * digest beta-reduce into the window loop (same pattern as
 * [[zio.scan.InlineByteScan]]).
 *
 * Digest shares the same windows by putting [[MessageDigest]] in ZPure state
 * beside the decode machine ([[Digested]]).
 */

package zio.pdf.pipe

import java.security.MessageDigest

import zio.Chunk
import zio.pdf.{Decoded, DecodedFromStreaming, Element, Elements, StreamingDecode, StreamingDecoded}
import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

private[pdf] object HyperFuse {

  /** Decode machine + incremental SHA-256 over the same windows. */
  private final case class Digested[S](machine: S, md: MessageDigest)

  private def streamingStep(cfg: Cfg): ByteFeed.Step[StreamingDecode.FinalState, StreamingDecoded] = {
    val Cfg(_, config, _) = cfg
    ByteFeed.fromSync((fs, buf, off, len) => StreamingDecode.stepChunkBytes(config, fs, buf, off, len))
  }

  private def streamingFinalize(cfg: Cfg): ByteFeed.Finalize[StreamingDecode.FinalState, StreamingDecoded] = {
    val Cfg(diag, _, _) = cfg
    fs => ByteFeed.logAll(StreamingDecode.finalizeToMetaSync(diag, fs))
  }

  private def streamingStepDigested(cfg: Cfg): ByteFeed.Step[Digested[StreamingDecode.FinalState], StreamingDecoded] = {
    val Cfg(_, config, _) = cfg
    ByteFeed.fromSync { (d, buf, off, len) =>
      d.md.update(buf, off, len)
      val (out, next) = StreamingDecode.stepChunkBytes(config, d.machine, buf, off, len)
      (out, Digested(next, d.md))
    }
  }

  private def streamingFinalizeDigested(cfg: Cfg): ByteFeed.Finalize[Digested[StreamingDecode.FinalState], StreamingDecoded] = {
    val Cfg(diag, _, _) = cfg
    d => ByteFeed.logAll(StreamingDecode.finalizeToMetaSync(diag, d.machine))
  }

  private inline def runStreaming(
    slice: Slice,
    cfg: Cfg,
    step: ByteFeed.Step[StreamingDecode.FinalState, StreamingDecoded]
  )(inline consume: Chunk[StreamingDecoded] => Unit): Unit = {
    val _ = ByteFeed.runWindows(
      slice,
      cfg.batchSize,
      StreamingDecode.initialFinalState,
      step,
      streamingFinalize(cfg)
    )(consume)
  }

  private inline def runStreamingDigested(
    slice: Slice,
    cfg: Cfg,
    md: MessageDigest
  )(inline consume: Chunk[StreamingDecoded] => Unit): MessageDigest = {
    val end = ByteFeed.runWindows(
      slice,
      cfg.batchSize,
      Digested(StreamingDecode.initialFinalState, md),
      streamingStepDigested(cfg),
      streamingFinalizeDigested(cfg)
    )(consume)
    end.md
  }

  /** Streaming parse + ObjStm/XRef bridge; emits each [[Decoded]] via `emit`. */
  inline def fuseDecodedBuild(slice: Slice, cfg: Cfg, inline emit: Decoded => Unit): Unit = {
    var bridge = DecodedFromStreaming.accInitial
    runStreaming(slice, cfg, streamingStep(cfg)) { log =>
      bridge = DecodedFromStreaming.foldEventsAcc(bridge, log, emit)
    }
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then
      val it = tail.iterator
      while it.hasNext do emit(it.next())
  }

  private inline def fuseDecodedBuildDigested(
    slice: Slice,
    cfg: Cfg,
    md: MessageDigest,
    inline emit: Decoded => Unit
  ): MessageDigest = {
    var bridge = DecodedFromStreaming.accInitial
    val endMd = runStreamingDigested(slice, cfg, md) { log =>
      bridge = DecodedFromStreaming.foldEventsAcc(bridge, log, emit)
    }
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then
      val it = tail.iterator
      while it.hasNext do emit(it.next())
    endMd
  }

  /** Streaming parse + ObjStm/XRef bridge; emits [[Decoded]] batches to [[sink]]. */
  inline def fuseDecoded(slice: Slice, cfg: Cfg)(inline sink: Chunk[Decoded] => Unit): Unit = {
    var bridge = DecodedFromStreaming.accInitial
    runStreaming(slice, cfg, streamingStep(cfg)) { log =>
      val (decoded, next) = DecodedFromStreaming.foldSync(bridge, log)
      if !decoded.isEmpty then sink(decoded)
      bridge = next
    }
    val tail = DecodedFromStreaming.finalizeSync(bridge)
    if !tail.isEmpty then sink(tail)
  }

  /** Triple-fuse: parse → expand → classify; emits each [[Element]] via `emit`. */
  inline def fuseElementsBuild(slice: Slice, cfg: Cfg, inline emit: Element => Unit): Unit =
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
  inline def fuseElements(slice: Slice, cfg: Cfg)(inline sink: Element => Unit): Unit =
    fuseElementsBuild(slice, cfg, sink)

  /** Decode + SHA-256 in one scan — digest windows match streaming batches. */
  def fuseDecodedWithDigest(slice: Slice, cfg: Cfg): (Chunk[Decoded], Array[Byte]) = {
    val builder = Chunk.newBuilder[Decoded]
    val r       = fuseDecodedWithDigestSink(slice, cfg)(d => builder += d)
    (builder.result(), r.digest)
  }

  /**
   * Decode + SHA-256 with a per-event sink — never materialises `Chunk[Decoded]`.
   * Digest is [[ZPure]] state beside the decode machine.
   */
  inline def fuseDecodedWithDigestSink(slice: Slice, cfg: Cfg)(inline sink: Decoded => Unit): DigestSink = {
    val md    = MessageDigest.getInstance("SHA-256")
    var count = 0L
    val endMd = fuseDecodedBuildDigested(
      slice,
      cfg,
      md,
      d => { sink(d); count += 1 }
    )
    DigestSink(count, endMd.digest())
  }

  def fuseElementsWithDigest(slice: Slice, cfg: Cfg): (Chunk[Element], Array[Byte]) = {
    val builder = Chunk.newBuilder[Element]
    val r       = fuseElementsWithDigestSink(slice, cfg)(el => builder += el)
    (builder.result(), r.digest)
  }

  /** Elements + SHA-256 with a per-event sink — never materialises timelines. */
  inline def fuseElementsWithDigestSink(slice: Slice, cfg: Cfg)(inline sink: Element => Unit): DigestSink = {
    val md    = MessageDigest.getInstance("SHA-256")
    var count = 0L
    val endMd = fuseDecodedBuildDigested(
      slice,
      cfg,
      md,
      d =>
        Elements.classifyOne(d) match {
          case Left(err) => throw err
          case Right(el) =>
            sink(el)
            count += 1
        }
    )
    DigestSink(count, endMd.digest())
  }

  /** Fused scan summary — event count plus raw-file SHA-256. */
  final case class DigestSink(count: Long, digest: Array[Byte])
}
