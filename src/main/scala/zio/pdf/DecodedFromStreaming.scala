/*
 * Bridge [[StreamingDecoded]] → [[Decoded]] using [[StatefulPipe]].
 * Embeds xrefs from expanded stream payloads (Left branch of
 * [[Decode.expandStreamPayload]]) into [[Decoded.Meta]] after the
 * textual xrefs carried on [[StreamingDecoded.Meta]].
 *
 * Hot path ([[foldSync]] / [[finalizeSync]]) is imperative — no
 * ZPure interpreter. The streaming [[pipeline]] reuses the same
 * [[applyStep]] via a thin ZPure wrapper.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.Chunk
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

object DecodedFromStreaming {

  final case class MaterializedStreamLimitExceeded(objectNumber: Long, declared: Long, maxBytes: Long)
      extends RuntimeException(
        s"content stream $objectNumber declares $declared bytes, above the configured $maxBytes-byte materialization limit"
      )

  /** Pre-sized buffer for chunked content streams (avoids Chunk concatenation). */
  private[pdf] final case class StreamBuf(obj: Obj, bytes: Array[Byte], filled: Int)

  /** Mutable bridge state between [[StreamingDecoded]] chunks and [[Decoded]] output. */
  final case class Acc(
    collect: Option[StreamBuf],
    embeddedXrefs: List[Xref]
  )

  val accInitial: Acc = Acc(None, Nil)

  private val acc0: Acc = accInitial

  /** Copy the in-flight content-stream buffer for an independent checkpoint. */
  private[pdf] def snapshot(acc: Acc): Acc =
    acc.copy(collect = acc.collect.map(buf => buf.copy(bytes = buf.bytes.clone())))

  private def fromAttempt[A](a: Attempt[A]): Either[Throwable, A] =
    a match {
      case Attempt.Successful(v) => Right(v)
      case Attempt.Failure(c)    => Left(new RuntimeException(c.messageWithContext))
    }

  private def appendChunk(bytes: Array[Byte], filled: Int, c: Chunk[Byte]): Either[Throwable, Int] =
    c match {
      case Chunk.ByteArray(arr, off, len) =>
        val space = bytes.length - filled
        if (len > space)
          Left(new IllegalStateException(s"content stream overflow: $len bytes at offset $filled"))
        else {
          System.arraycopy(arr, off, bytes, filled, len)
          Right(filled + len)
        }
      case _ =>
        val it = c.iterator
        var f  = filled
        while it.hasNext do
          if (f >= bytes.length)
            return Left(new IllegalStateException(s"content stream overflow at offset $f"))
          bytes(f) = it.next()
          f += 1
        Right(f)
    }

  /**
   * Core imperative step — shared by [[foldSync]] and the streaming
   * [[pipeline]] (via ZPure wrapper).
   */
  private[pdf] def applyStep(
    s: Acc,
    ev: StreamingDecoded,
    maxMaterializedStreamBytes: ByteLimit = StreamingDecode.Config.default.maxMaterializedStreamBytes
  ): Either[Throwable, (Chunk[Decoded], Acc)] =
    ev match {
      case m: StreamingDecoded.Meta =>
        val mergedXrefs = m.xrefs ++ s.embeddedXrefs.reverse
        val trailers    = mergedXrefs.map(_.trailer)
        val sanitised   = zio.NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
        Right((Chunk.single(Decoded.Meta(mergedXrefs, sanitised, m.version)), acc0))

      case StreamingDecoded.DataObj(obj) =>
        Right((Chunk.single(Decoded.DataObj(obj)), s))

      case StreamingDecoded.VersionT(_) | _: StreamingDecoded.CommentT |
          _: StreamingDecoded.StartXrefT | _: StreamingDecoded.XrefT |
          _: StreamingDecoded.ObjectEnd =>
        Right((Chunk.empty, s))

      case StreamingDecoded.ContentObjStart(obj, _, Some(bits)) =>
        fromAttempt(Decode.expandStreamPayload(obj.index, obj.data, bits, maxMaterializedStreamBytes)).map {
          case Left(xref)  => (Chunk.empty, s.copy(embeddedXrefs = xref :: s.embeddedXrefs))
          case Right(rows) => (Chunk.fromIterable(rows), s)
        }

      case StreamingDecoded.ContentObjStart(obj, length, None) =>
        s.collect match {
          case None =>
            if (length < 0L || length > Int.MaxValue)
              Left(new IllegalStateException(s"invalid stream length: $length"))
            else if (length > maxMaterializedStreamBytes.toLong)
              Left(MaterializedStreamLimitExceeded(obj.index.number, length, maxMaterializedStreamBytes.toLong))
            else if (length == 0L)
              fromAttempt(
                Decode.expandStreamPayload(
                  obj.index,
                  obj.data,
                  _root_.scodec.bits.BitVector.empty,
                  maxMaterializedStreamBytes
                )
              )
                .map {
                  case Left(xref)  => (Chunk.empty, s.copy(embeddedXrefs = xref :: s.embeddedXrefs))
                  case Right(rows) => (Chunk.fromIterable(rows), s)
                }
            else
              Right((Chunk.empty, s.copy(collect = Some(StreamBuf(obj, new Array(length.toInt), 0)))))
          case Some(_) => Left(new IllegalStateException("nested ContentObjStart"))
        }

      case StreamingDecoded.ContentObjBytes(c) =>
        s.collect match {
          case Some(buf @ StreamBuf(_, bytes, filled)) =>
            appendChunk(bytes, filled, c) match {
              case Left(err)   => Left(err)
              case Right(next) => Right((Chunk.empty, s.copy(collect = Some(buf.copy(filled = next)))))
            }
          case None => Left(new IllegalStateException("ContentObjBytes without ContentObjStart"))
        }

      case StreamingDecoded.ContentObjEnd =>
        s.collect match {
          case Some(StreamBuf(obj, bytes, filled)) =>
            if (filled != bytes.length)
              Left(new IllegalStateException(s"short content stream: expected ${bytes.length} got $filled"))
            else {
              val bits = _root_.scodec.bits.BitVector(bytes)
              fromAttempt(Decode.expandStreamPayload(obj.index, obj.data, bits, maxMaterializedStreamBytes)).map {
                case Left(xref)  => (Chunk.empty, Acc(None, xref :: s.embeddedXrefs))
                case Right(rows) => (Chunk.fromIterable(rows), Acc(None, s.embeddedXrefs))
              }
            }
          case None =>
            Left(new IllegalStateException("ContentObjEnd without start"))
        }
    }

  /**
   * Fold streaming events into `emit` — no per-batch [[Chunk]].
   * `emit` is `inline` so HyperFuse sink spines can beta-reduce the
   * consumer into this loop through a monomorphic callback.
   */
  inline def foldEventsAcc(
    acc: Acc,
    events: Chunk[StreamingDecoded],
    maxMaterializedStreamBytes: ByteLimit,
    inline emit: Decoded => Unit
  ): Acc = {
    var s  = acc
    val it = events.iterator
    while it.hasNext do
      applyStep(s, it.next(), maxMaterializedStreamBytes) match {
        case Left(err) => throw err
        case Right((chunk, next)) =>
          val dit = chunk.iterator
          while dit.hasNext do emit(dit.next())
          s = next
      }
    s
  }

  /**
   * Synchronous fold with no ZPure interpreter — used by `PdfHyperdrive`.
   */
  def foldSync(
    acc: Acc,
    chunk: Chunk[StreamingDecoded],
    maxMaterializedStreamBytes: ByteLimit = StreamingDecode.Config.default.maxMaterializedStreamBytes
  ): (Chunk[Decoded], Acc) = {
    val builder = Chunk.newBuilder[Decoded]
    val next    = foldEventsAcc(acc, chunk, maxMaterializedStreamBytes, d => builder += d)
    (builder.result(), next)
  }

  /** Validate bridge state after the last streaming event (no open content payload). */
  def finalizeSync(acc: Acc): Chunk[Decoded] =
    if (acc.collect.nonEmpty) throw new IllegalStateException("EOF inside content stream payload")
    else Chunk.empty

  /**
   * Synchronous fold of one [[StreamingDecoded]] chunk; returns emitted
   * [[Decoded]] values and next [[Acc]].
   */
  def foldChunk(
    acc: Acc,
    chunk: Chunk[StreamingDecoded],
    maxMaterializedStreamBytes: ByteLimit = StreamingDecode.Config.default.maxMaterializedStreamBytes
  ): (Chunk[Decoded], Either[Throwable, Acc]) =
    try {
      val (decoded, next) = foldSync(acc, chunk, maxMaterializedStreamBytes)
      (decoded, Right(next))
    } catch {
      case err: Throwable => (Chunk.empty, Left(err))
    }

  /** Validate bridge state after the last streaming event. */
  def finalizeAcc(acc: Acc): Either[Throwable, Chunk[Decoded]] =
    try Right(finalizeSync(acc))
    catch { case err: Throwable => Left(err) }

  def pipeline(
    maxMaterializedStreamBytes: ByteLimit = StreamingDecode.Config.default.maxMaterializedStreamBytes
  ): ZPipeline[Any, Throwable, StreamingDecoded, Decoded] =
    StatefulPipe.fromSync[StreamingDecoded, Acc, Decoded](
      acc0,
      finalizeAcc,
      (acc, event) => applyStep(acc, event, maxMaterializedStreamBytes)
    )
}
