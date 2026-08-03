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

  /** Mutable bridge state between [[StreamingDecoded]] chunks and [[Decoded]] output. */
  final case class Acc(
    collect: Option[(Obj, Chunk[Byte])],
    embeddedXrefs: List[Xref]
  )

  val accInitial: Acc = Acc(None, Nil)

  private val acc0: Acc = accInitial

  private def fromAttempt[A](a: Attempt[A]): Either[Throwable, A] =
    a match {
      case Attempt.Successful(v) => Right(v)
      case Attempt.Failure(c)    => Left(new RuntimeException(c.messageWithContext))
    }

  /**
   * Core imperative step — shared by [[foldSync]] and the streaming
   * [[pipeline]] (via ZPure wrapper).
   */
  private[pdf] def applyStep(s: Acc, ev: StreamingDecoded): Either[Throwable, (Chunk[Decoded], Acc)] =
    ev match {
      case m: StreamingDecoded.Meta =>
        val mergedXrefs = m.xrefs ++ s.embeddedXrefs.reverse
        val trailers    = mergedXrefs.map(_.trailer)
        val sanitised   = zio.NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
        Right((Chunk.single(Decoded.Meta(mergedXrefs, sanitised, m.version)), acc0))

      case StreamingDecoded.DataObj(obj) =>
        Right((Chunk.single(Decoded.DataObj(obj)), s))

      case StreamingDecoded.VersionT(_) | _: StreamingDecoded.CommentT |
          _: StreamingDecoded.StartXrefT | _: StreamingDecoded.XrefT =>
        Right((Chunk.empty, s))

      case StreamingDecoded.ContentObjStart(obj, _, Some(bits)) =>
        fromAttempt(Decode.expandStreamPayload(obj.index, obj.data, bits)).map {
          case Left(xref)  => (Chunk.empty, s.copy(embeddedXrefs = xref :: s.embeddedXrefs))
          case Right(rows) => (Chunk.fromIterable(rows), s)
        }

      case StreamingDecoded.ContentObjStart(obj, _, None) =>
        s.collect match {
          case None    => Right((Chunk.empty, s.copy(collect = Some((obj, Chunk.empty)))))
          case Some(_) => Left(new IllegalStateException("nested ContentObjStart"))
        }

      case StreamingDecoded.ContentObjBytes(c) =>
        s.collect match {
          case Some((obj, buf)) => Right((Chunk.empty, s.copy(collect = Some((obj, buf ++ c)))))
          case None             => Left(new IllegalStateException("ContentObjBytes without ContentObjStart"))
        }

      case StreamingDecoded.ContentObjEnd =>
        s.collect match {
          case Some((obj, buf)) =>
            val bits = _root_.scodec.bits.BitVector(buf.toArray)
            fromAttempt(Decode.expandStreamPayload(obj.index, obj.data, bits)).map {
              case Left(xref)  => (Chunk.empty, Acc(None, xref :: s.embeddedXrefs))
              case Right(rows) => (Chunk.fromIterable(rows), Acc(None, s.embeddedXrefs))
            }
          case None =>
            Left(new IllegalStateException("ContentObjEnd without start"))
        }
    }

  /**
   * Synchronous fold with no ZPure interpreter — used by [[zio.pdf.PdfHyperdrive]].
   */
  def foldSync(acc: Acc, chunk: Chunk[StreamingDecoded]): (Chunk[Decoded], Acc) = {
    val builder = Chunk.newBuilder[Decoded]
    var s       = acc
    val it      = chunk.iterator
    while it.hasNext do
      applyStep(s, it.next()) match {
        case Left(err)         => throw err
        case Right((out, next)) =>
          builder ++= out
          s = next
      }
    (builder.result(), s)
  }

  /** Validate bridge state after the last streaming event (no open content payload). */
  def finalizeSync(acc: Acc): Chunk[Decoded] =
    if (acc.collect.nonEmpty) throw new IllegalStateException("EOF inside content stream payload")
    else Chunk.empty

  /**
   * Synchronous fold of one [[StreamingDecoded]] chunk; returns emitted
   * [[Decoded]] values and next [[Acc]].
   */
  def foldChunk(acc: Acc, chunk: Chunk[StreamingDecoded]): (Chunk[Decoded], Either[Throwable, Acc]) =
    try {
      val (decoded, next) = foldSync(acc, chunk)
      (decoded, Right(next))
    } catch {
      case err: Throwable => (Chunk.empty, Left(err))
    }

  /** Validate bridge state after the last streaming event. */
  def finalizeAcc(acc: Acc): Either[Throwable, Chunk[Decoded]] =
    try Right(finalizeSync(acc))
    catch { case err: Throwable => Left(err) }

  val pipeline: ZPipeline[Any, Throwable, StreamingDecoded, Decoded] =
    StatefulPipe.fromSync[StreamingDecoded, Acc, Decoded](
      acc0,
      finalizeAcc,
      applyStep
    )
}
