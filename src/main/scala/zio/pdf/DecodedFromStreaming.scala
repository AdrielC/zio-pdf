/*
 * Bridge [[StreamingDecoded]] → [[Decoded]] using [[StatefulPipe]].
 * Embeds xrefs from expanded stream payloads (Left branch of
 * [[Decode.expandStreamPayload]]) into [[Decoded.Meta]] after the
 * textual xrefs carried on [[StreamingDecoded.Meta]].
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.Chunk
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

object DecodedFromStreaming {

  private final case class Acc(
    collect: Option[(Obj, Chunk[Byte])],
    embeddedXrefs: List[Xref]
  )

  private val acc0: Acc = Acc(None, Nil)

  private def fromAttempt[A](a: Attempt[A]): ZPure[Decoded, Acc, Acc, Any, Throwable, A] =
    a match {
      case Attempt.Successful(v) => ZPure.succeed(v)
      case Attempt.Failure(c)    => ZPure.fail(new RuntimeException(c.messageWithContext))
    }

  private def emitDecoded(d: Decoded): ZPure[Decoded, Acc, Acc, Any, Throwable, Unit] =
    ZPure.log[Acc, Decoded](d)

  private def emitsDecoded(ds: List[Decoded]): ZPure[Decoded, Acc, Acc, Any, Throwable, Unit] =
    ds.foldLeft[ZPure[Decoded, Acc, Acc, Any, Throwable, Unit]](ZPure.unit)((acc, d) => acc *> emitDecoded(d))

  private val step: StatefulPipe.Step[StreamingDecoded, Acc, Decoded] = {
    case m: StreamingDecoded.Meta =>
      ZPure.get[Acc].flatMap { s =>
        val mergedXrefs = m.xrefs ++ s.embeddedXrefs.reverse
        val trailers    = mergedXrefs.map(_.trailer)
        val sanitised   = zio.NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
        ZPure.set(acc0) *> emitDecoded(Decoded.Meta(mergedXrefs, sanitised, m.version))
      }
    case StreamingDecoded.DataObj(obj) =>
      emitDecoded(Decoded.DataObj(obj))
    case StreamingDecoded.VersionT(_) | _: StreamingDecoded.CommentT |
        _: StreamingDecoded.StartXrefT | _: StreamingDecoded.XrefT =>
      ZPure.unit
    case StreamingDecoded.ContentObjStart(obj, _, Some(bits)) =>
      fromAttempt(Decode.expandStreamPayload(obj.index, obj.data, bits)).flatMap {
        case Left(xref)  => ZPure.update[Acc, Acc](a => a.copy(embeddedXrefs = xref :: a.embeddedXrefs))
        case Right(rows) => emitsDecoded(rows)
      }
    case StreamingDecoded.ContentObjStart(obj, _, None) =>
      ZPure.get[Acc].flatMap {
        case s @ Acc(None, _) =>
          ZPure.set(s.copy(collect = Some((obj, Chunk.empty))))
        case _ =>
          ZPure.fail(new IllegalStateException("nested ContentObjStart"))
      }
    case StreamingDecoded.ContentObjBytes(c) =>
      ZPure.get[Acc].flatMap {
        case Acc(Some((obj, buf)), emb) =>
          ZPure.set(Acc(Some((obj, buf ++ c)), emb))
        case _ =>
          ZPure.fail(new IllegalStateException("ContentObjBytes without ContentObjStart"))
      }
    case StreamingDecoded.ContentObjEnd =>
      ZPure.get[Acc].flatMap {
        case Acc(Some((obj, buf)), emb) =>
          val bits = _root_.scodec.bits.BitVector(buf.toArray)
          fromAttempt(Decode.expandStreamPayload(obj.index, obj.data, bits)).flatMap {
            case Left(xref) =>
              ZPure.set(Acc(None, xref :: emb))
            case Right(rows) =>
              ZPure.set(Acc(None, emb)) *> emitsDecoded(rows)
          }
        case _ =>
          ZPure.fail(new IllegalStateException("ContentObjEnd without start"))
      }
  }

  private val finalize: Acc => ZPure[Decoded, Acc, Acc, Any, Throwable, Unit] = { s =>
    if (s.collect.nonEmpty) ZPure.fail(new IllegalStateException("EOF inside content stream payload"))
    else ZPure.unit
  }

  val pipeline: ZPipeline[Any, Throwable, StreamingDecoded, Decoded] =
    StatefulPipe[StreamingDecoded, Acc, Decoded](acc0, finalize)(step)
}
