/*
 * Port of fs2.pdf.Decode to Scala 3 + ZIO.
 *
 * The per-element pipe is a pure state transition:
 *   - data-only IndirectObj      -> Decoded.DataObj
 *   - IndirectObj with stream    -> Decoded.ContentObj (or, when
 *                                   the stream is /Type /ObjStm,
 *                                   fan out into multiple DataObjs)
 *   - Xref (textual)             -> accumulated, emitted as Meta at EOS
 *   - StartXref / Comment / WS   -> ignored
 *   - Version                    -> remembered for the final Meta
 *
 * That's textbook ZPure - state plus log emissions plus a fail
 * signal - so we use `StatefulPipe` and let the ZChannel layer
 * stay one line.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.NonEmptyChunk
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

object Decode {

  private final case class State(xrefs: List[Xref], version: Option[Version])

  private val initial: State = State(Nil, None)

  private def decodeObjectStream(stream: Uncompressed)(data: Prim): Option[Attempt[List[Decoded]]] =
    Content.extractObjectStream(stream)(data).map(_.map(_.objs.map(Decoded.DataObj(_))))

  private def extractMetadata(stream: Uncompressed): Prim => Option[Attempt[Either[Xref, List[Decoded]]]] = {
    case Prim.tpe("XRef", data) =>
      Some(stream.exec.flatMap(XrefStream(data)).map { xs =>
        Left(Xref(xs.tables, xs.trailer, StartXref(0L)))
      })
    case _ =>
      None
  }

  private def analyzeStream(
    index: Obj.Index,
    data: Prim
  )(rawStream: _root_.scodec.bits.BitVector, stream: Uncompressed): Attempt[Either[Xref, List[Decoded]]] =
    decodeObjectStream(stream)(data) match {
      case Some(att) => att.map(Right(_))
      case None =>
        extractMetadata(stream)(data) match {
          case Some(att) => att
          case None      => Attempt.successful(Right(List(Decoded.ContentObj(Obj(index, data), rawStream, stream))))
        }
    }

  /** Expand a raw (encoded) content stream into xref updates or
    * `Decoded` rows — shared by [[apply]] and [[DecodedFromStreaming]]. */
  private[pdf] def expandStreamPayload(
    index: Obj.Index,
    data: Prim,
    rawStream: _root_.scodec.bits.BitVector
  ): Attempt[Either[Xref, List[Decoded]]] =
    analyzeStream(index, data)(rawStream, Content.uncompress(rawStream)(data))

  /** Pure step: convert one TopLevel into zero-or-more Decoded
    * outputs, threading the xref / version accumulator. */
  private val step: StatefulPipe.Step[TopLevel, State, Decoded] = {

    def emit(d: Decoded): ZPure[Decoded, State, State, Any, Throwable, Unit] =
      ZPure.log[State, Decoded](d)

    def emits(ds: List[Decoded]): ZPure[Decoded, State, State, Any, Throwable, Unit] =
      ds.foldLeft[ZPure[Decoded, State, State, Any, Throwable, Unit]](ZPure.unit)(
        (acc, d) => acc *> emit(d)
      )

    {
      case TopLevel.IndirectObjT(IndirectObj(Obj(index, data), Some(stream))) =>
        analyzeStream(index, data)(stream, Content.uncompress(stream)(data)) match {
          case Attempt.Successful(Right(decoded)) => emits(decoded)
          case Attempt.Successful(Left(xref))     =>
            ZPure.update[State, State](s => s.copy(xrefs = xref :: s.xrefs))
          case Attempt.Failure(cause) =>
            ZPure.fail(new RuntimeException(s"extract stream objects: ${cause.messageWithContext}"))
        }
      case TopLevel.IndirectObjT(IndirectObj(Obj(_, Prim.Dict(d)), None)) if d.contains("Linearized") =>
        // Skip linearization parameter dicts.
        ZPure.unit
      case TopLevel.IndirectObjT(IndirectObj(obj, None)) =>
        emit(Decoded.DataObj(obj))
      case TopLevel.VersionT(version) =>
        ZPure.update[State, State](s => s.copy(version = Some(version)))
      case TopLevel.XrefT(xref) =>
        ZPure.update[State, State](s => s.copy(xrefs = xref :: s.xrefs))
      case TopLevel.StartXrefT(_) | TopLevel.CommentT(_) | TopLevel.WhitespaceT(_) =>
        ZPure.unit
    }
  }

  /** Pure finalizer: emit a single Meta record with the accumulated
    * xrefs, the sanitised trailer, and the version. */
  private val finalize: State => ZPure[Decoded, State, State, Any, Throwable, Unit] = s => {
    val trailers  = s.xrefs.map(_.trailer)
    val sanitised = NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
    ZPure.log[State, Decoded](Decoded.Meta(s.xrefs, sanitised, s.version))
  }

  /** Pipeline `TopLevel -> Decoded`, with a trailing `Meta` element. */
  val fromTopLevel: ZPipeline[Any, Throwable, TopLevel, Decoded] =
    StatefulPipe[TopLevel, State, Decoded](initial, finalize)(step)

  /** Full decoder pipeline `Byte -> Decoded`, including duplicate
    * filtering. */
  def apply(log: Log = Log.noop): ZPipeline[Any, Throwable, Byte, Decoded] =
    TopLevel.pipe >>> FilterDuplicates.pipe(log) >>> fromTopLevel
}
