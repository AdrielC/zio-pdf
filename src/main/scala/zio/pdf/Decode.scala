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
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.{Chunk, NonEmptyChunk}
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
    rawStream: _root_.scodec.bits.BitVector,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[Either[Xref, List[Decoded]]] =
    analyzeStream(index, data)(rawStream, Content.uncompress(rawStream, maxOutputBytes)(data))

  private def applyStep(s: State, ev: TopLevel): Either[Throwable, (Chunk[Decoded], State)] =
    ev match {
      case TopLevel.IndirectObjT(IndirectObj(Obj(index, data), Some(stream))) =>
        analyzeStream(index, data)(stream, Content.uncompress(stream)(data)) match {
          case Attempt.Successful(Right(decoded)) => Right((Chunk.fromIterable(decoded), s))
          case Attempt.Successful(Left(xref))      => Right((Chunk.empty, s.copy(xrefs = xref :: s.xrefs)))
          case Attempt.Failure(cause)             =>
            Left(new RuntimeException(s"extract stream objects: ${cause.messageWithContext}"))
        }
      // Keep the linearization dictionary in the decoded timeline. Earlier
      // versions treated it as encoder-only metadata, which made a genuine
      // `/Linearized` declaration invisible to composable preflight plans.
      case TopLevel.IndirectObjT(IndirectObj(obj @ Obj(_, Prim.Dict(d)), None)) if d.contains("Linearized") =>
        Right((Chunk.single(Decoded.DataObj(obj)), s))
      case TopLevel.IndirectObjT(IndirectObj(obj, None)) =>
        Right((Chunk.single(Decoded.DataObj(obj)), s))
      case TopLevel.VersionT(version) =>
        Right((Chunk.empty, s.copy(version = Some(version))))
      case TopLevel.XrefT(xref) =>
        Right((Chunk.empty, s.copy(xrefs = xref :: s.xrefs)))
      case TopLevel.StartXrefT(_) | TopLevel.CommentT(_) | TopLevel.WhitespaceT(_) =>
        Right((Chunk.empty, s))
    }

  private def finalizeSync(s: State): Either[Throwable, Chunk[Decoded]] = {
    val trailers  = s.xrefs.map(_.trailer)
    val sanitised = NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
    Right(Chunk.single(Decoded.Meta(s.xrefs, sanitised, s.version)))
  }

  /** Pipeline `TopLevel -> Decoded`, with a trailing `Meta` element. */
  val fromTopLevel: ZPipeline[Any, Throwable, TopLevel, Decoded] =
    StatefulPipe.fromSync[TopLevel, State, Decoded](initial, finalizeSync, applyStep)

  /** Full decoder pipeline `Byte -> Decoded`, including duplicate
    * filtering. */
  def apply(enableDiagnostics: Boolean = false): ZPipeline[Any, Throwable, Byte, Decoded] =
    TopLevel.pipe >>> FilterDuplicates.pipe(enableDiagnostics) >>> fromTopLevel
}
