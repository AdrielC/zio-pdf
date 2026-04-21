/*
 * Port of fs2.pdf.Decode to Scala 3 + ZIO.
 *
 * Walks a stream of TopLevel values:
 *   - data-only IndirectObj      -> Decoded.DataObj
 *   - IndirectObj with stream    -> Decoded.ContentObj (or, when the
 *                                   stream is an /ObjStm, fan out
 *                                   into multiple Decoded.DataObj)
 *   - Xref (textual)             -> accumulated, emitted as Meta at EOS
 *   - StartXref                  -> ignored (the trailing offset is
 *                                   already covered by the Xref)
 *   - Version                    -> remembered for the final Meta
 *   - Comment / Whitespace       -> ignored
 *
 * At end-of-stream a single Decoded.Meta is emitted with the
 * accumulated xrefs, the sanitised trailer, and the version.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.{Cause, Chunk, NonEmptyChunk}
import zio.stream.{ZChannel, ZPipeline}

object Decode {

  private final case class State(xrefs: List[Xref], version: Option[Version])

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

  private def topLevelOne(state: State, tl: TopLevel): (List[Decoded], State, Option[String]) =
    tl match {
      case TopLevel.IndirectObjT(IndirectObj(Obj(index, data), Some(stream))) =>
        analyzeStream(index, data)(stream, Content.uncompress(stream)(data)) match {
          case Attempt.Successful(Right(decoded)) => (decoded, state, None)
          case Attempt.Successful(Left(xref))     => (Nil, state.copy(xrefs = xref :: state.xrefs), None)
          case Attempt.Failure(cause)             => (Nil, state, Some(s"extract stream objects: ${cause.messageWithContext}"))
        }
      case TopLevel.IndirectObjT(IndirectObj(Obj(_, Prim.Dict(d)), None)) if d.contains("Linearized") =>
        // Skip linearization parameter dicts.
        (Nil, state, None)
      case TopLevel.IndirectObjT(IndirectObj(obj, None)) =>
        (List(Decoded.DataObj(obj)), state, None)
      case TopLevel.VersionT(version) =>
        (Nil, state.copy(version = Some(version)), None)
      case TopLevel.XrefT(xref) =>
        (Nil, state.copy(xrefs = xref :: state.xrefs), None)
      case TopLevel.StartXrefT(_) | TopLevel.CommentT(_) | TopLevel.WhitespaceT(_) =>
        (Nil, state, None)
    }

  private def loop(
    state: State
  ): ZChannel[Any, Throwable, Chunk[TopLevel], Any, Throwable, Chunk[Decoded], State] =
    ZChannel.readWithCause[Any, Throwable, Chunk[TopLevel], Any, Throwable, Chunk[Decoded], State](
      (chunk: Chunk[TopLevel]) => {
        var s   = state
        val out = Chunk.newBuilder[Decoded]
        var err: Option[String] = None
        chunk.foreach { tl =>
          if (err.isEmpty) {
            val (emitted, next, e) = topLevelOne(s, tl)
            emitted.foreach(out += _)
            s = next
            err = e
          }
        }
        err match {
          case Some(msg) => ZChannel.fail(new RuntimeException(msg))
          case None      => ZChannel.write(out.result()) *> loop(s)
        }
      },
      (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
      (_: Any) => ZChannel.succeed(state)
    )

  /** From accumulated state, build the final Meta and emit it. */
  private def emitMeta(s: State): ZChannel[Any, Any, Any, Any, Throwable, Chunk[Decoded], Unit] = {
    val xrefs    = s.xrefs
    val trailers = xrefs.map(_.trailer)
    val sanitised = NonEmptyChunk.fromIterableOption(trailers).map(Trailer.sanitize)
    ZChannel.write(Chunk.single(Decoded.Meta(xrefs, sanitised, s.version): Decoded))
  }

  /** Pipeline `TopLevel -> Decoded`, with a trailing `Meta` element. */
  val fromTopLevel: ZPipeline[Any, Throwable, TopLevel, Decoded] =
    ZPipeline.fromChannel(loop(State(Nil, None)).flatMap(emitMeta))

  /** Full decoder pipeline `Byte -> Decoded`, including duplicate
    * filtering. */
  def apply(log: Log = Log.noop): ZPipeline[Any, Throwable, Byte, Decoded] =
    TopLevel.pipe >>> FilterDuplicates.pipe(log) >>> fromTopLevel
}
