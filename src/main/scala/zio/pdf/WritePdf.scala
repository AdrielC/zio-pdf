/*
 * Port of fs2.pdf.WritePdf to Scala 3 + ZIO.
 *
 * The encoder pipeline turns a stream of Part[Trailer] values into
 * a stream of ByteVector chunks:
 *
 *   - the head of the stream may be a Part.Version (otherwise
 *     Version.default is prepended)
 *   - each Part.Obj is encoded; its byte size is recorded as
 *     XrefObjMeta in the running EncodeLog
 *   - exactly one Part.Meta(trailer) must appear somewhere in the
 *     stream
 *   - at end-of-stream, the xref/trailer/startxref triple is built
 *     by GenerateXref and written as the final ByteVector.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.{Cause, Chunk, NonEmptyChunk}
import zio.pdf.codec.Codecs
import zio.stream.{ZChannel, ZPipeline}

object WritePdf {

  private final case class EncodeLog(entries: List[XrefObjMeta], trailer: Option[Trailer]) {
    def entry(newEntry: XrefObjMeta): EncodeLog = copy(entries = newEntry :: entries)
  }

  private val emptyLog: EncodeLog = EncodeLog(Nil, None)

  /** Try to encode one Part. Returns the bytes to emit (possibly
    * empty) plus the updated log; on failure returns Left(message). */
  private def encodeOne(state: EncodeLog, part: Part[Trailer]): Either[String, (ByteVector, EncodeLog)] =
    part match {
      case Part.Obj(obj) =>
        EncodedObj.indirect(obj) match {
          case _root_.scodec.Attempt.Successful(EncodedObj(entry, bytes)) =>
            Right((bytes, state.entry(entry)))
          case _root_.scodec.Attempt.Failure(c) =>
            Left(s"encoding object ${obj.obj.index.number}: ${c.messageWithContext}")
        }
      case Part.Meta(trailer) =>
        Right((ByteVector.empty, state.copy(trailer = Some(trailer))))
      case Part.Version(_) =>
        Left("Part.Version not at the head of stream")
    }

  /**
   * Consume the version: if the first chunk leads with a
   * `Part.Version`, encode that version; otherwise prepend
   * `Version.default`. Returns the initial bytes plus the
   * remainder-stream-channel.
   */
  private def encodeVersion(part: Option[Part[Trailer]]): Either[String, (ByteVector, Option[Part[Trailer]])] =
    part match {
      case Some(Part.Version(version)) =>
        Codecs.encodeBytes(version)(using Version.codec) match {
          case _root_.scodec.Attempt.Successful(b) => Right((b, None))
          case _root_.scodec.Attempt.Failure(c)    => Left(s"encode version: ${c.messageWithContext}")
        }
      case other =>
        Codecs.encodeBytes(Version.default)(using Version.codec) match {
          case _root_.scodec.Attempt.Successful(b) => Right((b, other))
          case _root_.scodec.Attempt.Failure(c)    => Left(s"encode default version: ${c.messageWithContext}")
        }
    }

  /** Emit the final xref + trailer + startxref. */
  private def finishLog(
    initialOffset: Long
  )(log: EncodeLog): ZChannel[Any, Any, Any, Any, Throwable, Chunk[ByteVector], Unit] =
    log match {
      case EncodeLog(h :: t, Some(trailer)) =>
        val entries = NonEmptyChunk(h, t.reverse*)
        Codecs.encodeBytes(GenerateXref(entries, trailer, initialOffset))(using summon[_root_.scodec.Codec[Xref]]) match {
          case _root_.scodec.Attempt.Successful(bytes) => ZChannel.write(Chunk.single(bytes))
          case _root_.scodec.Attempt.Failure(c) =>
            ZChannel.fail(new RuntimeException(s"encoding xref: ${c.messageWithContext}"))
        }
      case EncodeLog(Nil, _) =>
        ZChannel.fail(new RuntimeException("no xref entries in parts stream"))
      case EncodeLog(_, None) =>
        ZChannel.fail(new RuntimeException("no trailer in parts stream"))
    }

  /**
   * Main encoder: `Part[Trailer]` -> `ByteVector` chunks (version
   * header, then encoded objects, then xref/trailer/startxref).
   *
   * NOTE: This implementation buffers the rest of the input
   * stream verbatim into the next downstream chunk after the
   * version header (so the loop sees it). For very large inputs,
   * a more sophisticated channel using `ZChannel.readWith` recursion
   * would stream rather than buffer per-chunk; for typical PDF
   * authoring workloads this is fine.
   */
  val parts: ZPipeline[Any, Throwable, Part[Trailer], ByteVector] =
    ZPipeline.fromChannel(streamingEncode)

  private def streamingEncode
      : ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {

    def initial: ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
        (chunk: Chunk[Part[Trailer]]) =>
          if (chunk.isEmpty) initial
          else {
            val first = chunk.head
            encodeVersion(Some(first)) match {
              case Right((vbytes, leftover)) =>
                val tail = leftover.fold[Chunk[Part[Trailer]]](chunk.drop(1))(p => p +: chunk.drop(1))
                ZChannel.write(Chunk.single(vbytes)) *> body(emptyLog, tail, vbytes.size.toLong)
              case Left(msg) =>
                ZChannel.fail(new RuntimeException(msg))
            }
          },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) =>
          encodeVersion(None) match {
            case Right((vbytes, _)) =>
              ZChannel.write(Chunk.single(vbytes)) *> finishEmpty(vbytes.size.toLong)
            case Left(msg) => ZChannel.fail(new RuntimeException(msg))
          }
      )

    def body(
      st: EncodeLog,
      pending: Chunk[Part[Trailer]],
      initialOffset: Long
    ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {
      var s   = st
      val out = Chunk.newBuilder[ByteVector]
      var err: Option[String] = None
      pending.foreach { p =>
        if (err.isEmpty) encodeOne(s, p) match {
          case Right((bytes, next)) =>
            s = next
            if (bytes.nonEmpty) out += bytes
          case Left(msg) =>
            err = Some(msg)
        }
      }
      err match {
        case Some(msg) => ZChannel.fail(new RuntimeException(msg))
        case None =>
          ZChannel.write(out.result()) *> readMore(s, initialOffset)
      }
    }

    def readMore(
      st: EncodeLog,
      initialOffset: Long
    ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
        (chunk: Chunk[Part[Trailer]]) => body(st, chunk, initialOffset),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => finalize(st, initialOffset)
      )

    def finalize(
      st: EncodeLog,
      initialOffset: Long
    ): ZChannel[Any, Throwable, Any, Any, Throwable, Chunk[ByteVector], Unit] =
      finishLog(initialOffset)(st)

    def finishEmpty(
      initialOffset: Long
    ): ZChannel[Any, Throwable, Any, Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.fail(new RuntimeException("no xref entries in parts stream"))

    initial
  }

  /** Convenience for raw indirect-object streams: every input is
    * wrapped as `Part.Obj`, and a single `Part.Meta(trailer)` is
    * appended at end-of-stream before the encoder runs. */
  def objects(trailer: Trailer): ZPipeline[Any, Throwable, IndirectObj, ByteVector] = {
    def loop: ZChannel[Any, Throwable, Chunk[IndirectObj], Any, Throwable, Chunk[Part[Trailer]], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[IndirectObj], Any, Throwable, Chunk[Part[Trailer]], Unit](
        (chunk: Chunk[IndirectObj]) =>
          ZChannel.write(chunk.map(Part.Obj(_): Part[Trailer])) *> loop,
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => ZChannel.write(Chunk.single(Part.Meta(trailer): Part[Trailer]))
      )
    ZPipeline.fromChannel(loop) >>> parts
  }
}
