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
 *   - each Part.StreamObj writes its header, then forwards its
 *     payload ZStream chunk-by-chunk (memory-bounded, never
 *     materialises the payload), then writes its trailer
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

  /** Try to encode the version header. */
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

  /** Encode the header of a streaming object: `<num> <gen> obj` +
    * dict (with `/Length` patched in) + `stream\n`. */
  private def encodeStreamHeader(index: Obj.Index, data: Prim, length: Long): Either[String, ByteVector] = {
    // Patch /Length into the dict.
    val patched: Prim = data match {
      case Prim.Dict(d) => Prim.Dict(d.updated("Length", Prim.Number(BigDecimal(length))))
      case other        => other
    }
    val obj = Obj(index, patched)
    // Encode <obj header> + <prim> by reusing IndirectObjCodec's preStream codec
    // and then appending the literal `stream\n`.
    Codecs.encodeBytes(obj)(using IndirectObj.preStream) match {
      case _root_.scodec.Attempt.Successful(headerBytes) =>
        Right(headerBytes ++ ByteVector("stream\n".getBytes))
      case _root_.scodec.Attempt.Failure(c) =>
        Left(s"encoding stream-object header ${index.number}: ${c.messageWithContext}")
    }
  }

  private val streamTrailer: ByteVector =
    ByteVector("\nendstream\nendobj\n".getBytes)

  /** Encode a non-streaming Part.Obj. */
  private def encodeObj(state: EncodeLog, obj: IndirectObj): Either[String, (ByteVector, EncodeLog)] =
    EncodedObj.indirect(obj) match {
      case _root_.scodec.Attempt.Successful(EncodedObj(entry, bytes)) =>
        Right((bytes, state.entry(entry)))
      case _root_.scodec.Attempt.Failure(c) =>
        Left(s"encoding object ${obj.obj.index.number}: ${c.messageWithContext}")
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
   * Per-Part processing: for each Part, return a channel that
   * emits its bytes (possibly streaming) and yields the updated
   * EncodeLog. Side-effecting channels are needed because
   * Part.StreamObj forwards a ZStream chunk-by-chunk.
   */
  private def emitPart(
    st: EncodeLog,
    part: Part[Trailer]
  ): ZChannel[Any, Throwable, Any, Any, Throwable, Chunk[ByteVector], EncodeLog] =
    part match {
      case Part.Obj(obj) =>
        encodeObj(st, obj) match {
          case Right((bytes, next)) =>
            (if (bytes.isEmpty) ZChannel.unit else ZChannel.write(Chunk.single(bytes))) *>
              ZChannel.succeed(next)
          case Left(msg) =>
            ZChannel.fail(new RuntimeException(msg))
        }

      case Part.StreamObj(index, data, length, payload) =>
        encodeStreamHeader(index, data, length) match {
          case Left(msg) =>
            ZChannel.fail(new RuntimeException(msg))
          case Right(header) =>
            // The header + the streamed payload + the trailer all go
            // into the byte count for this object's xref entry.
            val totalSize = header.size + length + streamTrailer.size
            val nextLog   = st.entry(XrefObjMeta(index, totalSize))
            // Convert the payload ZStream into a sub-channel that
            // writes Chunk[ByteVector] chunks downstream and finishes
            // with Unit. Then sandwich it between the header and the
            // trailer. Memory bounded: at most one upstream chunk
            // lives at a time.
            val forward
                : ZChannel[Any, Any, Any, Any, Throwable, Chunk[ByteVector], Any] =
              payload.channel.mapOut(c => Chunk.single(ByteVector.view(c.toArray)))
            ZChannel.write(Chunk.single(header)) *>
              forward.unit *>
              ZChannel.write(Chunk.single(streamTrailer)) *>
              ZChannel.succeed(nextLog)
        }

      case Part.Meta(trailer) =>
        ZChannel.succeed(st.copy(trailer = Some(trailer)))

      case Part.Version(_) =>
        ZChannel.fail(new RuntimeException("Part.Version not at the head of stream"))
    }

  /**
   * Main encoder: `Part[Trailer]` -> `ByteVector` chunks (version
   * header, then encoded objects, then xref/trailer/startxref).
   */
  val parts: ZPipeline[Any, Throwable, Part[Trailer], ByteVector] =
    ZPipeline.fromChannel(streamingEncode)

  private def streamingEncode
      : ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {

    val failNoEntries
        : ZChannel[Any, Throwable, Any, Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.fail(new RuntimeException("no xref entries in parts stream"))

    def initial: ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
        (chunk: Chunk[Part[Trailer]]) =>
          if (chunk.isEmpty) initial
          else {
            val first = chunk.head
            encodeVersion(Some(first)) match {
              case Right((vbytes, leftover)) =>
                val tail = leftover.fold[Chunk[Part[Trailer]]](chunk.drop(1))(p => p +: chunk.drop(1))
                ZChannel.write(Chunk.single(vbytes)) *> processChunk(emptyLog, tail, vbytes.size.toLong)
              case Left(msg) =>
                ZChannel.fail(new RuntimeException(msg))
            }
          },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) =>
          encodeVersion(None) match {
            case Right((vbytes, _)) =>
              ZChannel.write(Chunk.single(vbytes)) *> failNoEntries
            case Left(msg) => ZChannel.fail(new RuntimeException(msg))
          }
      )

    def processChunk(
      st: EncodeLog,
      pending: Chunk[Part[Trailer]],
      initialOffset: Long
    ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {
      // Process Parts one at a time so Part.StreamObj can interleave
      // its own writes between the header and the trailer.
      def goOne(
        s: EncodeLog,
        idx: Int
      ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
        if (idx >= pending.size) readMore(s, initialOffset)
        else
          emitPart(s, pending(idx)).flatMap(next => goOne(next, idx + 1))
      goOne(st, 0)
    }

    def readMore(
      st: EncodeLog,
      initialOffset: Long
    ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
        (chunk: Chunk[Part[Trailer]]) => processChunk(st, chunk, initialOffset),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => finishLog(initialOffset)(st)
      )

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
