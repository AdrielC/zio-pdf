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
import zio.{Cause, Chunk, NonEmptyChunk, Ref, ZIO}
import zio.pdf.codec.Codecs
import zio.stream.{ZChannel, ZPipeline}

object WritePdf {

  sealed abstract class Error(message: String) extends RuntimeException(message)

  /** The caller-supplied length for a streaming object did not match its bytes. */
  final case class StreamLengthMismatch(objectNumber: Long, declared: Long, actual: Long)
      extends Error(
        s"stream object $objectNumber declared /Length $declared but emitted $actual bytes"
      )

  final case class InvalidStreamLength(objectNumber: Long, declared: Long)
      extends Error(s"stream object $objectNumber declared a negative /Length: $declared")

  final case class StreamDictionaryRequired(objectNumber: Long)
      extends Error(s"stream object $objectNumber requires a dictionary so /Length can be encoded")

  final case class StreamLengthOverflow(objectNumber: Long, observed: Long, nextChunkBytes: Int)
      extends Error(
        s"stream object $objectNumber byte count overflowed Long after $observed bytes with a $nextChunkBytes-byte chunk"
      )

  final case class StreamHeaderEncodingFailed(objectNumber: Long, detail: String)
      extends Error(s"encoding stream-object header $objectNumber failed: $detail")

  private val OutputChunkBytes = 64 * 1024

  private[pdf] final case class EncodeLog(entries: List[XrefObjMeta], trailer: Option[Trailer]) {
    def entry(newEntry: XrefObjMeta): EncodeLog = copy(entries = newEntry :: entries)
  }

  private[pdf] val emptyLog: EncodeLog = EncodeLog(Nil, None)

  /** Try to encode the version header. */
  private[pdf] def encodeVersion(part: Option[Part[Trailer]]): Either[String, (ByteVector, Option[Part[Trailer]])] =
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
  private def encodeStreamHeader(index: Obj.Index, data: Prim, length: Long): Either[Error, ByteVector] =
    if length < 0L then Left(InvalidStreamLength(index.number, length))
    else
      data match
        case Prim.Dict(dictionary) =>
          val obj = Obj(index, Prim.Dict(dictionary.updated("Length", Prim.Number(BigDecimal(length)))))
          Codecs.encodeBytes(obj)(using IndirectObj.preStream) match
            case _root_.scodec.Attempt.Successful(headerBytes) =>
              Right(headerBytes ++ ByteVector("stream\n".getBytes))
            case _root_.scodec.Attempt.Failure(c) =>
              Left(StreamHeaderEncodingFailed(index.number, c.messageWithContext))
        case _ => Left(StreamDictionaryRequired(index.number))

  private val streamTrailer: ByteVector =
    ByteVector("\nendstream\nendobj\n".getBytes)

  /**
   * Count at ZStream chunk granularity and fail before `endstream` is emitted
   * when a caller supplied an incorrect `/Length`. The payload remains fully
   * streaming: the counter is one Long and no content chunk is retained.
   */
  private def checkedPayload(
    index: Obj.Index,
    declaredLength: Long,
    payload: zio.stream.ZStream[Any, Throwable, Byte]
  ): zio.stream.ZStream[Any, Throwable, Byte] =
    zio.stream.ZStream.unwrap {
      Ref.make(0L).map { emitted =>
        val forward = payload
          .rechunk(OutputChunkBytes)
          .chunks
          .mapZIO { chunk =>
            emitted.get.flatMap { actual =>
              val chunkBytes = chunk.size
              if actual > Long.MaxValue - chunkBytes.toLong then
                ZIO.fail(StreamLengthOverflow(index.number, actual, chunkBytes))
              else
                val next = actual + chunkBytes.toLong
                if next > declaredLength then ZIO.fail(StreamLengthMismatch(index.number, declaredLength, next))
                else emitted.set(next).as(chunk)
            }
          }
          .flattenChunks
        val verify = emitted.get.flatMap { actual =>
          if actual == declaredLength then ZIO.unit
          else ZIO.fail(StreamLengthMismatch(index.number, declaredLength, actual))
        }
        forward ++ zio.stream.ZStream.fromZIO(verify).drain
      }
    }

  /** Encode a non-streaming Part.Obj. */
  private def encodeObj(state: EncodeLog, obj: IndirectObj): Either[String, (ByteVector, EncodeLog)] =
    EncodedObj.indirect(obj) match {
      case _root_.scodec.Attempt.Successful(EncodedObj(entry, bytes)) =>
        Right((bytes, state.entry(entry)))
      case _root_.scodec.Attempt.Failure(c) =>
        Left(s"encoding object ${obj.obj.index.number}: ${c.messageWithContext}")
    }

  /** Emit the final xref + trailer + startxref. */
  private[pdf] def finishLog(
    initialOffset: Long
  )(log: EncodeLog): ZChannel[Any, Any, Any, Any, Throwable, Chunk[ByteVector], Unit] =
    log match {
      case EncodeLog(h :: t, Some(trailer)) =>
        val physicalOrder = (h :: t).reverse
        val entries       = NonEmptyChunk(physicalOrder.head, physicalOrder.tail*)
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
  private[pdf] def emitPart(
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
          case Left(error) =>
            ZChannel.fail(error)
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
              checkedPayload(index, length, payload).channel.mapOut(c => Chunk.single(ByteVector.view(c.toArray)))
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

  /**
   * Encode a part stream that has already emitted its version header.
   *
   * `initialPending` lets a caller which had to inspect a prefix (notably the
   * linearized writer) continue with the exact remainder of the same upstream
   * chunk. `terminalTrailer` wins over any earlier metadata when the stream
   * ends, which is how a linearized tail writes its final trailer.
   */
  private[pdf] def tailEncoder(
    initialOffset: Long,
    initialPending: Chunk[Part[Trailer]] = Chunk.empty,
    terminalTrailer: Option[Trailer] = None
  ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {

    def processChunk(
      st: EncodeLog,
      pending: Chunk[Part[Trailer]]
    ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {
      def goOne(
        state: EncodeLog,
        index: Int
      ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
        if index >= pending.size then readMore(state)
        else emitPart(state, pending(index)).flatMap(next => goOne(next, index + 1))
      goOne(st, 0)
    }

    def readMore(
      st: EncodeLog
    ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
        (chunk: Chunk[Part[Trailer]]) => processChunk(st, chunk),
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) => finishLog(initialOffset)(terminalTrailer.fold(st)(trailer => st.copy(trailer = Some(trailer))))
      )

    processChunk(emptyLog, initialPending)
  }

  private def streamingEncode
      : ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {

    def initial: ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
        (chunk: Chunk[Part[Trailer]]) =>
          if chunk.isEmpty then initial
          else {
            encodeVersion(Some(chunk.head)) match {
              case Right((bytes, leftover)) =>
                val tail = leftover.fold[Chunk[Part[Trailer]]](chunk.drop(1))(part => part +: chunk.drop(1))
                ZChannel.write(Chunk.single(bytes)) *> tailEncoder(bytes.size.toLong, tail)
              case Left(message) => ZChannel.fail(new RuntimeException(message))
            }
          },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any) =>
          encodeVersion(None) match {
            case Right((bytes, _)) =>
              ZChannel.write(Chunk.single(bytes)) *> tailEncoder(bytes.size.toLong)
            case Left(message) => ZChannel.fail(new RuntimeException(message))
          }
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
