/*
 * Port of fs2.pdf.StreamUtil — ZStream helpers replacing the legacy FS2 Pull layer.
 */

package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.stream.*

object StreamUtil {

  def attempt[A](message: String)(attempt: Attempt[A]): Task[A] =
    attempt.fold(
      e => ZIO.fail(new RuntimeException(s"$message: $e")),
      ZIO.succeed(_)
    )

  def attemptStream[A](message: String)(att: Attempt[A]): ZStream[Any, Throwable, A] =
    ZStream.fromZIO(attempt(message)(att))

  def bytes(b: ByteVector): ZStream[Any, Nothing, Byte] =
    ZStream.fromChunk(Chunk.fromArray(b.toArray))

  def bits(b: BitVector): ZStream[Any, Nothing, Byte] =
    bytes(b.bytes)

  val bytesPipe: ZPipeline[Any, Nothing, ByteVector, Byte] =
    ZPipeline.mapChunks(_.flatMap(bv => Chunk.fromArray(bv.toArray)))

  val bitsPipe: ZPipeline[Any, Nothing, BitVector, Byte] =
    ZPipeline.mapChunks(_.flatMap(bv => Chunk.fromArray(bv.bytes.toArray)))

  def string(s: String): ZStream[Any, Nothing, Byte] =
    ZStream.fromChunk(Chunk.fromArray(s.getBytes))
}
