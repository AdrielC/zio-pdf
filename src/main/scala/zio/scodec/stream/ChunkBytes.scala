/*
 * Zero-copy [[zio.Chunk]] byte views as [[scodec.bits.BitVector]].
 */

package zio.scodec.stream

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.Chunk

object ChunkBytes {

  /** View a byte chunk as bits without copying when it is array-backed. */
  def toBitVector(chunk: Chunk[Byte]): BitVector =
    if (chunk.isEmpty) BitVector.empty
    else toBitVectorMaterialized(chunk.materialize)

  def toBitVectorChunk(chunk: Chunk[Byte]): Chunk[BitVector] =
    Chunk.single(toBitVector(chunk))

  private def toBitVectorMaterialized(chunk: Chunk[Byte]): BitVector =
    chunk match {
      case Chunk.ByteArray(arr, off, len) =>
        if (off == 0 && len == arr.length) BitVector.view(arr)
        else BitVector(ByteVector.view(arr, off, len))
      case _ =>
        BitVector.view(chunk.toArray)
    }
}
