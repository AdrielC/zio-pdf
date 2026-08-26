/*
 * Zero-copy [[zio.Chunk]] byte views as [[scodec.bits.BitVector]].
 */

package zio.scodec.stream

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.Chunk

object ChunkBytes {

  private val CopyStepBytes = 64 * 1024

  /** View a byte chunk as bits without copying when it is array-backed. */
  def toBitVector(chunk: Chunk[Byte]): BitVector =
    if (chunk.isEmpty) BitVector.empty
    else
      chunk match {
        case Chunk.ByteArray(arr, off, len) =>
          if (off == 0 && len == arr.length) BitVector.view(arr)
          else BitVector(ByteVector.view(arr, off, len))
        case _ =>
          var output = BitVector.empty
          var from   = 0
          while (from < chunk.size) {
            val length = math.min(CopyStepBytes, chunk.size - from)
            val bytes  = new Array[Byte](length)
            var index  = 0
            while (index < length) {
              bytes(index) = chunk(from + index)
              index += 1
            }
            output = output ++ BitVector.view(bytes)
            from += length
          }
          output
      }

  def toBitVectorChunk(chunk: Chunk[Byte]): Chunk[BitVector] =
    Chunk.single(toBitVector(chunk))

}
