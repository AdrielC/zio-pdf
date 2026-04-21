/*
 * Schema instances for the third-party byte-buffer types we can't
 * co-locate on their own companions: `scodec.bits.BitVector` and
 * `scodec.bits.ByteVector`.
 *
 * Everything else in the PDF object model lives in its own file with a
 * `given Schema[X] = Schema.derived[X]` in the companion. These two are
 * upstream types, so they land here once, shared across the model.
 *
 * ------------------------------------------------------------------
 * Encoding choice
 * ------------------------------------------------------------------
 *
 * Both `BitVector` and `ByteVector` round-trip through
 * `zio.blocks.chunk.Chunk[Byte]`:
 *
 *   BitVector  <--transform--> zio.blocks.chunk.Chunk[Byte]
 *   ByteVector <--transform--> zio.blocks.chunk.Chunk[Byte]
 *
 * Why bytes and not bits? Every codec-format we care about (JSON, CBOR,
 * Protobuf, Avro) models binary as byte sequences, and PDF payloads are
 * byte-aligned everywhere it matters. The few bit-aligned codecs in
 * `zio.pdf.codec` (Version header sub-fields and the like) don't need
 * to be SchemaExpr-navigable -- they live on the parsing boundary, not
 * in the object model.
 *
 * BitVector conversions are padded with zero bits to a byte boundary
 * when serialising (`toByteArray` behaviour). This is lossy *iff* a
 * stream encodes information in its trailing sub-byte, which the PDF
 * spec forbids: `/Length` always counts whole bytes, and every
 * decoder in this codebase pads to a byte boundary after decode. So
 * the round-trip is exact for every byte-aligned value, which is
 * every value we emit.
 *
 * If that assumption ever breaks the failure mode is loud (a
 * round-trip property test catches it), not silent.
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.blocks.chunk.Chunk as BlocksChunk
import zio.blocks.schema.Schema
import zio.blocks.typeid.TypeId

package object schema {

  /** `Schema[scodec.bits.ByteVector]` via `zio.blocks.chunk.Chunk[Byte]`.
    *
    * The chunk encoding is JSON-serialised as a numeric array by
    * zio-blocks-schema's default JSON codec; CBOR / Avro / Protobuf
    * derive a binary encoding automatically. */
  given byteVectorSchema: Schema[ByteVector] = {
    given TypeId[ByteVector] = TypeId.of[ByteVector]
    summon[Schema[BlocksChunk[Byte]]].transform[ByteVector](
      to   = (c: BlocksChunk[Byte]) => ByteVector.view(c.toArray),
      from = (b: ByteVector)        => BlocksChunk.fromArray(b.toArray)
    )
  }

  /** `Schema[scodec.bits.BitVector]` via `Schema[ByteVector]`.
    *
    * See the module-level comment for the byte-alignment caveat (it
    * doesn't apply to any value this library emits). */
  given bitVectorSchema: Schema[BitVector] = {
    given TypeId[BitVector] = TypeId.of[BitVector]
    byteVectorSchema.transform[BitVector](
      to   = (b: ByteVector) => b.toBitVector,
      from = (bv: BitVector) => bv.toByteVector
    )
  }
}
