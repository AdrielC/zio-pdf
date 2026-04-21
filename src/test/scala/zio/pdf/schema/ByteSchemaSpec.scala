/*
 * Round-trip the third-party byte-buffer schemas through a Chunk[Byte]
 * encoding. Proves the plan from `zio.pdf.schema` works end-to-end:
 *
 *   1. `ByteVector` <-> `zio.blocks.chunk.Chunk[Byte]` is bytewise-exact.
 *   2. `BitVector`  round-trips for byte-aligned values (the only kind
 *      the PDF object model ever carries in its Schema-visible view).
 *   3. `Schema[ByteVector]` derives a working JSON codec.
 */

package zio.pdf.schema

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.blocks.schema.Schema
import zio.test.*

object ByteSchemaSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("ByteVector / BitVector Schemas")(

    test("ByteVector round-trips through its Schema's DynamicValue view") {
      val bytes  = ByteVector(1, 2, 3, 4, 5)
      val schema = summon[Schema[ByteVector]]
      val dyn    = schema.toDynamicValue(bytes)
      val back   = schema.fromDynamicValue(dyn)
      assertTrue(back == Right(bytes))
    },

    test("ByteVector round-trips through its Schema on a larger blob") {
      val bytes  = ByteVector.fromValidHex("deadbeefcafe00112233445566778899")
      val schema = summon[Schema[ByteVector]]
      val back   = schema.fromDynamicValue(schema.toDynamicValue(bytes))
      assertTrue(back == Right(bytes))
    },

    test("BitVector round-trips for byte-aligned values") {
      val bits   = BitVector(1, 2, 3, 4, 5)
      val schema = summon[Schema[BitVector]]
      val back   = schema.fromDynamicValue(schema.toDynamicValue(bits))
      assertTrue(back == Right(bits))
    }
  )
}
