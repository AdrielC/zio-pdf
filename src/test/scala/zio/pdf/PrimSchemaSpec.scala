/*
 * Schema round-trip on the full Prim ADT via the derived JSON codec.
 *
 * This is the keystone test: if the whole `Prim` ADT round-trips
 * through `JsonCodec.encodeToString` / `JsonCodec.decode(String)`, the
 * downstream PDF object model will too (it's all `Prim` underneath).
 *
 * We deliberately do NOT go through `DynamicValue` -- that's a two-hop
 * journey (value -> DynamicValue -> format) that costs extra
 * allocations and reflects nothing about the actual wire path.
 * `jsonCodec.encodeToString(v) |> decode` is the *one* path every
 * service will use on the wire, so that's the path the test covers.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.blocks.chunk.{Chunk as BlocksChunk, ChunkMap}
import zio.blocks.schema.Schema
import zio.test.*

object PrimSchemaSpec extends ZIOSpecDefault {

  /** A Dict covering every single `Prim` constructor. One round-trip
    * call exercises all nine. */
  private val everyConstructor: Prim =
    Prim.Dict(ChunkMap.from(Seq(
      "Type"       -> Prim.Name("Catalog"),
      "Version"    -> Prim.Number(BigDecimal("1.7")),
      "Linearized" -> Prim.Bool(false),
      "Nullable"   -> Prim.Null,
      "Root"       -> Prim.Ref(1, 0),
      "Title"      -> Prim.Str(ByteVector("The Trial".getBytes)),
      "ID"         -> Prim.HexStr(ByteVector.fromValidHex("deadbeefcafebabe")),
      "Pages"      -> Prim.Array(BlocksChunk(
        Prim.Ref(2, 0),
        Prim.Number(BigDecimal(42)),
        Prim.Name("Page")
      )),
      "Nested"     -> Prim.Dict(ChunkMap.from(Seq(
        "Leaf"      -> Prim.Number(BigDecimal(-1)),
        "Empty"     -> Prim.Array(BlocksChunk.empty[Prim]),
        "EmptyDict" -> Prim.Dict(ChunkMap.empty[String, Prim])
      )))
    )))

  private val primCodec = summon[Schema[Prim]].jsonCodec

  private def roundTrip(value: Prim): Either[String, Prim] =
    primCodec.decode(primCodec.encodeToString(value)).left.map(_.toString)

  def spec: Spec[Any, Throwable] = suite("Prim Schema")(

    test("summon[Schema[Prim]] is non-null and recognisable") {
      val s = summon[Schema[Prim]]
      assertTrue(s ne null) && assertTrue(s.toString.contains("Prim"))
    },

    test("every-constructor Dict round-trips through JSON") {
      val back = roundTrip(everyConstructor)
      assertTrue(back == Right(everyConstructor))
    },

    test("every individual Prim constructor round-trips through JSON") {
      val cases: List[Prim] = List(
        Prim.Null,
        Prim.Ref(7, 1),
        Prim.Bool(true),
        Prim.Bool(false),
        Prim.Number(BigDecimal("0")),
        Prim.Number(BigDecimal("3.14159265358979")),
        Prim.Name("Font"),
        Prim.Str(ByteVector.empty),
        Prim.Str(ByteVector("hello world".getBytes)),
        Prim.HexStr(ByteVector.fromValidHex("00ff")),
        Prim.Array(BlocksChunk.empty[Prim]),
        Prim.Array(BlocksChunk(Prim.Null, Prim.Number(BigDecimal(1)))),
        Prim.Dict(ChunkMap.empty[String, Prim]),
        Prim.Dict(ChunkMap("k" -> Prim.Name("v")))
      )
      val results  = cases.map(p => p -> roundTrip(p))
      val failures = results.collect { case (orig, Right(back)) if back != orig => (orig, back) }
      val errors   = results.collect { case (orig, Left(err))                     => (orig, err) }
      assertTrue(failures.isEmpty) && assertTrue(errors.isEmpty)
    },

    test("Dict key order survives JSON round-trip") {
      // ChunkMap preserves insertion order. The JSON codec should keep
      // it. That matters for evidence chain-of-custody: re-serialising
      // an ingested PDF must not silently reorder dict keys.
      val d = Prim.Dict(ChunkMap.from(Seq(
        "Zeta"  -> Prim.Number(BigDecimal(1)),
        "Alpha" -> Prim.Number(BigDecimal(2)),
        "Mu"    -> Prim.Number(BigDecimal(3))
      )))
      val back = primCodec.decode(primCodec.encodeToString(d))
      assertTrue(
        back.isRight,
        back.toOption.flatMap {
          case Prim.Dict(m) => Some(m.keys.toList)
          case _            => None
        } == Some(List("Zeta", "Alpha", "Mu"))
      )
    }
  )
}
