package io.github.adrielc.kyopdf.pipeline

import java.nio.ByteBuffer
import zio.blocks.schema.{Platform, Schema}
import zio.blocks.schema.json.{Json, JsonCodecDeriver}
import zio.test.*

object NativeSchemaRuntimeSpec extends ZIOSpecDefault:
  private case class Payload(text: String, minimum: Long, maximum: Long, values: Vector[Int])
  private lazy val codec = Schema.derived[Payload].derive(JsonCodecDeriver)

  def spec = suite("Public Native Schema runtime")(
    test("reports Native without claiming JVM reflection or JavaScript") {
      assertTrue(Platform.name == "Native", !Platform.isJVM, !Platform.isJS, !Platform.supportsReflection)
    },
    test("derived codecs preserve Unicode, escapes, integer boundaries and buffers") {
      val original = Payload("Résumé \uD83D\uDCC4\n\"quoted\"\\", Long.MinValue, Long.MaxValue,
        Vector(Int.MinValue, 0, Int.MaxValue))
      val bytes = codec.encode(original)
      val buffer = ByteBuffer.allocate(bytes.length + 7)
      val _ = buffer.position(3)
      val _ = buffer.put(bytes)
      val _ = buffer.flip()
      val _ = buffer.position(3)
      val _ = buffer.limit(3 + bytes.length)
      assertTrue(codec.decode(bytes) == Right(original), codec.decode(buffer) == Right(original),
        codec.decode(codec.encodeToString(original)) == Right(original))
    },
    test("JSON transport preserves extreme finite doubles and rejects malformed input") {
      val values = Vector(Double.MinPositiveValue, Double.MaxValue, -0.0, 1.2345678901234567)
      val codec = Schema[Double].derive(JsonCodecDeriver)
      val restored = values.map(value => codec.decode(codec.encode(value)).toOption.get)
      assertTrue(values.map(java.lang.Double.doubleToRawLongBits) == restored.map(java.lang.Double.doubleToRawLongBits),
        Json.parse(Array[Byte](34, -64, -81, 34)).isLeft,
        Json.parse("{\"unfinished\":").isLeft)
    }
  )
