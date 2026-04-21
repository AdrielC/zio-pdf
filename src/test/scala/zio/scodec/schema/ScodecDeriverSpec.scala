/*
 * End-to-end test for ScodecDeriver: derive a `Schema[A]` via
 * `Schema.derived[A]`, then derive a `scodec.Codec[A]` from it via
 * `schema.derive(ScodecDeriver)`, and round-trip values through the
 * resulting binary codec. This is the architectural payoff of
 * wiring zio-blocks-schema in: one `Schema.derived` declaration
 * gives us the binary codec without writing any per-type
 * Codec[Foo] instance by hand.
 */

package zio.scodec.schema

import _root_.scodec.bits.BitVector
import zio.blocks.schema.Schema
import zio.test.*

object ScodecDeriverSpec extends ZIOSpecDefault {

  // ---- Records (nested) ---------------------------------------------------

  final case class Address(street: String, zip: Int)
  object Address {
    given Schema[Address] = Schema.derived[Address]
  }

  final case class Person(name: String, age: Int, address: Address)
  object Person {
    given Schema[Person] = Schema.derived[Person]
  }

  // ---- Variant ------------------------------------------------------------

  sealed trait Shape
  final case class Circle(radius: Double)                   extends Shape
  final case class Rectangle(width: Double, height: Double) extends Shape
  final case class Triangle(a: Double, b: Double, c: Double) extends Shape
  object Shape {
    given Schema[Shape] = Schema.derived[Shape]
  }

  // ---- Record with sequence ----------------------------------------------

  final case class Team(name: String, members: List[String])
  object Team {
    given Schema[Team] = Schema.derived[Team]
  }

  def spec: Spec[Any, Throwable] = suite("ScodecDeriver - schema-derived scodec.Codec")(

    test("Codec[Address] round-trips") {
      val codec   = summon[Schema[Address]].derive(ScodecDeriver)
      val a       = Address("123 Main", 90210)
      val encoded = codec.encode(a).require
      val decoded = codec.decode(encoded).require
      assertTrue(decoded.value == a, decoded.remainder == BitVector.empty)
    },

    test("Codec[Person] round-trips a nested record") {
      val codec   = summon[Schema[Person]].derive(ScodecDeriver)
      val p       = Person("Alice", 30, Address("1 Wonderland Way", 12345))
      val encoded = codec.encode(p).require
      val decoded = codec.decode(encoded).require
      assertTrue(decoded.value == p, decoded.remainder == BitVector.empty)
    },

    test("Codec[Shape] round-trips each variant case") {
      val codec = summon[Schema[Shape]].derive(ScodecDeriver)
      val shapes: List[Shape] = List(
        Circle(5.0),
        Rectangle(3.0, 4.0),
        Triangle(3.0, 4.0, 5.0)
      )
      val results = shapes.map { s =>
        val bits = codec.encode(s).require
        codec.decode(bits).require.value
      }
      assertTrue(results == shapes)
    },

    test("Codec[Team] round-trips a record with a sequence field") {
      val codec   = summon[Schema[Team]].derive(ScodecDeriver)
      val t       = Team("alpha", List("alice", "bob", "carol", "dave"))
      val encoded = codec.encode(t).require
      val decoded = codec.decode(encoded).require
      assertTrue(decoded.value == t, decoded.remainder == BitVector.empty)
    },

    test("the variant tag byte and case payload survive across encode/decode") {
      val codec = summon[Schema[Shape]].derive(ScodecDeriver)
      val r     = Rectangle(11.0, 22.0)
      val bits  = codec.encode(r).require
      // Tag is 1 byte (uint8 discriminator), so the encoded payload
      // should be 1 + 8 + 8 = 17 bytes.
      assertTrue(
        bits.size == 17L * 8L,
        codec.decode(bits).require.value == r
      )
    }
  )
}
