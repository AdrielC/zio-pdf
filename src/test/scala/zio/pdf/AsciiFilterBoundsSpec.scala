package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.test.*

object AsciiFilterBoundsSpec extends ZIOSpecDefault:
  private def bits(value: String): BitVector = BitVector(value.getBytes("US-ASCII"))
  private def limit(n: Int): ByteLimit = ByteLimit.fromBytes(n.toLong).toOption.get

  def spec: Spec[Any, Any] = suite("ASCII filter bounds and malformed inputs")(
    test("ASCII85 z expansion stops at the configured output bound") {
      check(Gen.int(1, 1000)) { count =>
        val source = bits("z" * count + "~>")
        assertTrue(Ascii85Decode(source, limit(count * 4)).toOption.contains(BitVector(Array.fill[Byte](count * 4)(0))),
          Ascii85Decode(source, limit(count * 4 - 1)).isFailure)
      }
    },
    test("ASCII85 full and partial tuples respect exact output bounds") {
      check(Gen.int(1, 100), Gen.int(2, 4)) { (count, tail) =>
        val source = bits("!!!!!" * count + "!" * tail + "~>")
        val size = count * 4 + tail - 1
        assertTrue(Ascii85Decode(source, limit(size)).toOption.exists(_.size == size.toLong * 8),
          Ascii85Decode(source, limit(size - 1)).isFailure)
      }
    },
    test("ASCII85 rejects overflowing tuples, single trailing digits, embedded z and broken terminators") {
      assertTrue(List("uuuuu~>", "!~>", "!z~>", "!!!~", "!!!~x").forall(s => Ascii85Decode(bits(s)).isFailure))
    },
    test("ASCII85 decodes a known published encoding") {
      assertTrue(Ascii85Decode(bits("87cURD]j7BEbo80~>")).toOption.contains(bits("Hello world!")))
    },
    test("ASCII85 preserves high bytes in full and padded final tuples") {
      val encodings = List("rr", "s8N", "s8W*", "s8W-!")
      assertTrue(encodings.zipWithIndex.forall { (encoded, i) =>
        Ascii85Decode(bits(encoded + "~>"), limit(i + 1)).toOption
          .contains(BitVector(Array.fill[Byte](i + 1)(255.toByte)))
      })
    },
    test("ASCIIHex handles whitespace and odd final nibbles within its bound") {
      check(Gen.int(1, 1000), Gen.elements(" ", "\n", "\r", "\t", "\u0000")) { (count, space) =>
        val source = bits(("6" + space + "1" + space) * count + "6>")
        assertTrue(AsciiHexDecode(source, limit(count + 1)).toOption.contains(bits("a" * count + "`")),
          AsciiHexDecode(source, limit(count)).isFailure)
      }
    },
    test("ASCIIHex rejects bad digits") {
      check(Gen.int('g'.toInt, 'z'.toInt)) { char =>
        assertTrue(AsciiHexDecode(bits("6" + char.toChar + ">"), limit(1)).isFailure)
      }
    },
    test("filter chains pass the limit to the expanding ASCII stage") {
      check(Gen.int(1, 1000)) { count =>
        assertTrue(FilterDecode.applyChain(bits("z" * count + "~>"),
          Prim.dict("Filter" -> Prim.Name("ASCII85Decode")), limit(count * 4 - 1)).isFailure)
      }
    },
    test("logical multi-gigabyte input views are rejected without copying the input") {
      val size = Int.MaxValue.toLong + 1L
      val base85 = ByteVector.fill(size)('z'.toByte).bits
      val hex = ByteVector.fill(size)('6'.toByte).bits
      assertTrue(Ascii85Decode(base85, limit(4)).isFailure, AsciiHexDecode(hex, limit(4)).isFailure)
    }
  )
