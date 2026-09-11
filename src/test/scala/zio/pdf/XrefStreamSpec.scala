package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import zio.test.*

object XrefStreamSpec extends ZIOSpecDefault:
  private def dictionary(size: Long, widths: Int*): Prim.Dict =
    Prim.dict("Size" -> Prim.Number(BigDecimal(size)), "W" -> Prim.Array(widths.map(w => Prim.Number(BigDecimal(w)))*))

  private def bytes(values: Int*): BitVector = BitVector(values.map(_.toByte).toArray)
  private def field(width: Int, value: Long): BitVector =
    BitVector((0 until width).map(i => (value >>> ((width - i - 1) * 8)).toByte).toArray)
  private def failed(result: Attempt[?], fragment: String): Boolean = result match
    case Attempt.Failure(error) => error.messageWithContext.contains(fragment)
    case _ => false

  def spec: Spec[Any, Any] = suite("cross-reference streams")(
    test("DocuSign four-byte fields and permanently-free sentinel") {
      val parsed = XrefStream(dictionary(3, 1, 4, 4))(bytes(
        0, 0, 0, 0, 0, 255, 255, 255, 255,
        1, 0, 0, 1, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 5, 0, 0, 0, 7
      ))
      assertTrue(parsed.toOption.exists(_.tables.head.entries.toList == List(
        Xref.Entry.freeHead, Xref.entry(256, 0, Xref.EntryType.InUse), Xref.compressed(5, 7, Xref.EntryType.InUse)
      )))
    },
    test("eight-byte offset retains all digits without negative padding allocation") {
      val parsed = XrefStream(dictionary(1, 1, 8, 0))(bytes(1, 0, 0, 0, 16, 0, 0, 0, 0))
      assertTrue(parsed.toOption.exists(_.tables.head.entries.head.index == Xref.Index.Regular("68719476736", "00000")))
    },
    test("zero-width entry expansion is bounded before allocation") {
      assertTrue(failed(XrefStream(dictionary(Int.MaxValue.toLong + 1L, 0, 0, 0))(BitVector.empty), "entry limit"))
    },
    test("regular entries round-trip across every supported width") {
      check(Gen.int(0, 8), Gen.int(0, 8), Gen.int(0, 8), Gen.long(0, Long.MaxValue), Gen.int(0, 65535)) {
        (a, b, c, rawOffset, rawGeneration) =>
          val offset = if b == 8 then rawOffset else rawOffset & ((1L << (b * 8)) - 1L)
          val generation = if c == 0 then 0 else if c == 1 then rawGeneration & 255 else rawGeneration
          val payload = field(a, 1) ++ field(b, offset) ++ field(c, generation.toLong)
          assertTrue(XrefStream(dictionary(1, a, b, c))(payload).toOption.exists(
            _.tables.head.entries.head == Xref.entry(offset, generation, Xref.EntryType.InUse)
          ))
      }
    },
    test("compressed entries round-trip with padded unsigned fields") {
      check(Gen.int(1, 8), Gen.int(4, 8), Gen.long(1, 0xffffffffL), Gen.int(0, Int.MaxValue)) { (a, c, obj, index) =>
        val payload = field(a, 2) ++ field(8, obj) ++ field(c, index.toLong)
        assertTrue(XrefStream(dictionary(1, a, 8, c))(payload).toOption.exists(
          _.tables.head.entries.head == Xref.compressed(obj, index, Xref.EntryType.InUse)
        ))
      }
    },
    test("Index sections preserve payload order across sparse object ranges") {
      check(Gen.int(1, 1000), Gen.int(1, 1000), Gen.int(1, 1000000), Gen.int(1, 1000000)) { (gap, count, first, second) =>
        val data = Prim.Dict.updated("Index", Prim.Array.nums(0, 1, gap, count))(dictionary(gap.toLong + count, 1, 4, 0))
        val row = field(1, 1) ++ field(4, second)
        val payload = field(1, 1) ++ field(4, first) ++ BitVector.concat(Vector.fill(count)(row))
        assertTrue(XrefStream(data)(payload).toOption.exists { result =>
          result.tables.toList.map(_.offset) == List(0L, gap.toLong) &&
            result.tables.head.entries.head == Xref.entry(first, 0, Xref.EntryType.InUse) &&
            result.tables.last.entries.forall(_ == Xref.entry(second, 0, Xref.EntryType.InUse))
        })
      }
    },
    test("unsigned offsets above Long.MaxValue fail without overflow") {
      check(Gen.long(Long.MinValue, -1L)) { value =>
        assertTrue(failed(XrefStream(dictionary(1, 1, 8, 0))(field(1, 1) ++ field(8, value)), "offset"))
      }
    },
    test("out-of-range generations on in-use entries fail without narrowing") {
      check(Gen.long(65536, 0xffffffffL)) { generation =>
        assertTrue(failed(XrefStream(dictionary(1, 1, 1, 4))(bytes(1, 1) ++ field(4, generation)), "generation"))
      }
    },
    test("out-of-range compressed indexes fail without narrowing") {
      check(Gen.long(Int.MaxValue.toLong + 1L, 0xffffffffL)) { index =>
        assertTrue(failed(XrefStream(dictionary(1, 1, 1, 4))(bytes(2, 1) ++ field(4, index)), "index"))
      }
    },
    test("unknown entry types become null references") {
      check(Gen.int(3, 255)) { kind =>
        assertTrue(XrefStream(dictionary(1, 1, 1, 1))(bytes(kind, 0, 0)).toOption.exists(
          _.tables.head.entries.head.`type` == Xref.EntryType.Free
        ))
      }
    },
    test("malformed explicit Index never falls back to Size") {
      check(Gen.int(0, 1000)) { offset =>
        val data = Prim.Dict.updated("Index", Prim.Array.nums(offset))(dictionary(1, 1, 1, 1))
        assertTrue(failed(XrefStream(data)(bytes(1, 1, 0)), "Index"))
      }
    },
    test("invalid Index ranges are rejected before reading payloads") {
      check(Gen.int(1, 10000)) { n =>
        val indexes = List(Prim.Array.nums(0, n + 1, n, 1), Prim.Array.nums(n, 1, 0, 1),
          Prim.Array.nums(0.5, n), Prim.Array.nums(0, n + 0.5), Prim.Array.nums(n + 1, 1), Prim.Array.nums(-n, 1))
        assertTrue(indexes.forall(index => XrefStream.xrefStreamOffsets(
          Prim.Dict.updated("Index", index)(dictionary(n.toLong + 1, 1, 1, 1))
        ).isFailure))
      }
    },
    test("fractional, negative and excessive widths always return errors") {
      check(Gen.int(1, 10000)) { n =>
        val widths = List(Prim.Array.nums(-n, n + 1, 2), Prim.Array.nums(1, n + 0.5, 0.5),
          Prim.Array.nums(1, n + 8, 0), Prim.Array.nums(1, n), Prim.Array.nums(4294967296L + n, 0, 0))
        assertTrue(widths.forall(w => XrefStream(Prim.Dict.updated("W", w)(dictionary(1, 1, 1, 1)))(bytes(1, 1, 0)).isFailure))
      }
    },
    test("invalid Size declarations fail without truncation") {
      check(Gen.int(1, 100000)) { n =>
        assertTrue(List(BigDecimal(-n), BigDecimal(n) + BigDecimal("0.5"), BigDecimal(Long.MaxValue) + n).forall(size =>
          XrefStream.xrefStreamOffsets(Prim.Dict.updated("Size", Prim.Number(size))(dictionary(1, 1, 1, 1))).isFailure
        ))
      }
    },
    test("mutated payload lengths never produce a successful table") {
      check(Gen.int(1, 100), Gen.int(1, 7)) { (count, partialBits) =>
        val payload = BitVector.concat(Vector.fill(count)(bytes(1, 1, 0)))
        val data = dictionary(count, 1, 1, 1)
        assertTrue(XrefStream(data)(payload.dropRight(8)).isFailure,
          XrefStream(data)(payload ++ bytes(0)).isFailure,
          XrefStream(data)(payload ++ BitVector.high(partialBits.toLong)).isFailure)
      }
    },
    test("custom entry limits accept the boundary and reject the next entry") {
      check(Gen.int(1, 1000)) { limit =>
        val policy = XrefStream.Limits(limit)
        assertTrue(XrefStream(dictionary(limit, 0, 0, 0), policy)(BitVector.empty).isSuccessful,
          failed(XrefStream(dictionary(limit.toLong + 1, 0, 0, 0), policy)(BitVector.empty), "entry limit"))
      }
    }
  )
