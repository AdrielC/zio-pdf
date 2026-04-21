/*
 * Round-trip tests for Prim, Obj, IndirectObj, Trailer, Xref.
 * Mirrors a chunk of the legacy fs2.pdf.PrimTest.
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.test.*

object PrimSpec extends ZIOSpecDefault {

  private def roundTrip[A](a: A)(using c: _root_.scodec.Codec[A]): TestResult = {
    val bits    = c.encode(a).require
    val decoded = c.decode(bits).require
    assertTrue(decoded.value == a, decoded.remainder == BitVector.empty)
  }

  def spec: Spec[Any, Throwable] = suite("zio.pdf.Prim & friends")(

    test("Prim.Null round-trips") {
      val bits = Prim.Codec_Prim.encode(Prim.Null).require
      val r    = Prim.Codec_Prim.decode(bits).require
      assertTrue(r.value == Prim.Null)
    },

    test("Prim.Bool true / false round-trip") {
      assertTrue(
        Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(Prim.Bool(true)).require).require.value == Prim.Bool(true),
        Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(Prim.Bool(false)).require).require.value == Prim.Bool(false)
      )
    },

    test("Prim.Number integers and decimals round-trip") {
      val cases = List(Prim.Number(BigDecimal(0)), Prim.Number(BigDecimal(42)), Prim.Number(BigDecimal("-7.25")))
      assertTrue(cases.forall(n => Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(n).require).require.value == n))
    },

    test("Prim.Name round-trips") {
      val n = Prim.Name("Type")
      assertTrue(Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(n).require).require.value == n)
    },

    test("Prim.Ref round-trips") {
      val r = Prim.Ref(42, 0)
      assertTrue(Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(r).require).require.value == r)
    },

    test("Prim.Array round-trips") {
      val a = Prim.Array(Prim.Number(BigDecimal(1)), Prim.Number(BigDecimal(2)), Prim.Ref(7, 0))
      assertTrue(Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(a).require).require.value == a)
    },

    test("Prim.Dict round-trips") {
      val d = Prim.dict("Type" -> Prim.Name("Page"), "Count" -> Prim.Number(BigDecimal(3)))
      assertTrue(Prim.Codec_Prim.decode(Prim.Codec_Prim.encode(d).require).require.value == d)
    },

    test("Obj round-trips") {
      val o = Obj(Obj.Index(5, 0), Prim.dict("Type" -> Prim.Name("Catalog")))
      roundTrip(o)
    },

    test("IndirectObj without stream round-trips") {
      val o = IndirectObj(Obj(Obj.Index(5, 0), Prim.dict("Type" -> Prim.Name("Catalog"))), None)
      roundTrip(o)
    },

    test("IndirectObj with stream round-trips") {
      val payload = BitVector("hello, stream!".getBytes)
      val o       = IndirectObj.stream(7, Prim.dict("Type" -> Prim.Name("XObject")), payload)
      val bits    = summon[_root_.scodec.Codec[IndirectObj]].encode(o).require
      val r       = summon[_root_.scodec.Codec[IndirectObj]].decode(bits).require
      assertTrue(r.value.obj.index == o.obj.index, r.value.stream.contains(payload))
    }
  )
}
