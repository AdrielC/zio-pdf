package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.test.*

object ObjectStreamSpec extends ZIOSpecDefault {

  private val stream = ObjectStream(
    List(
      Obj(Obj.Index(11L, 0), Prim.dict("Type" -> Prim.Name("Example"), "Value" -> Prim.Number(1))),
      Obj(Obj.Index(29L, 0), Prim.Array(Prim.Name("A"), Prim.Number(BigDecimal("2.5")))),
      Obj(Obj.Index(41L, 0), Prim.Str(ByteVector.encodeUtf8("payload").toOption.get))
    )
  )

  def spec: Spec[Any, Nothing] = suite("ObjectStream")(
    test("generated /N and /First metadata round-trip every object and offset") {
      val result = for {
        encoded <- ObjectStream.encode(stream)
        dictionary = Prim.dict(
          "Type"  -> Prim.Name("ObjStm"),
          "N"     -> Prim.Number(BigDecimal(encoded.count)),
          "First" -> Prim.Number(BigDecimal(encoded.first))
        )
        decoded <- ObjectStream.decode(encoded.bytes, dictionary)
      } yield (encoded, decoded)

      assertTrue(result match {
        case Attempt.Successful((encoded, decoded)) =>
          encoded.count == 3 && encoded.first > 0 && decoded == stream
        case _ => false
      })
    },
    test("rejects an offset outside the payload instead of decoding adjacent bytes") {
      val bytes  = BitVector("11 99 \nnull\n".getBytes)
      val result = ObjectStream.decode(bytes, count = 1, first = 7)
      assertTrue(result.isFailure)
    },
    test("requires /N and /First from the enclosing dictionary") {
      val result = ObjectStream.decode(BitVector.empty, Prim.dict("Type" -> Prim.Name("ObjStm")))
      assertTrue(result.isFailure)
    }
  )
}
