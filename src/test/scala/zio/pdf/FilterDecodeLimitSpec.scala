package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import zio.test.*

object FilterDecodeLimitSpec extends ZIOSpecDefault:

  def spec: Spec[Any, Any] = suite("filter output bounds")(
    test("Flate decode rejects expansion beyond the typed byte limit") {
      val input      = BitVector(Array.fill[Byte](1024 * 1024)(0))
      val compressed = FlateEncode(input)
      val limit      = ByteLimit.fromBytes(64L * 1024L).toOption.get
      val decoded = compressed.flatMap { bytes =>
        FlateDecode(bytes, Prim.dict("DecodeParms" -> Prim.Dict.empty), limit)
      }

      assertTrue(
        decoded match
          case Attempt.Failure(error) => error.messageWithContext.contains("configured 65536-byte limit")
          case _                      => false
      )
    }
  )
