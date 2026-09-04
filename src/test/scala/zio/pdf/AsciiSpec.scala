package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.bits.ByteVector
import zio.test.*

object AsciiSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] = suite("ascii interpolator")(
    test("bakes US-ASCII literals into a ByteVector at compile time") {
      assertTrue(
        ascii"xref" == ByteVector.view("xref".getBytes(StandardCharsets.US_ASCII)),
        ascii"%%EOF" == ByteVector.view("%%EOF".getBytes(StandardCharsets.US_ASCII)),
        ascii"%PDF-" == ByteVector.view("%PDF-".getBytes(StandardCharsets.US_ASCII)),
        ascii"".isEmpty
      )
    },
    test("ascii escape sequences become control bytes") {
      assertTrue(
        ascii"stream\n".toArray.sameElements(Array[Byte]('s', 't', 'r', 'e', 'a', 'm', '\n')),
        ascii"\nendstream\n".head == '\n'.toByte,
        ascii"\nendstream\nendobj\n".last == '\n'.toByte
      )
    },
    test("asciiBytes matches the ByteVector payload") {
      assertTrue(asciiBytes"Length" sameElements ascii"Length".toArray)
    }
  )
}
