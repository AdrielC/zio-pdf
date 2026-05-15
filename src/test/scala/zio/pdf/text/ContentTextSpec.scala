package zio.pdf.text

import _root_.scodec.bits.BitVector
import zio.blocks.streams.Stream
import zio.test.*

object ContentTextSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("ContentText")(
    test("extracts simple Tj text inside a text object") {
      val bits = BitVector("BT /F1 24 Tf 100 700 Td (hi) Tj ET\n".getBytes)
      assertTrue(ContentText.extract(bits).contains("hi"))
    },
    test("extracts array text and literal escapes") {
      val bits = BitVector("""BT [(Hello\040) 120 (World) <21>] TJ ET""".getBytes)
      assertTrue(ContentText.extract(bits).contains("Hello World!"))
    },
    test("exposes a zio-blocks-streams token pipeline") {
      val chunks =
        Stream(TextToken.Str("A"), TextToken.Word("Tj"), TextToken.HexStr("B"))
          .via(ContentText.pipeline)
          .runCollect
      assertTrue(chunks.exists(_.toList == List(TextChunk("A"), TextChunk("B"))))
    }
  )
}
