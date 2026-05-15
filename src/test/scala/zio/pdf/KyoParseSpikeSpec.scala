/*
 * Spike: Kyo `Parse` on `Text` for ASCII/PDF-ish prefixes, then scodec on the byte tail.
 * Kyo 1.0 RC1 already ships `Parse.int` as `Parse.read` + `span` + `toIntOption` (same idea as a hand-rolled indexed parser).
 */

package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.{BitVector, ByteVector}
import kyo.{Result as KResult, *}
import zio.test.*

object KyoParseSpikeSpec extends ZIOSpecDefault {

  /** Run a parser that must consume the full `input` string. */
  private def runFull[A](input: String)(p: => A < Parse): KResult[ParseFailed, A] =
    Abort.run(Parse.run(Text(input))(p)).eval

  private val pdfVersion: (Int, Int) < Parse =
    for
      _     <- Parse.literal(Text("%PDF-"))
      major <- Parse.int
      _     <- Parse.char('.')
      minor <- Parse.int
    yield (major, minor)

  private val startxrefOffset: Long < Parse =
    for
      _ <- Parse.literal(Text("startxref"))
      _ <- Parse.readWhile(c => c == '\n' || c == '\r' || c == ' ')
      n <- Parse.int
    yield n.toLong

  def spec: Spec[Any, Nothing] =
    suite("Kyo Parse spike (native Text + layered scodec)")(
      test("Parse.int / full consume") {
        assertTrue(runFull("42")(Parse.int) == KResult.succeed(42))
      },
      test("%PDF-M.N version line") {
        assertTrue(runFull("%PDF-1.7")(pdfVersion) == KResult.succeed((1, 7)))
      },
      test("startxref + offset (mirrors PdfTopLevel epilogue shape)") {
        val r = runFull("startxref\n1493726")(startxrefOffset)
        assertTrue(r == KResult.succeed(1493726L))
      },
      test("layer: Kyo text prefix then scodec on remaining bytes from same string") {
        val asciiPrefix = "len3"
        val tail        = Array[Byte](0xde.toByte, 0xad.toByte, 0xbe.toByte)
        val s = new String(
          (asciiPrefix + new String(tail, java.nio.charset.StandardCharsets.ISO_8859_1))
            .getBytes(java.nio.charset.StandardCharsets.ISO_8859_1),
          java.nio.charset.StandardCharsets.ISO_8859_1
        )
        val kyoPart =
          for
            _ <- Parse.literal(Text("len"))
            n <- Parse.int
          yield n
        val r = runFull(s.take(asciiPrefix.length))(kyoPart)
        assertTrue(r == KResult.succeed(3)) &&
        assertTrue {
          val bits = BitVector(s.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1))
          val rest = bits.drop(asciiPrefix.length * 8L)
          _root_.scodec.codecs.bytes(3).decode(rest) match {
            case Attempt.Successful(dr) => dr.value == ByteVector.view(tail)
            case _                      => false
          }
        }
      }
    )
}
