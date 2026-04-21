/*
 * Demonstrates the cleaner extension-method API.
 */

package zio.scodec.stream

import _root_.scodec.bits.*
import _root_.scodec.codecs.*
import zio.*
import zio.stream.*
import zio.test.*

import zio.scodec.stream.syntax.*

object SyntaxSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("zio.scodec.stream.syntax")(

    test("decoder.streamMany builds a StreamDecoder") {
      val sd: StreamDecoder[Int] = uint8.streamMany
      val source                 = ZStream.succeed(hex"01 02 03".bits)
      for {
        out <- source.viaDecoder(sd).runCollect
      } yield assertTrue(out == Chunk(1, 2, 3))
    },

    test("byteStream.decodeMany(decoder) is the byte-stream sugar") {
      val source: ZStream[Any, Throwable, Byte] =
        ZStream.fromChunk(Chunk[Byte](10, 20, 30))
      for {
        out <- source.decodeMany(uint8).runCollect
      } yield assertTrue(out == Chunk(10, 20, 30))
    },

    test("bitStream.decodeManyBits(decoder) is the bit-stream sugar") {
      val source: ZStream[Any, Throwable, BitVector] =
        ZStream.succeed(hex"aa bb cc".bits)
      for {
        out <- source.decodeManyBits(uint8).runCollect
      } yield assertTrue(out == Chunk(0xaa, 0xbb, 0xcc))
    },

    test("decoder.pureMany.toStream is equivalent to StreamDecoder.fromPure(PureDecoder.many)") {
      val source = ZStream.succeed(hex"00 01 02 03 04 05 06 07".bits)
      val viaSyntax = uint8.pureMany.toStream
      val viaLong   = StreamDecoder.fromPure(PureDecoder.many(uint8))
      for {
        a <- source.viaDecoder(viaSyntax).runCollect
        b <- source.viaDecoder(viaLong).runCollect
      } yield assertTrue(a == b, a == Chunk[Int](0, 1, 2, 3, 4, 5, 6, 7))
    },

    test("byteStream.viaBytesDecoder(pure) drives a PureDecoder over bytes") {
      val source: ZStream[Any, Throwable, Byte] =
        ZStream.fromChunk(Chunk[Byte](1, 2, 3, 4))
      for {
        out <- source.viaBytesDecoder(uint8.pureMany).runCollect
      } yield assertTrue(out == Chunk(1, 2, 3, 4))
    },

    test("bits.decodeStrict(sd) returns Either[CodecError, DecodeResult[Chunk[A]]]") {
      val bits = hex"05 ff".bits
      val r    = bits.decodeStrict(uint8.streamMany)
      assertTrue(
        r match {
          case Right(dr) => dr.value == Chunk(5, 0xff) && dr.remainder.isEmpty
          case _         => false
        }
      )
    },

    test("decoder.asPureStep gives the canonical DecoderStep") {
      val step = uint8.asPureStep
      val (_, r) = step.runAll(hex"42 ff".bits)
      assertTrue(
        r match {
          case Right((rem, value)) => value == 0x42 && rem == hex"ff".bits
          case _                   => false
        }
      )
    }
  )
}
