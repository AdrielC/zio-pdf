/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 */

package zio.scodec.stream

import _root_.scodec.bits.*
import _root_.scodec.codecs.*
import _root_.scodec.{Attempt, DecodeResult, Err}
import zio.*
import zio.stream.*
import zio.test.*
import zio.test.Assertion.*

object StreamDecoderSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("StreamDecoder")(

    test("once decodes a single value and stops") {
      val bits   = uint8.encode(42).require ++ uint8.encode(99).require
      val stream = ZStream(bits)
      for {
        out <- StreamDecoder.once(uint8).decode(stream).runCollect
      } yield assertTrue(out == Chunk(42))
    },

    test("many decodes all values until the input ends") {
      val bits = (0 until 16).map(i => uint8.encode(i).require).reduce(_ ++ _)
      val stream = ZStream(bits)
      for {
        out <- StreamDecoder.many(uint8).decode(stream).runCollect
      } yield assertTrue(out == Chunk.fromIterable(0 until 16))
    },

    test("many decodes across many small chunks (rechunk by 1 byte == 8 bits)") {
      val values = (0 until 256).toVector
      val bytes = values.foldLeft(ByteVector.empty)((acc, v) => acc :+ v.toByte)
      // Feed one byte at a time
      val stream: ZStream[Any, Nothing, BitVector] =
        ZStream.fromIterable(bytes.toArray.toList).map(b => BitVector.fromByte(b))
      for {
        out <- StreamDecoder.many(uint8).decode(stream).runCollect
      } yield assertTrue(out.toList == values.toList.map(_ & 0xff))
    },

    test("many fails when an inner decoder fails (failOnErr = true)") {
      // 5 bytes; uint16 needs 2 each so the 5th decode will get only 1 byte and the
      // stream will end -> InsufficientBits is allowed (clean termination), so we
      // need a different failure mode. Use a codec that fails on certain values.
      val bits   = hex"01 02 03".bits
      val pickyCodec = uint8.exmap[Int](
        v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
        v => Attempt.successful(v)
      )
      val program = StreamDecoder.many(pickyCodec).decode(ZStream(bits)).runCollect.exit
      program.map(exit => assert(exit)(failsWithA[CodecError]))
    },

    test("tryMany stops cleanly when an inner decoder fails") {
      val bits = hex"01 02 03".bits
      val pickyCodec = uint8.exmap[Int](
        v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
        v => Attempt.successful(v)
      )
      for {
        out <- StreamDecoder.tryMany(pickyCodec).decode(ZStream(bits)).runCollect
      } yield assertTrue(out == Chunk(1, 2))
    },

    test("emit yields a single value without consuming any input") {
      for {
        out <- StreamDecoder.emit("hi").decode(ZStream.empty).runCollect
      } yield assertTrue(out == Chunk("hi"))
    },

    test("emits yields the supplied values") {
      for {
        out <- StreamDecoder.emits(List(1, 2, 3)).decode(ZStream.empty).runCollect
      } yield assertTrue(out == Chunk(1, 2, 3))
    },

    test("++ runs the right decoder on the leftover from the left") {
      // First decode one uint8, then decode many uint16s from the rest.
      val bits = hex"ff 00 01 00 02".bits
      val decoder = StreamDecoder.once(uint8) ++ StreamDecoder.many(uint16).map(_.toLong)
      for {
        out <- decoder.decode(ZStream(bits)).runCollect
      } yield assertTrue(out == Chunk[Long](0xff, 1L, 2L))
    },

    test("flatMap can choose a continuation based on a decoded value") {
      // First byte = how many uint16 values follow.
      val bits    = (uint8.encode(3).require ++ uint16.encode(10).require ++ uint16.encode(20).require ++ uint16.encode(30).require)
      val decoder = StreamDecoder.once(uint8).flatMap { _ =>
        StreamDecoder.many(uint16).map(_.toLong)
      }
      for {
        out <- decoder.decode(ZStream(bits)).runCollect
      } yield assertTrue(out == Chunk[Long](10L, 20L, 30L))
    },

    test("isolate reads exactly the requested number of bits") {
      // 3 uint8 values, but isolate to 16 bits == only first 2.
      val bits    = hex"01 02 03".bits
      val decoder = StreamDecoder.isolate(16)(StreamDecoder.many(uint8))
      for {
        out <- decoder.decode(ZStream(bits)).runCollect
      } yield assertTrue(out == Chunk(1, 2))
    },

    test("ignore drops the requested number of bits") {
      val bits    = hex"aa bb cc".bits
      val decoder = StreamDecoder.ignore(8) ++ StreamDecoder.many(uint8)
      for {
        out <- decoder.decode(ZStream(bits)).runCollect
      } yield assertTrue(out == Chunk(0xbb, 0xcc))
    },

    test("raiseError fails the stream with the supplied throwable") {
      val boom    = new RuntimeException("boom")
      val emptyBits: ZStream[Any, Throwable, BitVector] = ZStream.empty
      val program = StreamDecoder.raiseError(boom).decode(emptyBits).runCollect.exit
      program.map(exit => assert(exit)(fails(equalTo(boom))))
    },

    test("raiseError(Err) wraps in CodecError") {
      val err     = Err("nope")
      val emptyBits: ZStream[Any, Throwable, BitVector] = ZStream.empty
      val program = StreamDecoder.raiseError(err).decode(emptyBits).runCollect.exit
      program.map(exit => assert(exit)(fails(equalTo(CodecError(err)))))
    },

    test("strict round-trip via Decoder yields the same values plus leftover") {
      val bits = (0 until 4).map(i => uint8.encode(i).require).reduce(_ ++ _) ++ hex"ff".bits
      val decoder = StreamDecoder.many(uint8).strict
      val result  = decoder.decode(bits)
      assertTrue(
        result.isSuccessful,
        result.require.value == Chunk(0, 1, 2, 3, 0xff)
      )
    },

    test("toBytePipeline works on a Byte stream") {
      val bytes  = Chunk[Byte](1, 2, 3, 4)
      val stream = ZStream.fromChunk(bytes)
      for {
        out <- stream.via(StreamDecoder.many(uint8).toBytePipeline).runCollect
      } yield assertTrue(out == Chunk(1, 2, 3, 4))
    }
  )
}
