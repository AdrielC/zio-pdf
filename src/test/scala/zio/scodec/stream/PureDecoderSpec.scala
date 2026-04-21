/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 */

package zio.scodec.stream

import _root_.scodec.bits.*
import _root_.scodec.codecs.*
import _root_.scodec.{Attempt, Err}
import zio.*
import zio.stream.*
import zio.test.*

object PureDecoderSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Throwable] = suite("PureDecoder (ZPure)")(

    suite("pure-only - no Runtime needed")(

      test("emit logs the value and finishes Done") {
        val pd            = PureDecoder.emit(42)
        val (log, result) = pd.run.runAll(BitVector.empty)
        assertTrue(
          log == Chunk(42),
          result == Right((BitVector.empty, PureDecoder.Status.Done))
        )
      },

      test("emits logs every value in order") {
        val pd            = PureDecoder.emits(List("a", "b", "c"))
        val (log, result) = pd.run.runAll(BitVector.empty)
        assertTrue(
          log == Chunk("a", "b", "c"),
          result.map(_._2) == Right(PureDecoder.Status.Done)
        )
      },

      test("once on a sufficient buffer decodes a single value") {
        val bits          = uint8.encode(7).require ++ uint8.encode(8).require
        val pd            = PureDecoder.once(uint8)
        val (log, result) = pd.run.runAll(bits)
        assertTrue(
          log == Chunk(7),
          result.map(_._1.size) == Right(8L), // one byte left
          result.map(_._2) == Right(PureDecoder.Status.Done)
        )
      },

      test("once on an empty buffer asks for more bits via Status.NeedMore (not failure)") {
        val pd            = PureDecoder.once(uint8)
        val (log, result) = pd.run.runAll(BitVector.empty)
        assertTrue(
          log == Chunk.empty,
          result == Right((BitVector.empty, PureDecoder.Status.NeedMore))
        )
      },

      test("many drains the entire buffer and signals NeedMore at the end") {
        val bits          = (0 until 5).map(i => uint8.encode(i).require).reduce(_ ++ _)
        val pd            = PureDecoder.many(uint8)
        val (log, result) = pd.run.runAll(bits)
        assertTrue(
          log == Chunk(0, 1, 2, 3, 4),
          result == Right((BitVector.empty, PureDecoder.Status.NeedMore))
        )
      },

      test("many fails through the ZPure error channel on a non-insufficient-bits decode error") {
        val bits   = hex"01 02 03".bits
        val picky  = uint8.exmap[Int](
          v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
          v => Attempt.successful(v)
        )
        val pd            = PureDecoder.many(picky)
        val (log, result) = pd.run.runAll(bits)
        assertTrue(
          // The two successful values are still in the log.
          log == Chunk(1, 2),
          result.isLeft
        )
      },

      test("tryMany finishes cleanly (Status.Done) on a non-insufficient-bits decode error") {
        val bits   = hex"01 02 03".bits
        val picky  = uint8.exmap[Int](
          v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
          v => Attempt.successful(v)
        )
        val pd            = PureDecoder.tryMany(picky)
        val (log, result) = pd.run.runAll(bits)
        assertTrue(
          log == Chunk(1, 2),
          result.map(_._2) == Right(PureDecoder.Status.Done)
        )
      },

      test("++ concatenates two pure decoders, threading the carry buffer") {
        val bits = hex"ff 00 01 00 02".bits
        val pd   = PureDecoder.once(uint8) ++ PureDecoder.many(uint16)
        val (log, result) = pd.run.runAll(bits)
        assertTrue(
          log == Chunk(0xff, 1, 2),
          result.map(_._1) == Right(BitVector.empty)
        )
      },

      test("map transforms every emitted value (and preserves state and status)") {
        val bits          = (0 until 3).map(i => uint8.encode(i).require).reduce(_ ++ _)
        val pd            = PureDecoder.many(uint8).map(_.toString)
        val (log, result) = pd.run.runAll(bits)
        assertTrue(
          log == Chunk("0", "1", "2"),
          result == Right((BitVector.empty, PureDecoder.Status.NeedMore))
        )
      },

      test("decodeStrict returns the chunk and leftover via DecodeResult") {
        val bits = (0 until 4).map(i => uint8.encode(i).require).reduce(_ ++ _) ++ hex"ff".bits
        val pd   = PureDecoder.many(uint8)
        val r    = pd.decodeStrict(bits)
        assertTrue(
          r.isRight,
          r.toOption.exists(dr => dr.value == Chunk(0, 1, 2, 3, 0xff)),
          r.toOption.exists(_.remainder == BitVector.empty)
        )
      },

      test("attemptStrict surfaces a CodecError as Attempt.Failure") {
        val bits   = hex"01 02 03".bits
        val picky  = uint8.exmap[Int](
          v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
          v => Attempt.successful(v)
        )
        val pd     = PureDecoder.many(picky)
        val result = pd.attemptStrict(bits)
        assertTrue(!result.isSuccessful)
      }
    ),

    suite("driven by ZChannel via StreamDecoder.fromPure")(

      test("a PureDecoder.many lifted through fromPure decodes a chunked ZStream") {
        val values = (0 until 64).toVector
        val source: ZStream[Any, Throwable, BitVector] =
          ZStream.fromIterable(values).map(v => uint8.encode(v).require)
        val sd = StreamDecoder.fromPure(PureDecoder.many(uint8))
        for {
          out <- sd.decode(source).runCollect
        } yield assertTrue(out.toList == values.toList)
      },

      test("a PureDecoder.tryMany stops cleanly when an inner decode fails") {
        val bits   = hex"01 02 03".bits
        val picky  = uint8.exmap[Int](
          v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
          v => Attempt.successful(v)
        )
        val sd = StreamDecoder.fromPure(PureDecoder.tryMany(picky))
        for {
          out <- sd.decode(ZStream(bits)).runCollect
        } yield assertTrue(out == Chunk(1, 2))
      },

      test("a PureDecoder.many flushes already-emitted values before failing fatally") {
        val bits   = hex"01 02 03 04".bits
        val picky  = uint8.exmap[Int](
          v => if (v == 0x03) Attempt.failure(Err("nope")) else Attempt.successful(v),
          v => Attempt.successful(v)
        )
        val sd = StreamDecoder.fromPure(PureDecoder.many(picky))
        for {
          // Use either to capture both the partial chunk and the failure
          either <- sd.decode(ZStream(bits)).either.runCollect
        } yield assertTrue(
          either.collect { case Right(v) => v } == Chunk(1, 2),
          either.exists(_.isLeft)
        )
      },

      test("PureDecoder.toStreamDecoder is the same as StreamDecoder.fromPure") {
        val bits = (0 until 8).map(i => uint8.encode(i).require).reduce(_ ++ _)
        val a    = PureDecoder.many(uint8).toStreamDecoder
        val b    = StreamDecoder.fromPure(PureDecoder.many(uint8))
        for {
          ra <- a.decode(ZStream(bits)).runCollect
          rb <- b.decode(ZStream(bits)).runCollect
        } yield assertTrue(ra == rb, ra == Chunk(0, 1, 2, 3, 4, 5, 6, 7))
      },

      test("flatMap on a fromPure chains a continuation against every emitted value") {
        // Decode one uint8 = N, then decode N more uint8 values.
        val bits = uint8.encode(3).require ++ hex"0a 0b 0c".bits
        val sd = StreamDecoder
          .fromPure(PureDecoder.once(uint8))
          .flatMap(n => StreamDecoder.fromPure(PureDecoder.many(uint8)).map(v => (n, v)))
        for {
          out <- sd.decode(ZStream(bits)).runCollect
        } yield assertTrue(out == Chunk((3, 0x0a), (3, 0x0b), (3, 0x0c)))
      }
    )
  )
}
