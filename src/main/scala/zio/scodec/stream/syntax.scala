/*
 * Copyright (c) 2013, Scodec
 * All rights reserved.
 *
 * Cleaner call sites for the streaming decoder API. Importing
 *
 *   import zio.scodec.stream.syntax.*
 *
 * makes the following style available:
 *
 *   uint8.streamMany.decode(byteStream)            // ZStream[A] from a Decoder
 *   byteStream.decodeMany(uint8)                   // same thing, ZStream-first
 *   bitStream.decodeManyBits(uint8)                // BitVector source variant
 *   uint8.pureMany.toStreamDecoder                 // build via the ZPure layer
 *   bytes.decodeStrict(StreamDecoder.many(uint8))  // strict in-memory decode
 *
 * The goal is that the day-to-day callsite never has to mention
 * `StreamDecoder.fromPure(PureDecoder.many(...))` or chain
 * `.toBytePipeline.via(...)`-style noise.
 */

package zio.scodec.stream

import _root_.scodec.{Decoder, DecodeResult}
import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.stream.ZStream

object syntax {

  // -------------------------------------------------------------------
  // scodec.Decoder[A]  ->  StreamDecoder[A] / PureDecoder[A]
  // -------------------------------------------------------------------

  extension [A](decoder: Decoder[A]) {

    /** Stream-decode exactly one `A`. */
    inline def streamOnce: StreamDecoder[A] = StreamDecoder.once(decoder)

    /** Stream-decode `A` values until the input ends. */
    inline def streamMany: StreamDecoder[A] = StreamDecoder.many(decoder)

    /** Stream-decode at most one `A`; on failure the bits are not consumed. */
    inline def streamTryOnce: StreamDecoder[A] = StreamDecoder.tryOnce(decoder)

    /** Stream-decode `A` values until decoding fails (no failure bubbled up). */
    inline def streamTryMany: StreamDecoder[A] = StreamDecoder.tryMany(decoder)

    /** Pure-decode `A` values until the input ends (uses the ZPure layer). */
    inline def pureMany: PureDecoder[A] = PureDecoder.many(decoder)

    /** Pure-decode exactly one `A`. */
    inline def pureOnce: PureDecoder[A] = PureDecoder.once(decoder)

    /** Lift this `Decoder` into the canonical `DecoderStep`. */
    inline def asPureStep: PureDecoder.DecoderStep[A] = PureDecoder.fromDecoder(decoder)
  }

  // -------------------------------------------------------------------
  // PureDecoder[A]  ->  StreamDecoder[A]
  // -------------------------------------------------------------------

  extension [A](pure: PureDecoder[A]) {

    /** Lift this pure decoder into the streaming pipeline. */
    inline def toStream: StreamDecoder[A] = StreamDecoder.fromPure(pure)
  }

  // -------------------------------------------------------------------
  // Batched (chunked) PureDecoder convenience
  // -------------------------------------------------------------------

  extension [A](pure: PureDecoder[Chunk[A]]) {

    /** Lift a *batched* pure decoder into the streaming pipeline,
      * flattening the inner chunks into the downstream stream. */
    inline def toStreamChunked: StreamDecoder[A] = StreamDecoder.fromPureChunked(pure)
  }

  // -------------------------------------------------------------------
  // ZStream[..., BitVector]  ->  ZStream[..., A]
  // -------------------------------------------------------------------

  extension [R](source: ZStream[R, Throwable, BitVector]) {

    /** Decode `A` values from this bit-stream using the supplied decoder. */
    def decodeManyBits[A](decoder: Decoder[A]): ZStream[R, Throwable, A] =
      source.via(StreamDecoder.many(decoder).toPipeline)

    /** Decode at most one `A` value from this bit-stream. */
    def decodeOnceBits[A](decoder: Decoder[A]): ZStream[R, Throwable, A] =
      source.via(StreamDecoder.once(decoder).toPipeline)

    /** Drive the supplied `StreamDecoder` over this bit-stream. */
    def viaDecoder[A](sd: StreamDecoder[A]): ZStream[R, Throwable, A] =
      source.via(sd.toPipeline)

    /** Drive the supplied `PureDecoder` over this bit-stream. */
    def viaDecoder[A](pd: PureDecoder[A]): ZStream[R, Throwable, A] =
      source.via(StreamDecoder.fromPure(pd).toPipeline)
  }

  // -------------------------------------------------------------------
  // ZStream[..., Byte]  ->  ZStream[..., A]
  // -------------------------------------------------------------------

  extension [R](source: ZStream[R, Throwable, Byte]) {

    /** Decode `A` values from this byte-stream using the supplied decoder. */
    def decodeMany[A](decoder: Decoder[A]): ZStream[R, Throwable, A] =
      source.via(StreamDecoder.many(decoder).toBytePipeline)

    /** Decode at most one `A` value from this byte-stream. */
    def decodeOnce[A](decoder: Decoder[A]): ZStream[R, Throwable, A] =
      source.via(StreamDecoder.once(decoder).toBytePipeline)

    /** Drive a `StreamDecoder` over this byte-stream. */
    def viaBytesDecoder[A](sd: StreamDecoder[A]): ZStream[R, Throwable, A] =
      source.via(sd.toBytePipeline)

    /** Drive a `PureDecoder` over this byte-stream. */
    def viaBytesDecoder[A](pd: PureDecoder[A]): ZStream[R, Throwable, A] =
      source.via(StreamDecoder.fromPure(pd).toBytePipeline)
  }

  // -------------------------------------------------------------------
  // BitVector  ->  strict in-memory result
  // -------------------------------------------------------------------

  extension (bits: BitVector) {

    /** Strictly decode this `BitVector` using a `StreamDecoder`. */
    def decodeStrict[A](sd: StreamDecoder[A]): Either[CodecError, DecodeResult[Chunk[A]]] = {
      val attempt = sd.strict.decode(bits)
      attempt.toEither match {
        case Right(dr) => Right(dr)
        case Left(err) => Left(CodecError(err))
      }
    }

    /** Strictly decode this `BitVector` using a `PureDecoder`. */
    def decodePure[A](pd: PureDecoder[A]): Either[CodecError, DecodeResult[Chunk[A]]] =
      pd.decodeStrict(bits)
  }
}
