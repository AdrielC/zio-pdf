/*
 * Top-level facade for the PDF pipeline. Mirrors the legacy
 * fs2.pdf.PdfStream object but built on ZIO `ZStream` /
 * `ZPipeline`.
 */

package zio.pdf

import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.stream.{ZPipeline, ZStream}

object PdfStream {

  /**
   * Rechunk byte input into ~10 MiB BitVector chunks. Crucial for
   * performance, since constructors like `ZStream.fromInputStream`
   * and `ZStream.fromFile` use chunk sizes of a few KiB, which
   * causes the streaming decoder to re-parse large objects (like
   * images) until they have been read completely.
   */
  val bits: ZPipeline[Any, Throwable, Byte, BitVector] =
    ZPipeline
      .rechunk[Byte](10 * 1024 * 1024)
      .andThen(ZPipeline.mapChunks[Byte, BitVector](c => Chunk.single(BitVector.view(c.toArray))))

  /**
   * Decode top-level PDF chunks: indirect objects, the version
   * header, comments, xrefs, and stand-alone startxrefs.
   */
  val topLevel: ZPipeline[Any, Throwable, Byte, TopLevel] =
    bits >>> ZPipeline.fromChannel(
      zio.scodec.stream.StreamDecoder
        .many(TopLevel.streamDecoder)
        .toChannel
        .unit
    )

  /**
   * Decode to [[Decoded]]: streaming parse (memory-bounded for large
   * streams) plus expansion of each content stream via
   * [[Decode.expandStreamPayload]] (ObjStm, XRef stream metadata,
   * lazy decompression). Small streams (length <=
   * `config.inlineMaxBytes`) are buffered once as
   * [[StreamingDecoded.ContentObjStart]].inlinePayload; larger
   * streams use chunked bytes on the wire before expansion.
   */
  def decode(
    log: Log = Log.noop,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZPipeline[Any, Throwable, Byte, Decoded] =
    StreamingDecode.pipeline(log, config) >>> DecodedFromStreaming.pipeline

  /**
   * Raw streaming events only (no ObjStm / XRef expansion). Prefer
   * [[decode]] when you need [[elements]], [[validate]], or
   * [[compare]]. For a custom log or [[StreamingDecode.Config]], use
   * [[StreamingDecode.pipeline]].
   */
  val streamingDecode: ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    StreamingDecode.pipeline()

  /**
   * Decode the high-level Element layer: Page / Pages / Image /
   * FontResource / Info / etc.
   */
  def elements(log: Log = Log.noop): ZPipeline[Any, Throwable, Byte, Element] =
    decode(log) >>> Elements.pipe

  /**
   * After Part-shaping by a transformation, encode back to bytes
   * with a generated xref. The supplied `transform` should consume
   * `Element` values and produce `Part[Trailer]` values.
   */
  def transformElements[S](log: Log = Log.noop)(initial: S)(
    collect: RewriteState[S] => Element => (List[Part[Trailer]], RewriteState[S])
  )(
    update: RewriteUpdate[S] => Part[Trailer]
  ): ZPipeline[Any, Throwable, Byte, _root_.scodec.bits.ByteVector] =
    elements(log) >>> Rewrite.simpleParts(initial)(collect)(update) >>> WritePdf.parts

  /** Validate a PDF byte stream and return either Unit or a
    * non-empty list of errors. */
  def validate(log: Log = Log.noop)(
    bytes: zio.stream.ZStream[Any, Throwable, Byte]
  ): zio.ZIO[Any, Throwable, zio.prelude.Validation[PdfError, Unit]] =
    ValidatePdf.fromDecoded(bytes.via(decode(log)))

  /** Compare two PDF byte streams structurally. */
  def compare(log: Log = Log.noop)(
    old: zio.stream.ZStream[Any, Throwable, Byte],
    updated: zio.stream.ZStream[Any, Throwable, Byte]
  ): zio.ZIO[Any, Throwable, zio.prelude.Validation[CompareError, Unit]] =
    ComparePdfs.fromBytes(log)(old, updated)
}
