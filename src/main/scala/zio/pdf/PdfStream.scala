/*
 * Top-level facade for the PDF pipeline. Mirrors the legacy
 * fs2.pdf.PdfStream object but built on ZIO `ZStream` /
 * `ZPipeline`.
 */

package zio.pdf

import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.stream.{ZPipeline, ZStream}

/**
 * The main API for this library provides ZIO `ZPipeline`s for
 * decoding PDF streams. The encoder side (`write`, `transformElements`,
 * `validate`, `compare`) will land in a follow-up.
 */
object PdfStream {

  /**
   * Which PDF decode pipeline to run on raw bytes. Both modes apply
   * the same duplicate-object filtering as the legacy decoder; they
   * differ only in how content-stream payloads are represented.
   *
   *  - [[DecodeMode.Materialized]] — each stream is decoded (and for
   *    non-ObjStm objects, lazily decompressed) into [[Decoded]].
   *    Convenient for [[elements]] and typical PDFs; peak memory is
   *    bounded by the largest single object's payload.
   *  - [[DecodeMode.Streaming]] — SAX-style [[StreamingDecoded]] events;
   *    payload bytes are forwarded in chunks. Use for very large
   *    embedded streams when you want memory bounded by upstream chunk
   *    size.
   */
  sealed trait DecodeMode
  object DecodeMode {
    case object Materialized extends DecodeMode
    case object Streaming extends DecodeMode
  }

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
   * Decode the data layer: data objects (without stream), content
   * objects (with stream and lazy uncompression), and metadata
   * (`Decoded.Meta`) consisting of accumulated xrefs, the
   * sanitised trailer, and the version. Equivalent to the legacy
   * `PdfStream.decode`.
   *
   * Peak memory is bounded by *the largest single content stream*
   * because each `Decoded.ContentObj` carries the full payload as
   * a `BitVector`. For PDFs with multi-MB attachments / images /
   * fonts use [[decode]] with [[DecodeMode.Streaming]] instead.
   */
  def decode(log: Log = Log.noop): ZPipeline[Any, Throwable, Byte, Decoded] =
    decode(log, DecodeMode.Materialized)

  /** @see [[DecodeMode]] */
  def decode(log: Log, mode: DecodeMode.Materialized.type): ZPipeline[Any, Throwable, Byte, Decoded] =
    Decode(log)

  /** @see [[DecodeMode]] */
  def decode(log: Log, mode: DecodeMode.Streaming.type): ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    StreamingDecode.pipeline(log)

  def decode(log: Log, mode: DecodeMode): ZPipeline[Any, Throwable, Byte, Decoded | StreamingDecoded] =
    mode match {
      case DecodeMode.Materialized => Decode(log)
      case DecodeMode.Streaming    => StreamingDecode.pipeline(log)
    }

  /**
   * Memory-bounded SAX-style decoder. Same as
   * `decode(Log.noop, DecodeMode.Streaming)`.
   */
  val streamingDecode: ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    StreamingDecode.pipeline(Log.noop)

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
