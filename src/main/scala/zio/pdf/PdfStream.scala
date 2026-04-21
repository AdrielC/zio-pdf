/*
 * Top-level facade for the PDF pipeline. Mirrors the legacy
 * fs2.pdf.PdfStream object but built on ZIO `ZStream` /
 * `ZPipeline`.
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.Chunk
import zio.stream.{ZPipeline, ZStream}

/**
 * The main API for this library provides ZIO `ZPipeline`s for
 * decoding PDF streams. The encoder side (`write`, `transformElements`,
 * `validate`, `compare`) will land in a follow-up.
 */
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
   * Decode the data layer: data objects (without stream), content
   * objects (with stream and lazy uncompression), and metadata
   * (`Decoded.Meta`) consisting of accumulated xrefs, the
   * sanitised trailer, and the version. Equivalent to the legacy
   * `PdfStream.decode`.
   */
  def decode(log: Log = Log.noop): ZPipeline[Any, Throwable, Byte, Decoded] =
    Decode(log)
}
