/*
 * Top-level facade for the PDF pipeline. Mirrors the legacy
 * fs2.pdf.PdfStream object but built on ZIO `ZStream` /
 * `ZPipeline`.
 */

package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import zio.{Chunk, ZIO}
import zio.scodec.stream.ChunkBytes
import zio.stream.{ZPipeline, ZStream}

object PdfStream {

  /**
   * Rechunk byte input into ~10 MiB BitVector chunks. Crucial for
   * performance, since constructors like `ZStream.fromInputStream`
   * and `ZStream.fromFile` use chunk sizes of a few KiB, which
   * causes the streaming decoder to re-parse large objects (like
   * images) until they have been read completely.
   */
  val bits: ZPipeline[Any, Throwable, Byte, _root_.scodec.bits.BitVector] =
    ZPipeline
      .rechunk[Byte](10 * 1024 * 1024)
      .andThen(ZPipeline.mapChunks(ChunkBytes.toBitVectorChunk))

  /** Decode top-level PDF chunks (same hot path as [[TopLevel.pipe]]). */
  val topLevel: ZPipeline[Any, Throwable, Byte, TopLevel] =
    TopLevel.pipe

  /**
   * Decode to [[Decoded]]: streaming parse (memory-bounded for large
   * streams) plus expansion of each content stream via
   * [[Decode.expandStreamPayload]] (ObjStm, XRef stream metadata,
   * lazy decompression). Small streams (length <=
   * `config.inlineMaxBytes`) are buffered once as
   * [[StreamingDecoded.ContentObjStart]].inlinePayload; larger
   * streams use chunked bytes on the wire before expansion.
   *
   * When the PDF already fits in memory, prefer [[PdfHyperdrive.decodeSync]]
   * or [[zio.pdf.io.PdfIO.warp]] — no `ZChannel` per chunk.
   */
  def decode(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZPipeline[Any, Throwable, Byte, Decoded] =
    StreamingDecode.pipeline(enableDiagnostics, config) >>> DecodedFromStreaming.pipeline

  /**
   * Raw streaming events only (no ObjStm / XRef expansion). Prefer
   * [[decode]] when you need [[elements]], [[validate]], or
   * [[compare]].
   *
   * Returns a **new** pipeline on each call so duplicate-filter state
   * is not shared across streams (a shared `val` would reuse one
   * mutable [[DuplicateFilterState]] and break subsequent runs).
   * Call with empty parentheses: `streamingDecode()` — a bare
   * reference eta-expands to a function type in `.via(...)`.
   */
  def streamingDecode(
    enableDiagnostics: Boolean = false,
    config: StreamingDecode.Config = StreamingDecode.Config.default
  ): ZPipeline[Any, Throwable, Byte, StreamingDecoded] =
    StreamingDecode.pipeline(enableDiagnostics, config)

  /**
   * Decode the high-level Element layer: Page / Pages / Image /
   * FontResource / Info / etc.
   */
  def elements(enableDiagnostics: Boolean = false): ZPipeline[Any, Throwable, Byte, Element] =
    decode(enableDiagnostics) >>> Elements.pipe

  /**
   * Process decoded PDF attachment payloads (and any other stream objects) in a
   * single pass: `flatMap` runs only on [[Element.Content]]; [[Element.Data]] /
   * [[Element.Meta]] are re-emitted unchanged.
   *
   * Example — hash each `/Type /EmbeddedFile` stream without materialising the
   * whole PDF:
   *
   * {{{
   *   bytes.via(PdfStream.elements()).flatMap {
   *     case c @ Element.Content(_, _, stream, Element.ContentKind.EmbeddedFileStream(_)) =>
   *       ZStream.fromZIO(
   *         stream.value.map(bits => java.security.MessageDigest.getInstance("SHA-256").digest(bits.toByteArray))
   *       ).as(c)
   *     case other => ZStream.succeed(other)
   *   }
   * }}}
   */
  def mapContentElements[R](
    f: Element.Content => ZStream[R, Throwable, Element]
  ): ZPipeline[R, Throwable, Element, Element] =
    ZPipeline.mapChunksZIO { chunk =>
      ZIO.foreach(chunk) {
        case c: Element.Content => f(c).runCollect
        case e                    => ZIO.succeed(Chunk.single(e))
      }.map(_.flatten)
    }

  /**
   * After Part-shaping by a transformation, encode back to bytes
   * with a generated xref. The supplied `transform` should consume
   * `Element` values and produce `Part[Trailer]` values.
   */
  def transformElements[S](enableDiagnostics: Boolean = false)(initial: S)(
    collect: RewriteState[S] => Element => (List[Part[Trailer]], RewriteState[S])
  )(
    update: RewriteUpdate[S] => Part[Trailer]
  ): ZPipeline[Any, Throwable, Byte, _root_.scodec.bits.ByteVector] =
    elements(enableDiagnostics) >>> Rewrite.simpleParts(initial)(collect)(update) >>> WritePdf.parts

  /**
   * Decompress each rewritable content stream, apply `f` to the
   * uncompressed bytes, recompress with Flate, and re-encode the PDF.
   * Streams whose `/Filter` chain includes image/Crypt filters are
   * left byte-identical (raw payload preserved).
   */
  def mapUncompressedContent(
    enableDiagnostics: Boolean = false
  )(f: BitVector => BitVector): ZPipeline[Any, Throwable, Byte, _root_.scodec.bits.ByteVector] =
    transformElements(enableDiagnostics)(())(mapUncompressedCollect(f))(Rewrite.noUpdate)

  private def mapUncompressedCollect(
    f: BitVector => BitVector
  ): RewriteState[Unit] => Element => (List[Part[Trailer]], RewriteState[Unit]) =
    state => {
      case Element.Meta(trailer, _) =>
        (Nil, state.copy(trailer = trailer.orElse(state.trailer)))
      case Element.Data(obj, _) =>
        (List(Part.Obj(IndirectObj(obj, None))), state)
      case Element.Content(obj, rawStream, stream, _) =>
        if !Content.mayRewriteFilters(obj.data) then
          (List(Part.Obj(IndirectObj(obj, Some(rawStream)))), state)
        else
          stream.exec match {
            case Attempt.Failure(err) =>
              throw new RuntimeException(s"uncompress obj ${obj.index.number}: ${err.messageWithContext}")
            case Attempt.Successful(bits) =>
              Content.compressFlate(obj.data, f(bits)) match {
                case Attempt.Failure(err) =>
                  throw new RuntimeException(s"FlateEncode obj ${obj.index.number}: ${err.messageWithContext}")
                case Attempt.Successful((dict, compressed)) =>
                  (
                    List(Part.Obj(IndirectObj(Obj(obj.index, dict), Some(compressed)))),
                    state
                  )
              }
          }
    }

  /** Validate a PDF byte stream and return either Unit or a
    * non-empty list of errors. */
  def validate(enableDiagnostics: Boolean = false)(
    bytes: zio.stream.ZStream[Any, Throwable, Byte]
  ): zio.ZIO[Any, Throwable, zio.prelude.Validation[PdfError, Unit]] =
    ValidatePdf.fromDecoded(bytes.via(decode(enableDiagnostics)))

  /** Structural compliance ([[PdfPolicy]]) over a decoded byte stream. */
  def policy(rules: PdfPolicy = PdfPolicy.strict, enableDiagnostics: Boolean = false)(
    bytes: zio.stream.ZStream[Any, Throwable, Byte]
  ): zio.ZIO[Any, Throwable, zio.prelude.Validation[PolicyViolation, Unit]] =
    PdfPolicy.fromDecoded(rules)(bytes.via(decode(enableDiagnostics)))

  /** Decode elements then extract literal page text. */
  def extractText(enableDiagnostics: Boolean = false): ZPipeline[Any, Throwable, Byte, PageText] =
    ZPipeline.fromFunction[Any, Throwable, Byte, PageText] { bytes =>
      ZStream.unwrap {
        bytes.via(elements(enableDiagnostics)).runCollect.map { elems =>
          ZStream.fromChunk(TextExtract.fromElements(elems))
        }
      }
    }

  /** Compare two PDF byte streams structurally. */
  def compare(enableDiagnostics: Boolean = false)(
    old: zio.stream.ZStream[Any, Throwable, Byte],
    updated: zio.stream.ZStream[Any, Throwable, Byte]
  ): zio.ZIO[Any, Throwable, zio.prelude.Validation[CompareError, Unit]] =
    ComparePdfs.fromBytes(enableDiagnostics)(old, updated)
}
