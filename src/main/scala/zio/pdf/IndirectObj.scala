/*
 * Port of fs2.pdf.IndirectObj to Scala 3 + scodec 2.3.
 *
 * An indirect object: `<num> <gen> obj <data> [stream <bytes> endstream] endobj`.
 */

package zio.pdf

import zio.pdf.codec.{Codecs, Newline, Text, Whitespace}
import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder}
import _root_.scodec.bits.{BitVector, ByteVector}

final case class IndirectObj(obj: Obj, stream: Option[BitVector])

object IndirectObj extends IndirectObjCodec {

  def nostream(number: Long, data: Prim): IndirectObj =
    IndirectObj(Obj(Obj.Index(number, 0), data), None)

  private[pdf] def addLength(stream: BitVector): Prim => Prim = {
    case Prim.Dict(data) => Prim.Dict(data.updated("Length", Prim.Number(stream.bytes.size)))
    case other            => other
  }

  private[pdf] def ensureLength(stream: BitVector)(data: Prim): Prim =
    Prim.tryDict("Length")(data) match {
      case Some(_) => data
      case None    => addLength(stream)(data)
    }

  def stream(number: Long, data: Prim, payload: BitVector): IndirectObj =
    IndirectObj(Obj(Obj.Index(number, 0), ensureLength(payload)(data)), Some(payload))

  object number {
    def unapply(o: IndirectObj): Option[Long] = Some(o.obj.index.number)
  }

  object dict {
    def unapply(o: IndirectObj): Option[(Long, Prim.Dict)] = o match {
      case IndirectObj(Obj.dict(num, d), _) => Some((num, d))
      case _                                 => None
    }
  }
}

private[pdf] trait IndirectObjCodec {
  import _root_.scodec.codecs.{bits, choice, constant, optional, recover}
  import Newline.{crlf, lf, stripNewline}
  import Whitespace.{nlWs, ws}
  import Text.str

  /**
   * The standard requires the newline after `stream` to be either
   * `\n` or `\r\n` (not bare `\r`).
   */
  val streamStartKeyword: Codec[Unit] = str("stream") <~ choice(lf, crlf)

  val endstreamTest: Codec[Unit] = ws ~> constant(Content.endstream)

  /**
   * Find the end of a content stream. We trust `/Length` if it
   * lines up with `endstream`; otherwise we fall back to scanning
   * for `endstream` and stripping a trailing newline.
   */
  def stripStream(data: Prim)(bytes: ByteVector): Attempt[DecodeResult[BitVector]] =
    for {
      end    <- Content.endstreamIndex(bytes)
      length <- Content.streamLength(data)
    } yield {
      val payloadByLength   = bytes.take(length).bits
      val remainderByLength = bytes.drop(length).bits
      endstreamTest.decode(remainderByLength) match {
        case Attempt.Successful(_) =>
          DecodeResult(payloadByLength, remainderByLength)
        case Attempt.Failure(_) =>
          val payload = stripNewline(bytes.take(end))
          DecodeResult(payload.bits, bytes.drop(payload.size).bits)
      }
    }

  def streamPayload(data: Prim): Codec[BitVector] =
    Codec(bits, Decoder(b => stripStream(data)(b.bytes)))

  def streamCodec(data: Prim): Codec[Option[BitVector]] =
    optional(
      recover(streamStartKeyword),
      streamPayload(data) <~ nlWs <~ str("endstream") <~ nlWs
    )

  val objHeader: Codec[Obj.Index] =
    Whitespace.skipWs ~> summon[Codec[Obj.Index]] <~ nlWs

  val prim: Codec[Prim] = Prim.Codec_Prim <~ nlWs

  val endobj: Codec[Unit] = str("endobj") <~ nlWs

  val preStream: Codec[Obj] =
    (objHeader :: prim).xmap({ case (i, p) => Obj(i, p) }, o => (o.index, o.data))

  given Codec[IndirectObj] =
    (preStream
      .flatZip(o => streamCodec(o.data))
      <~ endobj)
      .xmap[IndirectObj](
        { case (o, s) => IndirectObj(o, s) },
        i => (i.obj, i.stream)
      )

  // -------------------------------------------------------------------
  // Header-only decoder for memory-bounded streaming.
  //
  // The standard `Codec[IndirectObj]` materialises the entire content
  // stream's payload as a `BitVector` so it can resolve `/Length` and
  // confirm the trailing `endstream`. That works fine for small/medium
  // PDFs but blows the heap on multi-GB content streams (large
  // attachments, embedded fonts, etc.).
  //
  // `headerOnly` decodes just the part of an indirect object that
  // comes *before* the stream payload:
  //
  //   <num> <gen> obj
  //   <data dict>
  //   stream\n           <-- only present iff the object has a stream
  //
  // and yields an `IndirectObjHeader` carrying the parsed `Obj` plus,
  // for stream-bearing objects, the declared `/Length`. The caller is
  // then responsible for consuming exactly `length` bytes from the
  // remainder of the byte stream and then re-entering the top-level
  // pipeline at `\nendstream\nendobj`.
  // -------------------------------------------------------------------

  /** Header of an indirect object as recognised by `headerOnly`. */
  final case class IndirectObjHeader(obj: Obj, streamLength: Option[Long])

  /** Decoder for the header part of an indirect object. Stops *just
    * after* the `stream\n` (or `stream\r\n`) keyword for a
    * stream-bearing object, or after `endobj\n` for a no-stream
    * object. */
  val headerOnly: Codec[IndirectObjHeader] = {
    val headerCodec: Codec[(Obj, Option[Unit])] =
      preStream
        .flatZip(_ => optional(recover(streamStartKeyword), constant(ByteVector.empty)))
    headerCodec.exmap(
      {
        case (obj, Some(())) =>
          // Stream-bearing object: pull /Length out of the data dict.
          Content.streamLength(obj.data).map(len => IndirectObjHeader(obj, Some(len)))
        case (obj, None) =>
          // No stream - we still need to consume the trailing `endobj`.
          // We cannot do that inside this codec because we are
          // header-only. Instead callers handle it by consuming the
          // bytes that precede `endobj` (i.e. nothing) and then the
          // `endobj` keyword itself.
          // For symmetry with the stream-bearing case, return a
          // length of 0 and let the streaming pipeline consume `endobj`
          // unconditionally.
          Attempt.successful(IndirectObjHeader(obj, None))
      },
      _ => Attempt.failure(_root_.scodec.Err("IndirectObj.headerOnly is decode-only"))
    )
  }

  /** Codec for the trailer of a stream-bearing object: the literal
    * `endstream` keyword (preceded by newline / whitespace) followed
    * by `endobj`. Used by the streaming pipeline once it has
    * forwarded `length` bytes. */
  val streamTrailer: Codec[Unit] =
    nlWs ~> str("endstream") ~> nlWs ~> endobj
}

/** Encoded indirect object + xref metadata. */
final case class EncodedObj(xref: XrefObjMeta, bytes: ByteVector)

object EncodedObj {
  def indirect(obj: IndirectObj): Attempt[EncodedObj] =
    Codecs.encodeBytes(obj).map(b => EncodedObj(XrefObjMeta(obj.obj.index, b.size), b))
}

/** Tiny record carrying just enough info to build an xref entry. */
final case class XrefObjMeta(index: Obj.Index, size: Long)
