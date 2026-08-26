/*
 * Byte-native top-level PDF lexer for [[StreamingDecode.WaitingHeader]].
 *
 * Fast paths avoid scodec `BitVector` choice on whitespace, `%PDF-`,
 * and `%` comments. Everything else (xref blocks, indirect-object
 * headers, startxref) falls back to the same scodec decoders as before.
 */

package zio.pdf

import _root_.scodec.{Attempt, DecodeResult, Decoder, Err}
import _root_.scodec.bits.ByteVector

import java.nio.charset.StandardCharsets

private[pdf] object PdfByteLexer {

  sealed trait HeaderEvent
  object HeaderEvent {
    final case class V(v: Version)                      extends HeaderEvent
    final case class C(b: ByteVector)                 extends HeaderEvent
    final case class S(s: StartXref)                  extends HeaderEvent
    final case class X(x: Xref)                       extends HeaderEvent
    final case class W(b: Byte)                       extends HeaderEvent
    final case class H(o: IndirectObj.IndirectObjHeader) extends HeaderEvent
  }

  sealed trait LexResult
  object LexResult {
    case object NeedMore extends LexResult
    final case class Ok(event: HeaderEvent, rest: ByteVector) extends LexResult
  }

  private val scodecDecoder: Decoder[HeaderEvent] =
    Decoder.choiceDecoder(
      Version.codec.map(HeaderEvent.V(_)),
      summon[_root_.scodec.Codec[Xref]].map(HeaderEvent.X(_)),
      StartXref.codec.map(HeaderEvent.S(_)),
      (Comment.start ~> Comment.line).map(HeaderEvent.C(_)),
      Decoder { bits =>
        if (bits.size < 8L) Attempt.failure(Err.InsufficientBits(8L, bits.size, Nil))
        else {
          val (head, rest) = bits.splitAt(8L)
          val byte         = head.bytes.head
          if (byte == ' '.toByte || byte == '\n'.toByte || byte == '\r'.toByte || byte == '\t'.toByte)
            Attempt.successful(DecodeResult(HeaderEvent.W(byte), rest))
          else
            Attempt.failure(Err(s"streaming top-level: unrecognised byte ${byte.toInt & 0xff}"))
        }
      }
    )

  private def isWs(b: Byte): Boolean =
    b == ' '.toByte || b == '\n'.toByte || b == '\r'.toByte || b == '\t'.toByte

  private def isDigit(b: Byte): Boolean =
    b >= '0'.toByte && b <= '9'.toByte

  private def newlineSize(bytes: ByteVector, at: Int): Int =
    if (at >= bytes.size) 0
    else if (bytes(at) == '\r'.toByte && at + 1 < bytes.size && bytes(at + 1) == '\n'.toByte) 2
    else if (bytes(at) == '\n'.toByte || bytes(at) == '\r'.toByte) 1
    else 0

  /** Index of the first newline byte at or after `from`, or -1 if none. */
  private def findNewline(bytes: ByteVector, from: Int): Int = {
    var i = from
    while (i < bytes.size) {
      if (newlineSize(bytes, i) > 0) return i
      i += 1
    }
    -1
  }

  private def skipPastNewline(bytes: ByteVector, nlAt: Int): Int =
    nlAt + newlineSize(bytes, nlAt)

  private def skipWs(bytes: ByteVector, from: Int): Int = {
    var i = from
    while (i < bytes.size && isWs(bytes(i))) i += 1
    i
  }

  private def readDigits(bytes: ByteVector, from: Int): (Int, Int) = {
    var i = from
    while (i < bytes.size && isDigit(bytes(i))) i += 1
    (from, i)
  }

  private def tryWhitespace(bytes: ByteVector): Option[LexResult] =
    if (bytes.nonEmpty && isWs(bytes(0)))
      Some(LexResult.Ok(HeaderEvent.W(bytes(0)), bytes.drop(1)))
    else
      None

  private val pdfMagic: ByteVector = ByteVector.view("%PDF-".getBytes)
  private val streamKeyword: ByteVector = ByteVector.view("stream".getBytes(StandardCharsets.US_ASCII))

  /** Decode through the object dictionary, then recognise a stream marker
    * directly. The following line ending must be LF or CRLF, and a CR split
    * across input chunks remains in carry until it can be disambiguated.
    */
  private def tryIndirectObject(bytes: ByteVector): Option[LexResult] =
    if (bytes.isEmpty || !isDigit(bytes(0))) None
    else
      IndirectObj.preStream.decode(bytes.bits) match
        case Attempt.Successful(DecodeResult(obj, remainder)) =>
          val rest   = remainder.bytes
          val offset = skipWs(rest, 0)
          val tail   = rest.drop(offset)
          if (tail.startsWith(streamKeyword)) then
            val afterKeyword = streamKeyword.size.toInt
            val newline =
              if tail.size <= afterKeyword then 0
              else
                tail(afterKeyword) match
                  case '\n' => 1
                  case '\r' if tail.size == afterKeyword + 1 => 0
                  case '\r' if tail(afterKeyword + 1) == '\n' => 2
                  case _ => 0
            if newline == 0 then Some(LexResult.NeedMore)
            else
              Content.streamLength(obj.data) match
                case Attempt.Successful(length) =>
                  Some(
                    LexResult.Ok(
                      HeaderEvent.H(IndirectObj.IndirectObjHeader(obj, Some(length))),
                      tail.drop(afterKeyword + newline)
                    )
                  )
                case Attempt.Failure(_) => Some(LexResult.NeedMore)
          else if streamKeyword.take(tail.size) == tail then Some(LexResult.NeedMore)
          else Some(LexResult.Ok(HeaderEvent.H(IndirectObj.IndirectObjHeader(obj, None)), tail))
        case Attempt.Failure(_) => None

  private def tryVersion(bytes: ByteVector): Option[LexResult] = {
    if (!bytes.startsWith(pdfMagic)) None
    else if (bytes.size < pdfMagic.size + 3) Some(LexResult.NeedMore)
    else {
      val (majFrom, majTo) = readDigits(bytes, pdfMagic.size.toInt)
      if (majTo == majFrom) Some(LexResult.NeedMore)
      else if (majTo >= bytes.size || bytes(majTo) != '.'.toByte) Some(LexResult.NeedMore)
      else {
        val (minFrom, minTo) = readDigits(bytes, majTo + 1)
        if (minTo == minFrom) Some(LexResult.NeedMore)
        else {
          val major = new String(bytes.slice(majFrom, majTo).toArray).toInt
          val minor = new String(bytes.slice(minFrom, minTo).toArray).toInt
          var p     = skipWs(bytes, minTo)
          var binaryMarker: Option[ByteVector] = None
          if (p < bytes.size && bytes(p) == '%'.toByte && (p + 1 >= bytes.size || bytes(p + 1) != '%'.toByte)) {
            val lineStart = p + 1
            val nl        = findNewline(bytes, lineStart)
            if (nl < 0) Some(LexResult.NeedMore)
            else {
              binaryMarker = Some(bytes.slice(lineStart, nl))
              p = skipPastNewline(bytes, nl)
              Some(LexResult.Ok(HeaderEvent.V(Version(major, minor, binaryMarker)), bytes.drop(p)))
            }
          } else
            Some(LexResult.Ok(HeaderEvent.V(Version(major, minor, binaryMarker)), bytes.drop(p)))
        }
      }
    }
  }

  private def tryComment(bytes: ByteVector): Option[LexResult] =
    if (bytes.isEmpty || bytes(0) != '%'.toByte) None
    else if (bytes.size >= 2 && bytes(1) == '%'.toByte) None
    else {
      val nl = findNewline(bytes, 1)
      if (nl < 0) Some(LexResult.NeedMore)
      else {
        val body = bytes.slice(1, nl)
        val rest = bytes.drop(skipPastNewline(bytes, nl))
        Some(LexResult.Ok(HeaderEvent.C(body), rest))
      }
    }

  private def scodecDecode(bytes: ByteVector): LexResult =
    scodecDecoder.decode(bytes.bits) match {
      case Attempt.Successful(DecodeResult(ev, rest)) =>
        LexResult.Ok(ev, rest.bytes)
      case Attempt.Failure(_) =>
        // Match legacy [[StreamingDecode.streamingHeaderDecoder]]: any
        // decode failure means we need more bytes in carry, not a hard error.
        LexResult.NeedMore
    }

  /** Scan the next top-level header event from `bytes` (no carry prefix). */
  def next(bytes: ByteVector): LexResult =
    tryWhitespace(bytes)
      .orElse(tryVersion(bytes))
      .orElse(tryComment(bytes))
      .orElse(tryIndirectObject(bytes))
      .getOrElse(scodecDecode(bytes))
}
