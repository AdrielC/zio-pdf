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
    final case class Failed(error: Throwable) extends LexResult
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
  private val objKeyword: ByteVector = ByteVector.view("obj".getBytes(StandardCharsets.US_ASCII))
  private val xrefKeyword: ByteVector = ByteVector.view("xref".getBytes(StandardCharsets.US_ASCII))
  private val eofKeyword: ByteVector = ByteVector.view("%%EOF".getBytes(StandardCharsets.US_ASCII))

  private enum RawLength:
    case Direct(value: Long)
    case Indirect(reference: Prim.Ref)

  private def keywordAt(bytes: ByteVector, at: Int, keyword: ByteVector): Boolean =
    at >= 0 && at.toLong + keyword.size <= bytes.size && bytes.slice(at, at + keyword.size.toInt) == keyword

  private def parseLong(bytes: ByteVector, from: Int, until: Int): Either[Throwable, Long] =
    try Right(new String(bytes.slice(from, until).toArray, StandardCharsets.US_ASCII).toLong)
    catch case error: NumberFormatException => Left(error)

  /** Find a top-level direct or indirect `/Length` inside one complete dictionary. */
  private def rawLength(bytes: ByteVector, from: Int, until: Int): Either[Throwable, RawLength] =
    var index        = from
    var depth        = 1
    var literalDepth = 0
    var escaped      = false
    var hexString    = false
    var comment      = false

    while index < until do
      val byte = bytes(index)
      if comment then
        if byte == '\n'.toByte || byte == '\r'.toByte then comment = false
        index += 1
      else if literalDepth > 0 then
        if escaped then escaped = false
        else if byte == '\\'.toByte then escaped = true
        else if byte == '('.toByte then literalDepth += 1
        else if byte == ')'.toByte then literalDepth -= 1
        index += 1
      else if hexString then
        if byte == '>'.toByte then hexString = false
        index += 1
      else if byte == '%'.toByte then
        comment = true
        index += 1
      else if byte == '('.toByte then
        literalDepth = 1
        index += 1
      else if byte == '<'.toByte && index + 1 < until && bytes(index + 1) == '<'.toByte then
        depth += 1
        index += 2
      else if byte == '>'.toByte && index + 1 < until && bytes(index + 1) == '>'.toByte then
        depth -= 1
        index += 2
      else if byte == '<'.toByte then
        hexString = true
        index += 1
      else if depth == 1 && byte == '/'.toByte then
        val nameStart = index + 1
        var nameEnd   = nameStart
        while nameEnd < until && !isWs(bytes(nameEnd)) && !"/[]<>()".contains(bytes(nameEnd).toChar) do
          nameEnd += 1
        val name = new String(bytes.slice(nameStart, nameEnd).toArray, StandardCharsets.US_ASCII)
        if name == "Length" then
          val firstStart = skipWs(bytes, nameEnd)
          val (_, firstEnd) = readDigits(bytes, firstStart)
          if firstEnd == firstStart then
            return Left(IllegalArgumentException("stream /Length is not numeric"))
          parseLong(bytes, firstStart, firstEnd) match
            case Left(error) => return Left(error)
            case Right(first) =>
              val secondStart = skipWs(bytes, firstEnd)
              val (_, secondEnd) = readDigits(bytes, secondStart)
              if secondEnd > secondStart then
                val marker = skipWs(bytes, secondEnd)
                if marker < until && bytes(marker) == 'R'.toByte then
                  parseLong(bytes, secondStart, secondEnd) match
                    case Left(error) => return Left(error)
                    case Right(generation) if generation <= Int.MaxValue.toLong =>
                      return Right(RawLength.Indirect(Prim.Ref(first, generation.toInt)))
                    case Right(_) => return Left(IllegalArgumentException("stream /Length generation overflows Int"))
              return Right(RawLength.Direct(first))
        index = nameEnd
      else index += 1

    Left(IllegalArgumentException("stream dictionary has no /Length"))

  /**
   * Boundary-mode fallback for compact dictionaries rejected by the semantic
   * scodec parser. It recognizes only a complete dictionary followed by a
   * stream marker and extracts no metadata beyond object index and /Length.
   */
  private def tryRawDictionaryStream(bytes: ByteVector): Option[LexResult] =
    if bytes.isEmpty || !isDigit(bytes(0)) then None
    else
      val (numberStart, numberEnd) = readDigits(bytes, 0)
      val generationStart         = skipWs(bytes, numberEnd)
      val (_, generationEnd)      = readDigits(bytes, generationStart)
      val objectMarker            = skipWs(bytes, generationEnd)
      if numberEnd == numberStart || generationEnd == generationStart then Some(LexResult.NeedMore)
      else if !keywordAt(bytes, objectMarker, objKeyword) then Some(LexResult.NeedMore)
      else
        val dictionaryStart = skipWs(bytes, objectMarker + objKeyword.size.toInt)
        if dictionaryStart + 2 > bytes.size then Some(LexResult.NeedMore)
        else if bytes(dictionaryStart) != '<'.toByte || bytes(dictionaryStart + 1) != '<'.toByte then None
        else
          var index        = dictionaryStart + 2
          var depth        = 1
          var literalDepth = 0
          var escaped      = false
          var hexString    = false
          var comment      = false
          var dictionaryEnd = -1
          while index < bytes.size && dictionaryEnd < 0 do
            val byte = bytes(index)
            if comment then
              if byte == '\n'.toByte || byte == '\r'.toByte then comment = false
              index += 1
            else if literalDepth > 0 then
              if escaped then escaped = false
              else if byte == '\\'.toByte then escaped = true
              else if byte == '('.toByte then literalDepth += 1
              else if byte == ')'.toByte then literalDepth -= 1
              index += 1
            else if hexString then
              if byte == '>'.toByte then hexString = false
              index += 1
            else if byte == '%'.toByte then
              comment = true
              index += 1
            else if byte == '('.toByte then
              literalDepth = 1
              index += 1
            else if byte == '<'.toByte && index + 1 < bytes.size && bytes(index + 1) == '<'.toByte then
              depth += 1
              index += 2
            else if byte == '>'.toByte && index + 1 < bytes.size && bytes(index + 1) == '>'.toByte then
              depth -= 1
              index += 2
              if depth == 0 then dictionaryEnd = index
            else if byte == '<'.toByte then
              hexString = true
              index += 1
            else index += 1

          if dictionaryEnd < 0 then Some(LexResult.NeedMore)
          else
            val marker = skipWs(bytes, dictionaryEnd)
            val tail   = bytes.drop(marker)
            if streamKeyword.take(tail.size) == tail then Some(LexResult.NeedMore)
            else if !tail.startsWith(streamKeyword) then None
            else
              val afterKeyword = streamKeyword.size.toInt
              val newline =
                if tail.size <= afterKeyword then 0
                else if tail(afterKeyword) == '\n'.toByte then 1
                else if tail(afterKeyword) == '\r'.toByte && tail.size == afterKeyword + 1 then 0
                else if tail(afterKeyword) == '\r'.toByte && tail(afterKeyword + 1) == '\n'.toByte then 2
                else 0
              if newline == 0 then Some(LexResult.NeedMore)
              else
                (parseLong(bytes, numberStart, numberEnd), parseLong(bytes, generationStart, generationEnd)) match
                  case (Right(number), Right(generation)) if generation <= Int.MaxValue.toLong =>
                    val objectIndex = Obj.Index(number, generation.toInt)
                    rawLength(bytes, dictionaryStart + 2, dictionaryEnd - 2) match
                      case Right(RawLength.Direct(length)) =>
                        Some(
                          LexResult.Ok(
                            HeaderEvent.H(IndirectObj.IndirectObjHeader(Obj(objectIndex, Prim.Dict.empty), Some(length))),
                            tail.drop(afterKeyword + newline)
                          )
                        )
                      case Right(RawLength.Indirect(reference)) =>
                        Some(LexResult.Failed(StreamingDecode.UnresolvedIndirectStreamLength(objectIndex, reference)))
                      case Left(error) =>
                        Some(LexResult.Failed(StreamingDecode.InvalidDeclaredStreamLength(objectIndex, error.getMessage)))
                  case (_, Right(generation)) if generation > Int.MaxValue.toLong =>
                    Some(LexResult.Failed(IllegalArgumentException("object generation overflows Int")))
                  case (Right(_), Right(_)) =>
                    Some(LexResult.Failed(IllegalArgumentException("object generation is invalid")))
                  case (Left(error), _) => Some(LexResult.Failed(error))
                  case (_, Left(error)) => Some(LexResult.Failed(error))

  /** Boundary mode does not need xref rows, only to advance to the next revision. */
  private def tryRawXrefSection(bytes: ByteVector): Option[LexResult] =
    if !bytes.startsWith(xrefKeyword) then None
    else
      bytes.indexOfSlice(eofKeyword) match
        case offset if offset >= 0L =>
          Some(
            LexResult.Ok(
              HeaderEvent.W(' '.toByte),
              bytes.drop(offset + eofKeyword.size)
            )
          )
        case _ => Some(LexResult.NeedMore)

  /**
   * Decode through the object dictionary, then recognise a stream marker
   * directly. This keeps the lexical boundary explicit: optional whitespace
   * may precede `stream`, while its following line ending must be LF or CRLF.
   * The previous optional scodec branch could classify a stream object as
   * non-stream and then fail at its `stream` keyword while looking for `endobj`.
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
                  // A CR on the final byte may be the first half of CRLF.
                  // Keep the header in carry until the next byte disambiguates it.
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
                case Attempt.Failure(error) =>
                  Content.streamLengthRef(obj.data) match
                    case Some(reference) =>
                      Some(LexResult.Failed(StreamingDecode.UnresolvedIndirectStreamLength(obj.index, reference)))
                    case None =>
                      Some(
                        LexResult.Failed(
                          StreamingDecode.InvalidDeclaredStreamLength(obj.index, error.messageWithContext)
                        )
                      )
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
  def next(bytes: ByteVector, allowRawDictionaryStream: Boolean = false): LexResult =
    tryWhitespace(bytes)
      .orElse(tryVersion(bytes))
      .orElse(tryComment(bytes))
      .orElse(tryIndirectObject(bytes))
      .orElse(Option.when(allowRawDictionaryStream)(tryRawDictionaryStream(bytes)).flatten)
      .orElse(Option.when(allowRawDictionaryStream)(tryRawXrefSection(bytes)).flatten)
      .getOrElse(scodecDecode(bytes))
}
