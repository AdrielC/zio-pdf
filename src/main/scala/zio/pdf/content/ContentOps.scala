/*
 * Best-effort PDF content-stream tokenizer (page operators).
 */

package zio.pdf.content

import scala.collection.mutable.ArrayBuffer

import _root_.scodec.bits.ByteVector

sealed trait ContentToken

object ContentToken {
  final case class Number(value: BigDecimal)                    extends ContentToken
  final case class Name(value: String)                          extends ContentToken
  final case class Literal(bytes: ByteVector)                   extends ContentToken
  final case class Hex(bytes: ByteVector)                       extends ContentToken
  final case class Array(elems: List[ContentToken])             extends ContentToken
  final case class Dict(entries: List[(String, ContentToken)])  extends ContentToken
  final case class Op(name: String)                             extends ContentToken
  case object Null                                              extends ContentToken
  final case class Bool(value: Boolean)                         extends ContentToken
}

object ContentOps {

  /**
   * A deliberately conservative structural hint, not semantic table
   * recognition. This lexical probe does not allocate a token tree for every
   * general stream: PDFs often carry large non-page payloads. It skips strings,
   * names, hex data, and comments, so text such as `(re Tj)` cannot be
   * mistaken for drawing commands.
   */
  def looksLikeTable(bytes: Array[Byte]): Boolean = {
    var rectangle = false
    var text      = false
    var i         = 0

    def at(index: Int): Int = if index < bytes.length then bytes(index) & 0xff else -1
    def isWhitespace(byte: Int): Boolean =
      byte == ' ' || byte == '\n' || byte == '\r' || byte == '\t' || byte == '\f' || byte == 0
    def isDelimiter(byte: Int): Boolean =
      isWhitespace(byte) || byte == '(' || byte == ')' || byte == '<' || byte == '>' ||
        byte == '[' || byte == ']' || byte == '{' || byte == '}' || byte == '/' || byte == '%'
    def skipLiteral(start: Int): Int = {
      var cursor = start + 1
      var depth  = 1
      while cursor < bytes.length && depth > 0 do
        at(cursor) match {
          case '\\' => cursor += 2
          case '('  =>
            depth += 1
            cursor += 1
          case ')'  =>
            depth -= 1
            cursor += 1
          case _    => cursor += 1
        }
      cursor
    }
    def skipUntil(start: Int, end: Int): Int = {
      var cursor = start
      while cursor < bytes.length && at(cursor) != end do cursor += 1
      math.min(cursor + 1, bytes.length)
    }
    def skipComment(start: Int): Int = {
      var cursor = start
      while cursor < bytes.length && at(cursor) != '\n' && at(cursor) != '\r' do cursor += 1
      cursor
    }

    while i < bytes.length && !(rectangle && text) do
      at(i) match {
        case byte if isWhitespace(byte) => i += 1
        case '%'                        => i = skipComment(i + 1)
        case '('                        => i = skipLiteral(i)
        case '<'                        => i = skipUntil(i + 1, '>')
        case '/'                        =>
          i += 1
          while i < bytes.length && !isDelimiter(at(i)) do i += 1
        case '\'' | '\"' =>
          text = true
          i += 1
        case ')' | '>' | '[' | ']' | '{' | '}' => i += 1
        case _ =>
          val start = i
          while i < bytes.length && !isDelimiter(at(i)) do i += 1
          val length = i - start
          if length == 2 && at(start) == 'r' && at(start + 1) == 'e' then rectangle = true
          else if
            length == 2 && at(start) == 'T' && (at(start + 1) == 'j' || at(start + 1) == 'J')
          then text = true
      }

    rectangle && text
  }

  def tokenize(bytes: Array[Byte]): List[ContentToken] = {
    val out = ArrayBuffer.empty[ContentToken]
    var i   = 0
    val n   = bytes.length

    def peek: Int = if i < n then bytes(i) & 0xff else -1
    def adv(): Int = {
      val b = peek
      i += 1
      b
    }
    def isWs(b: Int): Boolean =
      b == ' ' || b == '\n' || b == '\r' || b == '\t' || b == '\f' || b == 0
    def skipWs(): Unit =
      while isWs(peek) || peek == '%' do
        if peek == '%' then while i < n && bytes(i) != '\n' && bytes(i) != '\r' do i += 1
        else i += 1

    def readNumber(): ContentToken.Number = {
      val start = i
      if peek == '+' || peek == '-' then i += 1
      while { val c = peek; c >= '0' && c <= '9' } do i += 1
      if peek == '.' then
        i += 1
        while { val c = peek; c >= '0' && c <= '9' } do i += 1
      val s = new String(bytes, start, i - start, java.nio.charset.StandardCharsets.US_ASCII)
      ContentToken.Number(BigDecimal(s))
    }

    def readName(): ContentToken.Name = {
      i += 1
      val start = i
      while i < n && !isWs(peek) && "/<>[](){}".indexOf(peek.toChar) < 0 do i += 1
      val raw = new String(bytes, start, i - start, java.nio.charset.StandardCharsets.ISO_8859_1)
      ContentToken.Name(unescapeName(raw))
    }

    def readLiteral(): ContentToken.Literal = {
      i += 1
      val buf   = ArrayBuffer.empty[Byte]
      var depth = 1
      while i < n && depth > 0 do
        val b = adv()
        if b == '\\' then
          if i < n then buf += adv().toByte
        else if b == '(' then
          depth += 1
          buf += '('.toByte
        else if b == ')' then
          depth -= 1
          if depth > 0 then buf += ')'.toByte
        else buf += b.toByte
      ContentToken.Literal(ByteVector(buf.toArray))
    }

    def readHex(): ContentToken.Hex = {
      i += 1
      val nibbles = ArrayBuffer.empty[Int]
      while i < n && peek != '>' do
        val b = adv()
        if !isWs(b) then
          val v =
            if b >= '0' && b <= '9' then b - '0'
            else if b >= 'A' && b <= 'F' then b - 'A' + 10
            else if b >= 'a' && b <= 'f' then b - 'a' + 10
            else -1
          if v >= 0 then nibbles += v
      if peek == '>' then i += 1
      if nibbles.size % 2 == 1 then nibbles += 0
      val outb = new Array[Byte](nibbles.size / 2)
      var k    = 0
      while k < outb.length do
        outb(k) = ((nibbles(k * 2) << 4) | nibbles(k * 2 + 1)).toByte
        k += 1
      ContentToken.Hex(ByteVector(outb))
    }

    def readArray(): ContentToken.Array = {
      i += 1
      val elems = ArrayBuffer.empty[ContentToken]
      skipWs()
      while i < n && peek != ']' do
        elems += readOne()
        skipWs()
      if peek == ']' then i += 1
      ContentToken.Array(elems.toList)
    }

    def readDict(): ContentToken.Dict = {
      i += 2
      val entries = ArrayBuffer.empty[(String, ContentToken)]
      skipWs()
      while i < n && !(peek == '>' && i + 1 < n && (bytes(i + 1) & 0xff) == '>') do
        skipWs()
        if peek == '/' then
          val ContentToken.Name(key) = readName(): @unchecked
          skipWs()
          entries += (key -> readOne())
        else if peek != '>' then i += 1
        skipWs()
      if peek == '>' then i += 2
      ContentToken.Dict(entries.toList)
    }

    def readOpOrKeyword(): ContentToken = {
      val start = i
      while i < n && !isWs(peek) && "/<>[](){}".indexOf(peek.toChar) < 0 do i += 1
      val s = new String(bytes, start, i - start, java.nio.charset.StandardCharsets.US_ASCII)
      s match {
        case "null"  => ContentToken.Null
        case "true"  => ContentToken.Bool(true)
        case "false" => ContentToken.Bool(false)
        case other   => ContentToken.Op(other)
      }
    }

    def readOne(): ContentToken = {
      skipWs()
      peek match {
        case -1                                          => ContentToken.Op("")
        case b if b == '/'                               => readName()
        case b if b == '('                               => readLiteral()
        case b if b == '['                               => readArray()
        case b if b == '<' =>
          if i + 1 < n && (bytes(i + 1) & 0xff) == '<' then readDict()
          else readHex()
        case b if b == '+' || b == '-' || b == '.' || (b >= '0' && b <= '9') =>
          readNumber()
        case _ => readOpOrKeyword()
      }
    }

    skipWs()
    while i < n do
      out += readOne()
      skipWs()
    out.toList
  }

  private def unescapeName(raw: String): String = {
    val sb = new StringBuilder(raw.length)
    var i  = 0
    while i < raw.length do
      if raw.charAt(i) == '#' && i + 2 < raw.length then
        try {
          sb.append(Integer.parseInt(raw.substring(i + 1, i + 3), 16).toChar)
          i += 3
        } catch {
          case _: NumberFormatException =>
            sb.append(raw.charAt(i))
            i += 1
        }
      else {
        sb.append(raw.charAt(i))
        i += 1
      }
    sb.toString
  }

  def tokenText(t: ContentToken): Option[String] =
    t match {
      case ContentToken.Literal(b) => Some(new String(b.toArray, java.nio.charset.StandardCharsets.ISO_8859_1))
      case ContentToken.Hex(b)     => Some(new String(b.toArray, java.nio.charset.StandardCharsets.ISO_8859_1))
      case _                       => None
    }

  def tjArrayText(arr: ContentToken.Array): String =
    arr.elems.flatMap(tokenText).mkString
}
