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
