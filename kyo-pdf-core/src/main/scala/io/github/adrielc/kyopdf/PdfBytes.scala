package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1
import io.github.adrielc.bytescan.SharedScan.ByteReader

/** Shared random-access PDF input. Only tokens and retained output payloads are
  * copied. A view never converts the complete document to characters.
  * All instances are admitted through a ByteLimit before entering a parser.
  */
private[kyopdf] final class PdfBytes private (
    val length: Int, array: Array[Byte], offset: Int, read: ByteReader):
  private def byteAt(index: Int): Byte = if array != null then array(offset + index) else read(index)
  def charAt(index: Int): Char = (byteAt(index) & 255).toChar
  def startsWith(value: String, at: Int = 0): Boolean =
    if at < 0 || value.length > length - at then false
    else
      var i = 0
      while i < value.length && charAt(at + i) == value.charAt(i) do i += 1
      i == value.length
  def indexOf(value: String, from: Int = 0): Int =
    var at = math.max(0, from)
    while at <= length - value.length do
      if startsWith(value, at) then return at
      at += 1
    -1
  def indexWhere(predicate: Char => Boolean, from: Int): Int =
    var at = math.max(0, from)
    while at < length do
      if predicate(charAt(at)) then return at
      at += 1
    -1
  def lastIndexOf(value: String): Int =
    var at = length - value.length
    while at >= 0 do
      if startsWith(value, at) then return at
      at -= 1
    -1
  def substring(from: Int, until: Int): String =
    // Tokens are owned Strings; array input uses the charset constructor's
    // direct byte path instead of allocating an intermediate character array.
    if array != null then new String(array, offset + from, until - from, ISO_8859_1)
    else
      val chars = new Array[Char](until - from)
      var i = 0
      while i < chars.length do
        chars(i) = charAt(from + i)
        i += 1
      new String(chars)
  def slice(from: Int, until: Int): PdfBytes =
    if array != null then new PdfBytes(until - from, array, offset + from, null)
    else new PdfBytes(until - from, null, 0, index => byteAt(from + index))
  def payload(from: Int, until: Int): Vector[Byte] = Vector.tabulate(until - from)(index => byteAt(from + index))
  def copyBytes(from: Int, until: Int): Array[Byte] =
    val bytes = new Array[Byte](until - from)
    var i = 0
    while i < bytes.length do
      bytes(i) = byteAt(from + i)
      i += 1
    bytes

private[kyopdf] object PdfBytes:
  def borrow(bytes: Array[Byte]): PdfBytes = new PdfBytes(bytes.length, bytes, 0, null)
  def indexed(length: Int)(read: ByteReader): PdfBytes = new PdfBytes(length, null, 0, read)
