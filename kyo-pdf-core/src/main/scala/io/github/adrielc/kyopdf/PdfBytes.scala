package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1

/** Shared random-access PDF input. Only tokens and retained output payloads are
  * copied. A view never converts the complete document to characters.
  * Inputs are admitted through byte and metadata limits before parsing.
  */
private[kyopdf] final class PdfBytes private (
    val length: Long, array: Array[Byte], offset: Int, read: Long => Byte, maxTokenBytes: Int):
  private def size(from: Long, until: Long): Int =
    if from < 0 || until < from || until > length || until - from > Int.MaxValue then
      throw PdfBytes.BoundsFault("PDF token or retained payload exceeds addressable memory")
    (until - from).toInt
  private def byteAt(index: Long): Byte = if array != null then array(offset + index.toInt) else read(index)
  def charAt(index: Long): Char = (byteAt(index) & 255).toChar
  def startsWith(value: String, at: Long = 0): Boolean =
    if at < 0 || value.length > length - at then false
    else
      var i = 0
      while i < value.length && charAt(at + i) == value.charAt(i) do i += 1
      i == value.length
  def indexOf(value: String, from: Long = 0): Long =
    var at = math.max(0L, from)
    while at <= length - value.length do
      if startsWith(value, at) then return at
      at += 1
    -1
  def indexWhere(predicate: Char => Boolean, from: Long): Long =
    var at = math.max(0L, from)
    while at < length do
      if predicate(charAt(at)) then return at
      at += 1
    -1
  def lastIndexOf(value: String): Long =
    var at = length - value.length
    while at >= 0 do
      if startsWith(value, at) then return at
      at -= 1
    -1
  def substring(from: Long, until: Long): String =
    if until - from > maxTokenBytes then throw PdfBytes.BoundsFault("PDF token exceeds metadata budget")
    // Tokens are owned Strings; array input uses the charset constructor's
    // direct byte path instead of allocating an intermediate character array.
    if array != null then new String(array, offset + from.toInt, size(from, until), ISO_8859_1)
    else
      val chars = new Array[Char](size(from, until))
      var i = 0
      while i < chars.length do
        chars(i) = charAt(from + i)
        i += 1
      new String(chars)
  def slice(from: Long, until: Long): PdfBytes =
    if array != null then new PdfBytes(until - from, array, offset + from.toInt, null, maxTokenBytes)
    else new PdfBytes(until - from, null, 0, index => byteAt(from + index), maxTokenBytes)
  def payload(from: Long, until: Long): Vector[Byte] = Vector.tabulate(size(from, until))(index => byteAt(from + index))
  def copyBytes(from: Long, until: Long): Array[Byte] =
    val bytes = new Array[Byte](size(from, until))
    var i = 0
    while i < bytes.length do
      bytes(i) = byteAt(from + i)
      i += 1
    bytes

private[kyopdf] object PdfBytes:
  final case class BoundsFault(detail: String) extends RuntimeException(detail)
  def borrow(bytes: Array[Byte]): PdfBytes = new PdfBytes(bytes.length, bytes, 0, null, Int.MaxValue)
  def indexed(length: Long, maxTokenBytes: Int = Int.MaxValue)(read: Long => Byte): PdfBytes = new PdfBytes(length, null, 0, read, maxTokenBytes)
