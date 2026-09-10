package com.tybera.kyopdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.zip.InflaterInputStream
import kyo.*
import scala.collection.mutable
import scala.util.Try

/** Full primitive/object decoder behind the Kyo document algebra.
  *
  * The structural scanner first establishes bounded, unambiguous stream
  * boundaries. This decoder then retains typed primitive values and verbatim
  * stream bytes for layout and rewrite operations.
  */
object PdfDocumentParser:
  import PdfValue.*

  private final case class Header(index: ObjectRef, bodyStart: Int)
  private final case class Parsed(objects: Vector[PdfObject], trailer: Option[Dict], end: Int)

  def decode(bytes: scala.Array[Byte], limit: ByteLimit, limits: RetentionLimits = RetentionLimits()): PdfDocument < Abort[PdfError] =
    PdfParser.scan(bytes, limit).map { report =>
      PdfParser.validate(report, limits).map { _ =>
        decodeChecked(bytes, report.version) match
          case Left((at, message)) => Abort.fail(PdfError.InvalidPdf(s"$at: $message"))
          case Right(document) if document.objects.length.toLong != report.facts.objects =>
            Abort.fail(PdfError.InvalidPdf(s"Decoded ${document.objects.length} objects but structural scan admitted ${report.facts.objects}"))
          case Right(document) => document
      }
    }

  private def decodeChecked(bytes: scala.Array[Byte], version: String): Either[(Int, String), PdfDocument] =
    val text = new String(bytes, ISO_8859_1)
    val indirect = classicNumericObjects(text)
    val objects = Vector.newBuilder[PdfObject]
    var trailer: Option[Dict] = None
    var cursor = 0
    while cursor < text.length do
      cursor = skip(text, cursor)
      if cursor < text.length then
        header(text, cursor) match
          case Some(found) =>
            parseObject(text, found, indirect) match
              case Left(error) => return Left(error)
              case Right(parsed) =>
                objects ++= parsed.objects
                trailer = parsed.trailer.orElse(trailer)
                cursor = parsed.end
          case None if keywordAt(text, cursor, "trailer") =>
            ValueReader(text, cursor + 7, text.length).read() match
              case Right((value: Dict, next)) => trailer = Some(value); cursor = next
              case Right((_, _)) => return Left((cursor, "PDF trailer is not a dictionary"))
              case Left(error) => return Left(error)
          case None => cursor += 1
    val retained = objects.result()
    trailer match
      case Some(value) => Right(PdfDocument(version, retained, value))
      case None => Left((0, "PDF has no trailer or xref-stream dictionary"))

  private def parseObject(text: String, header: Header, indirect: Map[ObjectRef, Long]): Either[(Int, String), Parsed] =
    ValueReader(text, header.bodyStart, text.length).read().flatMap { (value, afterValue) =>
      readStream(text, value, skip(text, afterValue), indirect).flatMap { (raw, afterStream) =>
        var cursor = skip(text, afterStream)
        cursor = skip(text, cursor)
        if !keywordAt(text, cursor, "endobj") then Left((cursor, "PDF indirect object has no endobj marker"))
        else
          val end = cursor + 6
          value match
            case dict: Dict if typeName(dict).contains("ObjStm") =>
              raw.toRight((cursor, "PDF object stream has no bytes")).flatMap(decodeObjectStream(dict, _, cursor)).map(values => Parsed(values, None, end))
            case dict: Dict if typeName(dict).contains("XRef") => Right(Parsed(Vector.empty, Some(dict), end))
            case _ => Right(Parsed(Vector(PdfObject(header.index, value, raw)), None, end))
      }
    }

  private def readStream(
    text: String,
    value: PdfValue,
    cursor: Int,
    indirect: Map[ObjectRef, Long]
  ): Either[(Int, String), (Option[Vector[Byte]], Int)] =
    if !keywordAt(text, cursor, "stream") then Right((None, cursor))
    else value match
      case dict: Dict =>
        val length = dict.get("Length") match
          case Some(Number(number)) if number.isValidLong => Right(number.toLong)
          case Some(Ref(reference)) => indirect.get(reference).toRight((cursor, "PDF stream /Length reference is unresolved"))
          case _ => Left((cursor, "PDF stream has no valid /Length"))
        length.flatMap { count =>
          if count < 0 || count > Int.MaxValue then Left((cursor, "PDF stream length is outside decoder bounds"))
          else
            val start0 = cursor + 6
            val start =
              if text.startsWith("\r\n", start0) then Some(start0 + 2)
              else if start0 < text.length && (text.charAt(start0) == '\r' || text.charAt(start0) == '\n') then Some(start0 + 1)
              else None
            start.toRight((cursor, "PDF stream keyword is not followed by a newline")).flatMap { valueStart =>
              val end = valueStart.toLong + count
              if end > text.length then Left((cursor, "PDF stream exceeds input"))
              else
                val marker = skip(text, end.toInt)
                if !keywordAt(text, marker, "endstream") then Left((marker, "PDF stream length does not land on endstream"))
                else Right((Some(text.substring(valueStart, end.toInt).getBytes(ISO_8859_1).toVector), marker + 9))
            }
        }
      case _ => Left((cursor, "PDF stream object does not contain a dictionary"))

  private def decodeObjectStream(dict: Dict, raw: Vector[Byte], at: Int): Either[(Int, String), Vector[PdfObject]] =
    val parameters = for
      count <- integer(dict.get("N")).toRight((at, "PDF object stream is missing /N"))
      first <- integer(dict.get("First")).toRight((at, "PDF object stream is missing /First"))
    yield (count, first)
    parameters.flatMap { (countLong, firstLong) =>
      if countLong < 0 || countLong > 100000 || firstLong < 0 || firstLong > Int.MaxValue then
        Left((at, "PDF object-stream /N or /First is invalid"))
      else decodeFilters(dict, raw.toArray, 8 * 1024 * 1024).left.map(message => (at, message)).flatMap { bytes =>
        val count = countLong.toInt
        val first = firstLong.toInt
        if first > bytes.length then Left((at, "PDF object-stream /First exceeds expanded bytes"))
        else
          val input = new String(bytes, ISO_8859_1)
          val headerTokens = input.substring(0, first).trim.split("\\s+").toVector.filter(_.nonEmpty)
          if headerTokens.length < count * 2 then Left((at, "PDF object-stream header has fewer than /N entries"))
          else
            val entries = (0 until count).map { index =>
              for
                number <- headerTokens(index * 2).toLongOption
                offset <- headerTokens(index * 2 + 1).toIntOption
              yield (number, offset)
            }.toVector
            if entries.exists(_.isEmpty) then Left((at, "PDF object-stream header contains an invalid object number or offset"))
            else
              val values = entries.flatten
              if values.map(_._2) != values.map(_._2).sorted || values.exists((_, offset) => offset < 0 || first.toLong + offset > bytes.length) then
                Left((at, "PDF object-stream offsets are invalid"))
              else
                values.zipWithIndex.foldLeft[Either[(Int, String), Vector[PdfObject]]](Right(Vector.empty)) {
                  case (result, ((number, offset), index)) => result.flatMap { decoded =>
                    val start = first + offset
                    val end = if index + 1 < values.length then first + values(index + 1)._2 else bytes.length
                    ValueReader(input, start, end).read().flatMap { (value, next) =>
                      if skip(input, next) < end then Left((next, s"Embedded PDF object $number contains trailing syntax"))
                      else Right(decoded :+ PdfObject(ObjectRef(number), value))
                    }
                  }
                }
      }
    }

  private def decodeFilters(dict: Dict, bytes: scala.Array[Byte], maxBytes: Int): Either[String, scala.Array[Byte]] =
    val filters = dict.get("Filter") match
      case None => Vector.empty
      case Some(Name(value)) => Vector(value)
      case Some(Array(values)) => values.collect { case Name(value) => value }
      case _ => Vector("unsupported")
    if filters.isEmpty then Right(bytes)
    else if filters == Vector("FlateDecode") || filters == Vector("Fl") then inflate(bytes, maxBytes)
    else Left(s"Unsupported PDF object-stream filter chain: ${filters.mkString(",")}")

  private def inflate(bytes: scala.Array[Byte], maxBytes: Int): Either[String, scala.Array[Byte]] =
    Try {
      val input = new InflaterInputStream(new ByteArrayInputStream(bytes))
      val output = new ByteArrayOutputStream(math.min(maxBytes, math.max(32, bytes.length * 2)))
      val buffer = new scala.Array[Byte](8192)
      try
        var total = 0
        var read = input.read(buffer)
        while read >= 0 do
          if read > 0 then
            total += read
            if total > maxBytes then throw IllegalArgumentException(s"Expanded PDF object stream exceeds $maxBytes bytes")
            output.write(buffer, 0, read)
          read = input.read(buffer)
        output.toByteArray
      finally input.close()
    }.toEither.left.map(error => Option(error.getMessage).getOrElse(error.getClass.getSimpleName))

  private def typeName(dict: Dict): Option[String] = dict.get("Type").collect { case Name(value) => value }
  private def integer(value: Option[PdfValue]): Option[Long] = value.collect { case Number(number) if number.isValidLong => number.toLong }

  private final class ValueReader(input: String, initial: Int, end: Int):
    def read(): Either[(Int, String), (PdfValue, Int)] = value(skip(input, initial), 0)

    private def value(at: Int, depth: Int): Either[(Int, String), (PdfValue, Int)] =
      if depth > 128 then Left((at, "PDF primitive nesting exceeds 128 levels"))
      else if at >= end then Left((at, "Unexpected end of PDF primitive"))
      else input.charAt(at) match
        case '<' if at + 1 < end && input.charAt(at + 1) == '<' => dictionary(at, depth + 1)
        case '<' => hexadecimal(at)
        case '(' => literal(at, depth + 1)
        case '[' => array(at, depth + 1)
        case '/' => name(at).map((name, next) => (Name(name), next))
        case ']' | ')' | '>' | '{' | '}' => Left((at, "Unexpected PDF primitive delimiter"))
        case _ => atom(at)

    private def dictionary(at: Int, depth: Int): Either[(Int, String), (PdfValue, Int)] =
      val fields = Vector.newBuilder[PdfField]
      var cursor = skip(input, at + 2)
      while cursor < end && !input.startsWith(">>", cursor) do
        if input.charAt(cursor) != '/' then return Left((cursor, "PDF dictionary key is not a name"))
        name(cursor) match
          case Left(error) => return Left(error)
          case Right((key, afterKey)) =>
            value(skip(input, afterKey), depth) match
              case Left((position, message)) => return Left((position, s"$message while reading /$key"))
              case Right((entry, next)) => fields += PdfField(key, entry); cursor = skip(input, next)
      if cursor >= end then Left((at, "Unterminated PDF dictionary"))
      else Right((Dict(fields.result()), cursor + 2))

    private def array(at: Int, depth: Int): Either[(Int, String), (PdfValue, Int)] =
      val values = Vector.newBuilder[PdfValue]
      var cursor = skip(input, at + 1)
      while cursor < end && input.charAt(cursor) != ']' do
        value(cursor, depth) match
          case Left(error) => return Left(error)
          case Right((entry, next)) => values += entry; cursor = skip(input, next)
      if cursor >= end then Left((at, "Unterminated PDF array"))
      else Right((Array(values.result()), cursor + 1))

    private def name(at: Int): Either[(Int, String), (String, Int)] =
      var cursor = at + 1
      val result = new StringBuilder
      while cursor < end && !delimiter(input.charAt(cursor)) do
        if input.charAt(cursor) == '#' then
          if cursor + 2 >= end then return Left((cursor, "Incomplete hexadecimal PDF name escape"))
          val high = nibble(input.charAt(cursor + 1)); val low = nibble(input.charAt(cursor + 2))
          if high < 0 || low < 0 then return Left((cursor, "Invalid hexadecimal PDF name escape"))
          result.append(((high << 4) | low).toChar); cursor += 3
        else
          result.append(input.charAt(cursor))
          cursor += 1
      if cursor == at + 1 then Left((at, "Empty PDF name")) else Right((result.result(), cursor))

    private def hexadecimal(at: Int): Either[(Int, String), (PdfValue, Int)] =
      val digits = new StringBuilder
      var cursor = at + 1
      while cursor < end && input.charAt(cursor) != '>' do
        val char = input.charAt(cursor)
        if !white(char) then
          if nibble(char) < 0 then return Left((cursor, "Invalid PDF hexadecimal string"))
          digits.append(char)
        cursor += 1
      if cursor >= end then Left((at, "Unterminated PDF hexadecimal string"))
      else
        if digits.length % 2 == 1 then digits.append('0')
        val bytes = Vector.tabulate(digits.length / 2)(index => ((nibble(digits(index * 2)) << 4) | nibble(digits(index * 2 + 1))).toByte)
        Right((Hex(bytes), cursor + 1))

    private def literal(at: Int, depth: Int): Either[(Int, String), (PdfValue, Int)] =
      val bytes = mutable.ArrayBuffer.empty[Byte]
      var cursor = at + 1
      var nesting = 1
      while cursor < end && nesting > 0 do
        val char = input.charAt(cursor)
        if char == '\\' then
          cursor += 1
          if cursor < end then
            input.charAt(cursor) match
              case '\n' => ()
              case '\r' => if cursor + 1 < end && input.charAt(cursor + 1) == '\n' then cursor += 1
              case 'n' => bytes += '\n'.toByte
              case 'r' => bytes += '\r'.toByte
              case 't' => bytes += '\t'.toByte
              case 'b' => bytes += '\b'.toByte
              case 'f' => bytes += '\f'.toByte
              case escaped @ ('(' | ')' | '\\') => bytes += escaped.toByte
              case digit if digit >= '0' && digit <= '7' =>
                var octal = digit - '0'; var taken = 1
                while taken < 3 && cursor + 1 < end && input.charAt(cursor + 1) >= '0' && input.charAt(cursor + 1) <= '7' do
                  cursor += 1; taken += 1; octal = octal * 8 + input.charAt(cursor) - '0'
                bytes += octal.toByte
              case other => bytes += other.toByte
        else if char == '(' then
          nesting += 1
          if nesting + depth > 128 then return Left((cursor, "PDF string nesting exceeds 128 levels"))
          bytes += char.toByte
        else if char == ')' then
          nesting -= 1
          if nesting > 0 then bytes += char.toByte
        else bytes += char.toByte
        cursor += 1
      if nesting != 0 then Left((at, "Unterminated PDF literal string"))
      else Right((Literal(bytes.toVector), cursor))

    private def atom(at: Int): Either[(Int, String), (PdfValue, Int)] =
      val firstEnd = tokenEnd(input, at, end)
      val token = input.substring(at, firstEnd)
      token match
        case "null" => Right((Null, firstEnd))
        case "true" => Right((Bool(true), firstEnd))
        case "false" => Right((Bool(false), firstEnd))
        case _ =>
          Try(BigDecimal(token)).toOption match
            case None => Left((at, s"Unsupported PDF primitive token: $token"))
            case Some(number) =>
              val secondStart = skip(input, firstEnd)
              val secondEnd = tokenEnd(input, secondStart, end)
              val marker = skip(input, secondEnd)
              if number.isWhole && number.isValidLong && secondEnd > secondStart &&
                  input.substring(secondStart, secondEnd).toIntOption.nonEmpty && keywordAt(input, marker, "R") then
                Right((Ref(ObjectRef(number.toLong, input.substring(secondStart, secondEnd).toInt)), marker + 1))
              else Right((Number(number), firstEnd))

  private def classicNumericObjects(text: String): Map[ObjectRef, Long] =
    classicXrefOffsets(text).flatMap { (ref, offset) =>
      header(text, offset).filter(_.index == ref).flatMap { found =>
        val end = findKeyword(text, found.bodyStart, "endobj").getOrElse(found.bodyStart)
        text.substring(found.bodyStart, end).trim.toLongOption.map(ref -> _)
      }
    }

  private def classicXrefOffsets(text: String): Map[ObjectRef, Int] =
    val marker = text.lastIndexOf("startxref")
    if marker < 0 then Map.empty
    else
      val offsetStart = skip(text, marker + 9)
      val offsetEnd = digitsEnd(text, offsetStart)
      text.substring(offsetStart, offsetEnd).toIntOption match
        case Some(xref) if xref >= 0 && xref < text.length && keywordAt(text, xref, "xref") =>
          val output = Map.newBuilder[ObjectRef, Int]
          var cursor = skip(text, xref + 4)
          var done = false
          while cursor < text.length && !done do
            if keywordAt(text, cursor, "trailer") then done = true
            else
              val firstEnd = digitsEnd(text, cursor)
              val countStart = skip(text, firstEnd)
              val countEnd = digitsEnd(text, countStart)
              (text.substring(cursor, firstEnd).toLongOption, text.substring(countStart, countEnd).toIntOption) match
                case (Some(first), Some(count)) if count >= 0 =>
                  cursor = nextLine(text, countEnd)
                  var index = 0
                  while index < count && cursor < text.length do
                    val lineEnd0 = text.indexWhere(char => char == '\r' || char == '\n', cursor)
                    val lineEnd = if lineEnd0 < 0 then text.length else lineEnd0
                    val columns = text.substring(cursor, lineEnd).trim.split("\\s+")
                    if columns.length >= 3 && columns(2) == "n" then
                      for offset <- columns(0).toIntOption; generation <- columns(1).toIntOption
                      do output += ObjectRef(first + index, generation) -> offset
                    cursor = nextLine(text, lineEnd); index += 1
                  cursor = skip(text, cursor)
                case _ => done = true
          output.result()
        case _ => Map.empty

  private def header(text: String, at: Int): Option[Header] =
    val firstEnd = digitsEnd(text, at)
    val generationStart = skip(text, firstEnd)
    val generationEnd = digitsEnd(text, generationStart)
    val marker = skip(text, generationEnd)
    for
      number <- text.substring(at, firstEnd).toLongOption
      generation <- text.substring(generationStart, generationEnd).toIntOption
      if firstEnd > at && generationEnd > generationStart && keywordAt(text, marker, "obj")
    yield Header(ObjectRef(number, generation), marker + 3)

  private def skip(text: String, start: Int): Int =
    var at = start
    var again = true
    while at < text.length && again do
      if white(text.charAt(at)) then at += 1
      else if text.charAt(at) == '%' && !text.startsWith("%%EOF", at) then
        while at < text.length && text.charAt(at) != '\r' && text.charAt(at) != '\n' do at += 1
      else again = false
    at

  private def nextLine(text: String, from: Int): Int =
    var at = from
    while at < text.length && text.charAt(at) != '\r' && text.charAt(at) != '\n' do at += 1
    if at < text.length && text.charAt(at) == '\r' then at += 1
    if at < text.length && text.charAt(at) == '\n' then at += 1
    at

  private def findKeyword(text: String, from: Int, keyword: String): Option[Int] =
    var at = text.indexOf(keyword, from)
    while at >= 0 do
      if keywordAt(text, at, keyword) then return Some(at)
      at = text.indexOf(keyword, at + 1)
    None

  private def keywordAt(text: String, at: Int, keyword: String): Boolean =
    at >= 0 && text.startsWith(keyword, at) && (at == 0 || delimiter(text.charAt(at - 1))) &&
      (at + keyword.length >= text.length || delimiter(text.charAt(at + keyword.length)))

  private def white(char: Char): Boolean = char.isWhitespace || char == 0
  private def delimiter(char: Char): Boolean = white(char) || "()<>[]{}/%".contains(char)
  private def digitsEnd(text: String, start: Int): Int =
    var at = start
    while at < text.length && text.charAt(at).isDigit do at += 1
    at
  private def tokenEnd(text: String, start: Int, end: Int): Int =
    var at = start
    while at < end && !delimiter(text.charAt(at)) do at += 1
    at
  private def nibble(char: Char): Int =
    if char >= '0' && char <= '9' then char - '0'
    else if char >= 'a' && char <= 'f' then char - 'a' + 10
    else if char >= 'A' && char <= 'F' then char - 'A' + 10
    else -1
