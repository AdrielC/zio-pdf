package com.tybera.kyopdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.zip.InflaterInputStream
import kyo.*
import scala.util.Try

/** PDF lexical boundary implemented on kyo-parse.
  *
  * Streams are skipped using a direct `/Length` or a classic-xref-resolved
  * indirect `/Length`; their payload is never searched for object markers.
  * The scanner deliberately rejects ambiguous stream boundaries instead of
  * guessing from `endstream` bytes.
  */
object PdfParser:
  private final case class Ref(number: Long, generation: Int)
  private final case class ObjectHeader(ref: Ref, bodyStart: Int)
  private final case class Embedded(objects: Long, pages: Long, nodes: Long, depth: Int, payload: Long)
  private final case class ObjectScan(
      end: Int,
      objects: Long,
      streams: Long,
      pages: Long,
      nodes: Long,
      depth: Int,
      payload: Long,
      encrypted: Boolean
  )

  def scan(bytes: Array[Byte], limit: ByteLimit): ScanReport < Abort[PdfError] =
    if bytes.length.toLong > limit.toLong then Abort.fail(PdfError.TooLarge(limit.toLong, bytes.length.toLong))
    else
      val input = new String(bytes, ISO_8859_1)
      Parse.runResult(input)(document).eval match
        case result if result.errors.nonEmpty =>
          val message = result.errors.map(failure => s"${failure.position}: ${failure.message}").mkString("; ")
          Abort.fail(PdfError.InvalidPdf(message))
        case result => result.out.fold(Abort.fail(PdfError.InvalidPdf("PDF parser produced no document")))(identity)

  def validate(report: ScanReport, limits: RetentionLimits): Unit < Abort[PdfError] =
    if limits.maxObjects <= 0 || limits.maxNodes <= 0 || limits.maxDepth < 0 || limits.maxPayloadBytes <= 0 then
      Abort.fail(PdfError.RetentionExceeded("Invalid PDF retention limits"))
    else if report.facts.objects > limits.maxObjects then
      Abort.fail(PdfError.RetentionExceeded(s"PDF retains ${report.facts.objects} objects"))
    else if report.retention.nodes > limits.maxNodes then
      Abort.fail(PdfError.RetentionExceeded(s"PDF retains ${report.retention.nodes} syntax nodes"))
    else if report.retention.depth > limits.maxDepth then
      Abort.fail(PdfError.RetentionExceeded(s"PDF syntax depth is ${report.retention.depth}"))
    else if report.retention.payloadBytes > limits.maxPayloadBytes then
      Abort.fail(PdfError.RetentionExceeded(s"PDF retains ${report.retention.payloadBytes} payload bytes"))
    else if report.encrypted then Abort.fail(PdfError.InvalidPdf("Encrypted PDFs are not supported"))
    else ()

  /** Reads `/E` from the first linearization dictionary. */
  def firstPageByteLength(bytes: Array[Byte]): Long < Abort[PdfError] =
    val sample = new String(bytes.take(math.min(bytes.length, 8192)), ISO_8859_1)
    val parser = for
      _ <- Parse.skipUntil(Parse.literal("/Linearized"))
      _ <- Parse.skipUntil(Parse.literal("/E"))
      _ <- Parse.whitespaces
      end <- Parse.regex("[0-9]+")
    yield end
    val result = Parse.runResult(sample)(parser).eval
    val parsed = result.out.toOption.flatMap(_.toLongOption)
    parsed match
      case Some(end) if end > 0 && end < bytes.length => end
      case _ =>
        val detail = result.errors.map(_.message).mkString("; ")
        Abort.fail(PdfError.InvalidPdf(if detail.nonEmpty then detail else "Invalid linearized first-page byte boundary"))

  private val document: ScanReport < Parse[Char] =
    Parse.read { input =>
      val text = input.remaining.mkString
      scanDocument(text) match
        case Left((at, message)) => Result.fail(Chunk(ParseFailure(message, input.position + at)))
        case Right(report) => Result.succeed((input.advance(input.remaining.length), report))
    }

  private def scanDocument(text: String): Either[(Int, String), ScanReport] =
    val header = text.indexOf("%PDF-")
    if header < 0 || header > 1024 then Left((0, "Missing PDF header"))
    else
      val versionStart = header + 5
      val versionEnd = text.indexWhere(ch => ch == '\r' || ch == '\n' || ch.isWhitespace, versionStart) match
        case -1 => text.length
        case at => at
      val version = text.substring(versionStart, versionEnd)
      if !version.matches("[0-9]+\\.[0-9]+") then Left((versionStart, "Invalid PDF version"))
      else
        val indirectLengths = classicNumericObjects(text)
        var cursor = versionEnd
        var objects = 0L
        var streams = 0L
        var pages = 0L
        var nodes = 0L
        var maxDepth = 0
        var payload = 0L
        var encrypted = false
        var failure: Option[(Int, String)] = None
        while cursor < text.length && failure.isEmpty do
          skipTrivia(text, cursor) match
            case Left(error) => failure = Some(error)
            case Right(next) =>
              cursor = next
              if cursor < text.length then
                indirectHeader(text, cursor) match
                  case Some(header) =>
                    scanObject(text, header.bodyStart, indirectLengths) match
                      case Left(error) => failure = Some(error)
                      case Right(found) =>
                        objects += found.objects
                        streams += found.streams
                        pages += found.pages
                        nodes += found.nodes
                        maxDepth = math.max(maxDepth, found.depth)
                        payload += found.payload
                        encrypted ||= found.encrypted
                        cursor = found.end
                  case None =>
                    if keywordAt(text, cursor, "trailer") then
                      val trailerEnd = findKeyword(text, cursor + 7, "startxref").getOrElse(text.length)
                      encrypted ||= containsName(text, cursor, trailerEnd, "Encrypt")
                      cursor = trailerEnd
                    else cursor += 1
        failure.toLeft {
          val facts = Facts(objects, streams, pages)
          ScanReport(version, facts, Retention(objects.toInt, nodes, maxDepth, payload), encrypted)
        }.flatMap { report =>
          if report.facts.objects == 0 then Left((header, "PDF contains no indirect objects"))
          else if report.facts.pages == 0 then Left((header, "PDF contains no page objects"))
          else Right(report)
        }

  private def scanObject(text: String, start: Int, indirectLengths: Map[Ref, Long]): Either[(Int, String), ObjectScan] =
    var cursor = start
    var streams = 0
    var nodes = 0L
    var depth = 0
    var maxDepth = 0
    var payload = 0L
    var page = false
    var objectStream = false
    var xrefStream = false
    var encrypted = false
    var embedded = Embedded(0, 0, 0, 0, 0)
    var literalDepth = 0
    var escaped = false
    var hex = false
    var comment = false
    while cursor < text.length do
      val ch = text.charAt(cursor)
      if comment then
        comment = ch != '\n' && ch != '\r'
        cursor += 1
      else if literalDepth > 0 then
        payload += 1
        if escaped then escaped = false
        else if ch == '\\' then escaped = true
        else if ch == '(' then literalDepth += 1
        else if ch == ')' then literalDepth -= 1
        maxDepth = math.max(maxDepth, depth + literalDepth)
        cursor += 1
      else if hex then
        if ch == '>' then hex = false else if !ch.isWhitespace then payload += 1
        cursor += 1
      else if ch == '%' then
        comment = true
        cursor += 1
      else if ch == '(' then
        literalDepth = 1
        nodes += 1
        maxDepth = math.max(maxDepth, depth + 1)
        cursor += 1
      else if cursor + 1 < text.length && text.startsWith("<<", cursor) then
        depth += 1
        nodes += 1
        maxDepth = math.max(maxDepth, depth)
        cursor += 2
      else if cursor + 1 < text.length && text.startsWith(">>", cursor) then
        depth = math.max(0, depth - 1)
        cursor += 2
      else if ch == '<' then
        hex = true
        nodes += 1
        cursor += 1
      else if ch == '[' then
        depth += 1
        nodes += 1
        maxDepth = math.max(maxDepth, depth)
        cursor += 1
      else if ch == ']' then
        depth = math.max(0, depth - 1)
        cursor += 1
      else if ch == '/' then
        val end = tokenEnd(text, cursor + 1)
        val name = text.substring(cursor + 1, end)
        nodes += 1
        payload += name.length.toLong * 4L
        encrypted ||= name == "Encrypt"
        if name == "Type" then
          val valueStart = skipWhitespace(text, end)
          if valueStart < text.length && text.charAt(valueStart) == '/' then
            val valueEnd = tokenEnd(text, valueStart + 1)
            val kind = text.substring(valueStart + 1, valueEnd)
            page ||= kind == "Page"
            objectStream ||= kind == "ObjStm"
            xrefStream ||= kind == "XRef"
        cursor = end
      else if keywordAt(text, cursor, "stream") then
        streamLength(text, start, cursor, indirectLengths) match
          case Left(error) => return Left((cursor, error))
          case Right(length) if length < 0 || length > Int.MaxValue => return Left((cursor, "Invalid PDF stream length"))
          case Right(length) =>
            val dataStart0 = cursor + 6
            val dataStart =
              if text.startsWith("\r\n", dataStart0) then dataStart0 + 2
              else if dataStart0 < text.length && (text.charAt(dataStart0) == '\r' || text.charAt(dataStart0) == '\n') then dataStart0 + 1
              else return Left((cursor, "PDF stream keyword is not followed by a newline"))
            val dataEnd = dataStart.toLong + length
            if dataEnd > text.length then return Left((cursor, "PDF stream exceeds input"))
            val endstream = skipWhitespace(text, dataEnd.toInt)
            if !keywordAt(text, endstream, "endstream") then return Left((endstream, "PDF stream length does not land on endstream"))
            if objectStream then
              val dictionary = text.substring(start, cursor)
              val raw = text.substring(dataStart, dataEnd.toInt).getBytes(ISO_8859_1)
              decodeObjectStream(dictionary, raw, cursor) match
                case Left(error) => return Left(error)
                case Right(value) => embedded = value
            streams += 1
            payload += length
            cursor = endstream + 9
      else if keywordAt(text, cursor, "endobj") then
        val container = objectStream || xrefStream
        val semanticObjects = (if container then 0L else 1L) + embedded.objects
        val semanticStreams = (if container then 0L else streams.toLong)
        return Right(ObjectScan(cursor + 6, semanticObjects, semanticStreams, (if page then 1L else 0L) + embedded.pages, nodes + embedded.nodes,
          math.max(maxDepth, embedded.depth), payload + embedded.payload, encrypted))
      else if !ch.isWhitespace then
        nodes += 1
        cursor = tokenEnd(text, cursor)
      else cursor += 1
    Left((start, "Unterminated indirect object"))

  private def indirectHeader(text: String, at: Int): Option[ObjectHeader] =
    val firstEnd = digitsEnd(text, at)
    if firstEnd == at then None
    else
      val generationStart = skipWhitespace(text, firstEnd)
      val generationEnd = digitsEnd(text, generationStart)
      val marker = skipWhitespace(text, generationEnd)
      for
        number <- text.substring(at, firstEnd).toLongOption
        generation <- text.substring(generationStart, generationEnd).toIntOption
        if generationEnd > generationStart && keywordAt(text, marker, "obj")
      yield ObjectHeader(Ref(number, generation), marker + 3)

  private def streamLength(text: String, from: Int, until: Int, indirect: Map[Ref, Long]): Either[String, Long] =
    var at = from
    var found: Option[Either[String, Long]] = None
    while at < until && found.isEmpty do
      if text.charAt(at) == '/' && text.startsWith("Length", at + 1) && tokenBoundary(text, at + 7) then
        val numberStart = skipWhitespace(text, at + 7)
        val numberEnd = digitsEnd(text, numberStart)
        if numberEnd > numberStart then
          val after = skipWhitespace(text, numberEnd)
          val first = text.substring(numberStart, numberEnd).toLongOption
          val generationEnd = digitsEnd(text, after)
          val marker = skipWhitespace(text, generationEnd)
          if generationEnd > after && marker < until && keywordAt(text, marker, "R") then
            val ref = for
              number <- first
              generation <- text.substring(after, generationEnd).toIntOption
            yield Ref(number, generation)
            found = Some(ref.flatMap(indirect.get).toRight("PDF stream /Length reference cannot be resolved from the classic xref"))
          else found = Some(first.toRight("Invalid direct PDF stream /Length"))
      at += 1
    found.getOrElse(Left("PDF stream does not have a /Length"))

  private def classicNumericObjects(text: String): Map[Ref, Long] =
    classicXrefOffsets(text).flatMap { (ref, offset) =>
      indirectHeader(text, offset).filter(_.ref == ref).flatMap { header =>
        findKeyword(text, header.bodyStart, "endobj").flatMap { end =>
          val body = text.substring(header.bodyStart, end).trim
          body.toLongOption.map(ref -> _)
        }
      }
    }

  /** Resolve the latest classic xref table. Malformed and xref-stream inputs
    * simply yield no offsets; callers then reject unresolved indirect lengths.
    */
  private def classicXrefOffsets(text: String): Map[Ref, Int] =
    val marker = text.lastIndexOf("startxref")
    if marker < 0 then Map.empty
    else
      val offsetStart = skipWhitespace(text, marker + 9)
      val offsetEnd = digitsEnd(text, offsetStart)
      text.substring(offsetStart, offsetEnd).toIntOption match
        case None => Map.empty
        case Some(xref) if xref < 0 || xref >= text.length || !keywordAt(text, xref, "xref") => Map.empty
        case Some(xref) =>
          val output = Map.newBuilder[Ref, Int]
          var cursor = skipWhitespace(text, xref + 4)
          var done = false
          while cursor < text.length && !done do
            if keywordAt(text, cursor, "trailer") then done = true
            else
              val firstEnd = digitsEnd(text, cursor)
              val countStart = skipWhitespace(text, firstEnd)
              val countEnd = digitsEnd(text, countStart)
              val subsection = for
                first <- text.substring(cursor, firstEnd).toLongOption
                count <- text.substring(countStart, countEnd).toIntOption
                if count >= 0
              yield (first, count)
              subsection match
                case None => done = true
                case Some((first, count)) =>
                  cursor = nextLine(text, countEnd)
                  var index = 0
                  while index < count && cursor < text.length do
                    val lineEnd0 = text.indexWhere(ch => ch == '\r' || ch == '\n', cursor)
                    val lineEnd = if lineEnd0 < 0 then text.length else lineEnd0
                    val columns = text.substring(cursor, lineEnd).trim.split("\\s+")
                    if columns.length >= 3 && columns(2) == "n" then
                      for
                        offset <- columns(0).toIntOption
                        generation <- columns(1).toIntOption
                      do output += Ref(first + index, generation) -> offset
                    cursor = nextLine(text, lineEnd)
                    index += 1
                  cursor = skipWhitespace(text, cursor)
          output.result()

  private def decodeObjectStream(dictionary: String, raw: Array[Byte], at: Int): Either[(Int, String), Embedded] =
    val parameters = for
      count <- integerEntry(dictionary, "N").toRight((at, "PDF object stream is missing /N"))
      first <- integerEntry(dictionary, "First").toRight((at, "PDF object stream is missing /First"))
    yield (count, first)
    parameters.flatMap { case (countValue, firstValue) =>
      if countValue > 100000L || firstValue > Int.MaxValue.toLong then Left((at, "PDF object-stream /N or /First is invalid"))
      else
        val count = countValue.toInt
        val first = firstValue.toInt
        val filters = nameEntries(dictionary, "Filter")
        val expanded =
          if filters.isEmpty then Right(raw)
          else if filters == List("FlateDecode") || filters == List("Fl") then inflateBounded(raw, 8 * 1024 * 1024)
          else Left(s"Unsupported PDF object-stream filter chain: ${filters.mkString(",")}")
        expanded.left.map(message => (at, message)).flatMap { bytes =>
          if first > bytes.length then Left((at, "PDF object-stream /First exceeds expanded bytes"))
          else
            val text = new String(bytes, ISO_8859_1)
            val header = text.substring(0, first).trim.split("\\s+").toVector.filter(_.nonEmpty)
            if header.length < count * 2 then Left((at, "PDF object-stream header has fewer than /N entries"))
            else
              val offsets = (0 until count).map(index => header(index * 2 + 1).toIntOption).toVector
              if offsets.exists(_.isEmpty) then Left((at, "PDF object-stream header contains an invalid offset"))
              else
                val values = offsets.flatten
                if values != values.sorted || values.exists(value => value < 0 || first.toLong + value > bytes.length) then
                  Left((at, "PDF object-stream offsets are invalid"))
                else
                  var pages = 0L
                  var nodes = 0L
                  var depth = 0
                  var payload = bytes.length.toLong
                  values.zipWithIndex.foreach { (offset, index) =>
                    val start = first + offset
                    val end = if index + 1 < values.length then first + values(index + 1) else bytes.length
                    val body = text.substring(start, end)
                    if typeName(body).contains("Page") then pages += 1
                    val measured = measureValue(body)
                    nodes += measured._1
                    depth = math.max(depth, measured._2)
                    payload += measured._3
                  }
                  Right(Embedded(count.toLong, pages, nodes, depth, payload))
        }
    }

  private def integerEntry(dictionary: String, name: String): Option[Long] =
    val marker = dictionary.indexOf("/" + name)
    if marker < 0 || !tokenBoundary(dictionary, marker + name.length + 1) then None
    else
      val start = skipWhitespace(dictionary, marker + name.length + 1)
      val end = digitsEnd(dictionary, start)
      dictionary.substring(start, end).toLongOption

  private def nameEntries(dictionary: String, name: String): List[String] =
    val marker = dictionary.indexOf("/" + name)
    if marker < 0 || !tokenBoundary(dictionary, marker + name.length + 1) then Nil
    else
      var cursor = skipWhitespace(dictionary, marker + name.length + 1)
      val array = cursor < dictionary.length && dictionary.charAt(cursor) == '['
      if array then cursor = skipWhitespace(dictionary, cursor + 1)
      val output = List.newBuilder[String]
      var done = false
      while cursor < dictionary.length && !done do
        if dictionary.charAt(cursor) == '/' then
          val end = tokenEnd(dictionary, cursor + 1)
          output += dictionary.substring(cursor + 1, end)
          cursor = skipWhitespace(dictionary, end)
          if !array then done = true
        else if array && dictionary.charAt(cursor) == ']' then done = true
        else done = true
      output.result()

  private def inflateBounded(bytes: Array[Byte], maxBytes: Int): Either[String, Array[Byte]] =
    Try {
      val input = new InflaterInputStream(new ByteArrayInputStream(bytes))
      val initialCapacity = math.min(bytes.length.toLong * 2L, maxBytes.toLong).toInt
      val output = new ByteArrayOutputStream(initialCapacity)
      val buffer = new Array[Byte](8192)
      try
        var total = 0
        var exceeded = false
        var read = input.read(buffer)
        while read >= 0 && !exceeded do
          total += read
          if total > maxBytes then exceeded = true
          else
            if read > 0 then output.write(buffer, 0, read)
            read = input.read(buffer)
        if exceeded then Left(s"Expanded PDF object stream exceeds $maxBytes bytes")
        else Right(output.toByteArray)
      finally input.close()
    }.toEither.left.map(error => Option(error.getMessage).getOrElse(error.getClass.getSimpleName)).flatten

  private def typeName(body: String): Option[String] =
    val marker = body.indexOf("/Type")
    if marker < 0 || !tokenBoundary(body, marker + 5) then None
    else
      val start = skipWhitespace(body, marker + 5)
      Option.when(start < body.length && body.charAt(start) == '/') {
        val end = tokenEnd(body, start + 1)
        body.substring(start + 1, end)
      }

  private def measureValue(body: String): (Long, Int, Long) =
    var cursor = 0
    var nodes = 0L
    var depth = 0
    var maxDepth = 0
    var payload = 0L
    while cursor < body.length do
      body.charAt(cursor) match
        case '<' if cursor + 1 < body.length && body.charAt(cursor + 1) == '<' => depth += 1; nodes += 1; maxDepth = math.max(maxDepth, depth); cursor += 2
        case '>' if cursor + 1 < body.length && body.charAt(cursor + 1) == '>' => depth = math.max(0, depth - 1); cursor += 2
        case '[' => depth += 1; nodes += 1; maxDepth = math.max(maxDepth, depth); cursor += 1
        case ']' => depth = math.max(0, depth - 1); cursor += 1
        case '/' =>
          val end = tokenEnd(body, cursor + 1)
          nodes += 1
          payload += (end - cursor - 1).toLong * 4L
          cursor = end
        case char if char.isWhitespace => cursor += 1
        case _ => nodes += 1; cursor = tokenEnd(body, cursor)
    (nodes, maxDepth, payload)

  private def nextLine(text: String, from: Int): Int =
    var at = from
    while at < text.length && text.charAt(at) != '\r' && text.charAt(at) != '\n' do at += 1
    if at < text.length && text.charAt(at) == '\r' then at += 1
    if at < text.length && text.charAt(at) == '\n' then at += 1
    at

  private def skipTrivia(text: String, start: Int): Either[(Int, String), Int] =
    var at = start
    var continue = true
    while at < text.length && continue do
      if text.charAt(at).isWhitespace || text.charAt(at) == 0 then at += 1
      else if text.charAt(at) == '%' && !text.startsWith("%%EOF", at) then
        val end = text.indexWhere(ch => ch == '\r' || ch == '\n', at)
        at = if end < 0 then text.length else end + 1
      else continue = false
    Right(at)

  private def containsName(text: String, from: Int, until: Int, name: String): Boolean =
    val needle = "/" + name
    var at = text.indexOf(needle, from)
    while at >= 0 && at < until do
      if tokenBoundary(text, at + needle.length) then return true
      at = text.indexOf(needle, at + 1)
    false

  private def findKeyword(text: String, from: Int, keyword: String): Option[Int] =
    var at = text.indexOf(keyword, from)
    while at >= 0 do
      if keywordAt(text, at, keyword) then return Some(at)
      at = text.indexOf(keyword, at + 1)
    None

  private def keywordAt(text: String, at: Int, keyword: String): Boolean =
    at >= 0 && text.startsWith(keyword, at) &&
      (at == 0 || delimiter(text.charAt(at - 1))) && tokenBoundary(text, at + keyword.length)

  private def tokenBoundary(text: String, at: Int): Boolean = at >= text.length || delimiter(text.charAt(at))
  private def delimiter(ch: Char): Boolean = ch.isWhitespace || "()<>[]{}/%".contains(ch) || ch == 0
  private def skipWhitespace(text: String, start: Int): Int =
    var at = start
    while at < text.length && (text.charAt(at).isWhitespace || text.charAt(at) == 0) do at += 1
    at
  private def digitsEnd(text: String, start: Int): Int =
    var at = start
    while at < text.length && text.charAt(at).isDigit do at += 1
    at
  private def tokenEnd(text: String, start: Int): Int =
    var at = start
    while at < text.length && !delimiter(text.charAt(at)) do at += 1
    if at == start then start + 1 else at
