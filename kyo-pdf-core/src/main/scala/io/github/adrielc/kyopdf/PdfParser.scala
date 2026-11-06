package io.github.adrielc.kyopdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.zip.InflaterInputStream
import kyo.*
import scala.util.Try

/** PDF lexical boundaries over shared byte input.
  *
  * Streams are skipped using a direct `/Length` or a classic-xref-resolved
  * indirect `/Length`; their payload is never searched for object markers.
  * The scanner deliberately rejects ambiguous stream boundaries instead of
  * guessing from `endstream` bytes.
  */
object PdfParser:
  private final case class ObjectHeader(ref: ObjectRef, bodyStart: Long)
  private final case class Embedded(objects: Long, pages: Long, nodes: Long, depth: Int, payload: Long)
  private final case class ObjectScan(
      end: Long,
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
    else Abort.get(scanBytes(PdfBytes.borrow(bytes)))

  /** Partial structural observation. A successful early result does not certify
    * the unread suffix; use scan/decode for whole-document validation.
    */
  def scanUntil(bytes: Array[Byte], limit: ByteLimit)(stop: ScanReport => Boolean): ScanReport < Abort[PdfError] =
    if bytes.length.toLong > limit.toLong then Abort.fail(PdfError.TooLarge(limit.toLong, bytes.length.toLong))
    else Abort.get(scanBytes(PdfBytes.borrow(bytes), stop = stop))

  private[kyopdf] enum Event:
    case Object(number: Long, generation: Int, bodyStart: Long, end: Long, lengths: () => Map[ObjectRef, Long])
    case Trailer(start: Long)

  private[kyopdf] def scanBytes(text: PdfBytes,
      visit: Event => Either[PdfError, Unit] = _ => Right(()),
      limits: Option[RetentionLimits] = None,
      stop: ScanReport => Boolean = _ => false,
      externalStreams: Boolean = false): Either[PdfError, ScanReport] =
    Try(scanDocument(text, visit, limits, stop, externalStreams)).recover {
      case error: PdfBytes.BoundsFault => Left(PdfError.RetentionExceeded(error.detail))
    }.get

  def validate(report: ScanReport, limits: RetentionLimits): Unit < Abort[PdfError] =
    Abort.get(checkLimits(report, limits))

  private[kyopdf] def checkLimits(report: ScanReport, limits: RetentionLimits): Either[PdfError, Unit] =
    if limits.maxObjects <= 0 || limits.maxNodes <= 0 || limits.maxDepth < 0 || limits.maxPayloadBytes <= 0 then
      Left(PdfError.RetentionExceeded("Invalid PDF retention limits"))
    else if report.facts.objects > limits.maxObjects then
      Left(PdfError.RetentionExceeded(s"PDF retains ${report.facts.objects} objects"))
    else if report.retention.nodes > limits.maxNodes then
      Left(PdfError.RetentionExceeded(s"PDF retains ${report.retention.nodes} syntax nodes"))
    else if report.retention.depth > limits.maxDepth then
      Left(PdfError.RetentionExceeded(s"PDF syntax depth is ${report.retention.depth}"))
    else if report.retention.payloadBytes > limits.maxPayloadBytes then
      Left(PdfError.RetentionExceeded(s"PDF retains ${report.retention.payloadBytes} payload bytes"))
    else if report.encrypted then Left(PdfError.InvalidPdf("Encrypted PDFs are not supported"))
    else Right(())

  /** Reads `/E` from the first linearization dictionary. */
  def firstPageByteLength(bytes: Array[Byte]): Long < Abort[PdfError] =
    val sample = new String(bytes, 0, math.min(bytes.length, 8192), ISO_8859_1)
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

  private def scanDocument(text: PdfBytes, visit: Event => Either[PdfError, Unit],
      limits: Option[RetentionLimits], stop: ScanReport => Boolean, externalStreams: Boolean): Either[PdfError, ScanReport] =
    def invalid(at: Long, message: String): PdfError = PdfError.InvalidPdf(s"$at: $message")
    val header = text.slice(0, math.min(text.length, 1029)).indexOf("%PDF-")
    if header < 0 || header > 1024 then Left(invalid(0, "Missing PDF header"))
    else
      val versionStart = header + 5
      val versionWindow = text.slice(versionStart, math.min(text.length, versionStart + 65))
      val versionEnd = versionWindow.indexWhere(ch => ch == '%' || ch == '\r' || ch == '\n' || ch.isWhitespace, 0) match
        case -1 => versionStart + versionWindow.length
        case at => versionStart + at
      val version = if versionEnd - versionStart <= 64 then text.substring(versionStart, versionEnd) else ""
      if !version.matches("[0-9]+\\.[0-9]+") then Left(invalid(versionStart, "Invalid PDF version"))
      else
        lazy val indirectLengths = classicNumericObjects(text, limits.fold(100000L)(_.maxObjects))
        var cursor = versionEnd
        var objects = 0L
        var streams = 0L
        var pages = 0L
        var nodes = 0L
        var maxDepth = 0
        var payload = 0L
        var encrypted = false
        var failure: Option[PdfError] = limits.flatMap(value => checkLimits(
          ScanReport(version, Facts(0, 0, 0), Retention(0, 0, 0, 0), false), value).left.toOption)
        var stopped = false
        def report: ScanReport = ScanReport(version, Facts(objects, streams, pages), Retention(objects.toInt, nodes, maxDepth, payload), encrypted)
        while cursor < text.length && failure.isEmpty && !stopped do
          skipTrivia(text, cursor) match
            case Left((at, message)) => failure = Some(invalid(at, message))
            case Right(next) =>
              cursor = next
              if cursor < text.length then
                indirectHeader(text, cursor) match
                  case Some(header) =>
                    scanObject(text, header.bodyStart, indirectLengths, limits, externalStreams) match
                      case Left((at, message)) => failure = Some(invalid(at, message))
                      case Right(found) =>
                        objects += found.objects
                        streams += found.streams
                        pages += found.pages
                        nodes += found.nodes
                        maxDepth = math.max(maxDepth, found.depth)
                        payload += found.payload
                        encrypted ||= found.encrypted
                        cursor = found.end
                        limits.foreach(value => failure = checkLimits(report, value).left.toOption)
                        if failure.isEmpty then
                          failure = visit(Event.Object(header.ref.number, header.ref.generation, header.bodyStart, found.end, () => indirectLengths)).left.toOption
                          if failure.isEmpty then stopped = stop(report)
                  case None =>
                    if keywordAt(text, cursor, "trailer") then
                      val trailerEnd = findKeyword(text, cursor + 7, "startxref").getOrElse(text.length)
                      encrypted ||= containsName(text, cursor, trailerEnd, "Encrypt")
                      limits.foreach(value => failure = checkLimits(report, value).left.toOption)
                      if failure.isEmpty then failure = visit(Event.Trailer(cursor + 7)).left.toOption
                      cursor = trailerEnd
                    else cursor += 1
        failure.toLeft(report).flatMap { found =>
          if found.facts.objects == 0 then Left(invalid(header, "PDF contains no indirect objects"))
          else if found.facts.pages == 0 && !stopped then Left(invalid(header, "PDF contains no page objects"))
          else Right(found)
        }

  private def scanObject(text: PdfBytes, start: Long, indirectLengths: => Map[ObjectRef, Long], limits: Option[RetentionLimits], externalStreams: Boolean): Either[(Long, String), ObjectScan] =
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
      if limits.exists(l => nodes > l.maxNodes || maxDepth > l.maxDepth || payload > l.maxPayloadBytes) then
        throw PdfBytes.BoundsFault("PDF source metadata exceeds retention limits")
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
          case Right(length) if length < 0 => return Left((cursor, "Invalid PDF stream length"))
          case Right(length) =>
            val dataStart0 = cursor + 6
            val dataStart =
              if text.startsWith("\r\n", dataStart0) then dataStart0 + 2
              else if dataStart0 < text.length && (text.charAt(dataStart0) == '\r' || text.charAt(dataStart0) == '\n') then dataStart0 + 1
              else return Left((cursor, "PDF stream keyword is not followed by a newline"))
            val dataEnd = dataStart.toLong + length
            if length > text.length - dataStart then return Left((cursor, "PDF stream exceeds input"))
            val endstream = skipWhitespace(text, dataEnd)
            // The byte immediately before this exact, /Length-derived offset is
            // payload and need not be a PDF delimiter (qpdf commonly ends a
            // Flate stream with an ordinary byte). Only the trailing boundary
            // belongs to the endstream token.
            if !keywordStartsAt(text, endstream, "endstream") then return Left((endstream, "PDF stream length does not land on endstream"))
            if objectStream then
              val dictionary = text.slice(start, cursor)
              val maxExpanded = math.min(Int.MaxValue.toLong, limits.fold(8L * 1024 * 1024)(_.maxPayloadBytes)).toInt
              if length > maxExpanded then return Left((cursor, "Compressed PDF object stream exceeds metadata budget"))
              val raw = text.copyBytes(dataStart, dataEnd)
              decodeObjectStream(dictionary, raw, cursor, maxExpanded, limits.fold(100000)(_.maxObjects)) match
                case Left(error) => return Left(error)
                case Right(value) => embedded = value
            streams += 1
            if !externalStreams then payload += length
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

  private def indirectHeader(text: PdfBytes, at: Long): Option[ObjectHeader] =
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
      yield ObjectHeader(ObjectRef(number, generation), marker + 3)

  private def streamLength(text: PdfBytes, from: Long, until: Long, indirect: => Map[ObjectRef, Long]): Either[String, Long] =
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
            yield ObjectRef(number, generation)
            found = Some(ref.flatMap(indirect.get).toRight("PDF stream /Length reference cannot be resolved from the classic xref"))
          else found = Some(first.toRight("Invalid direct PDF stream /Length"))
      at += 1
    found.getOrElse(Left("PDF stream does not have a /Length"))

  private def classicNumericObjects(text: PdfBytes, maxObjects: Long): Map[ObjectRef, Long] =
    classicXrefOffsets(text, maxObjects).flatMap { (ref, offset) =>
      Option.when(offset >= 0 && offset < text.length)(offset).flatMap(indirectHeader(text, _)).filter(_.ref == ref).flatMap { header =>
        val start = skipWhitespace(text, header.bodyStart)
        val end = digitsEnd(text, start)
        if end == start || end - start > 20 || !keywordAt(text, skipWhitespace(text, end), "endobj") then None
        else text.substring(start, end).toLongOption.map(ref -> _)
      }
    }

  /** Resolve the latest classic xref table. Malformed and xref-stream inputs
    * simply yield no offsets; callers then reject unresolved indirect lengths.
    */
  private def classicXrefOffsets(text: PdfBytes, maxObjects: Long): Map[ObjectRef, Long] =
    val tail = math.max(0L, text.length - 65536)
    val relative = text.slice(tail, text.length).lastIndexOf("startxref")
    val marker = if relative < 0 then -1L else tail + relative
    if marker < 0 then Map.empty
    else
      val offsetStart = skipWhitespace(text, marker + 9)
      val offsetEnd = digitsEnd(text, offsetStart)
      text.substring(offsetStart, offsetEnd).toLongOption match
        case None => Map.empty
        case Some(xref) if xref < 0 || xref >= text.length || !keywordAt(text, xref, "xref") => Map.empty
        case Some(xref) =>
          val output = Map.newBuilder[ObjectRef, Long]
          var cursor = skipWhitespace(text, xref + 4)
          var done = false
          var entries = 0L
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
                  entries += count
                  if entries > maxObjects + 1 then throw PdfBytes.BoundsFault("PDF xref exceeds object budget")
                  cursor = nextLine(text, countEnd)
                  var index = 0
                  while index < count && cursor < text.length do
                    val lineEnd0 = text.indexWhere(ch => ch == '\r' || ch == '\n', cursor)
                    val lineEnd = if lineEnd0 < 0 then text.length else lineEnd0
                    if lineEnd - cursor > 128 then throw PdfBytes.BoundsFault("PDF xref entry exceeds 128 bytes")
                    val columns = text.substring(cursor, lineEnd).trim.split("\\s+")
                    if columns.length >= 3 && columns(2) == "n" then
                      for
                        offset <- columns(0).toLongOption
                        generation <- columns(1).toIntOption
                      do output += ObjectRef(first + index, generation) -> offset
                    cursor = nextLine(text, lineEnd)
                    index += 1
                  cursor = skipWhitespace(text, cursor)
          output.result()

  private def decodeObjectStream(dictionary: PdfBytes, raw: Array[Byte], at: Long, maxBytes: Int, maxObjects: Int): Either[(Long, String), Embedded] =
    val parameters = for
      count <- integerEntry(dictionary, "N").toRight((at, "PDF object stream is missing /N"))
      first <- integerEntry(dictionary, "First").toRight((at, "PDF object stream is missing /First"))
    yield (count, first)
    parameters.flatMap { case (countValue, firstValue) =>
      if countValue > maxObjects.toLong || firstValue > Int.MaxValue.toLong then Left((at, "PDF object-stream /N or /First is invalid"))
      else
        val count = countValue.toInt
        val first = firstValue.toInt
        val filters = nameEntries(dictionary, "Filter")
        val expanded =
          if filters.isEmpty then Right(raw)
          else if filters == List("FlateDecode") || filters == List("Fl") then inflateBounded(raw, maxBytes)
          else Left(s"Unsupported PDF object-stream filter chain: ${filters.mkString(",")}")
        expanded.left.map(message => (at, message)).flatMap { bytes =>
          if first > bytes.length then Left((at, "PDF object-stream /First exceeds expanded bytes"))
          else
            val text = PdfBytes.borrow(bytes)
            PdfObjectStreamHeader.read(text, count, first).left.map(message => (at, message)).flatMap { entries =>
              val values = entries.map(_._2)
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
                  val body = text.slice(start, end)
                  if typeName(body).contains("Page") then pages += 1
                  val measured = measureValue(body)
                  nodes += measured._1
                  depth = math.max(depth, measured._2)
                  payload += measured._3
                }
                Right(Embedded(count.toLong, pages, nodes, depth, payload))
            }
        }
    }

  private def integerEntry(dictionary: PdfBytes, name: String): Option[Long] =
    val marker = dictionary.indexOf("/" + name)
    if marker < 0 || !tokenBoundary(dictionary, marker + name.length + 1) then None
    else
      val start = skipWhitespace(dictionary, marker + name.length + 1)
      val end = digitsEnd(dictionary, start)
      dictionary.substring(start, end).toLongOption

  private def nameEntries(dictionary: PdfBytes, name: String): List[String] =
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

  private def typeName(body: PdfBytes): Option[String] =
    val marker = body.indexOf("/Type")
    if marker < 0 || !tokenBoundary(body, marker + 5) then None
    else
      val start = skipWhitespace(body, marker + 5)
      Option.when(start < body.length && body.charAt(start) == '/') {
        val end = tokenEnd(body, start + 1)
        body.substring(start + 1, end)
      }

  private def measureValue(body: PdfBytes): (Long, Int, Long) =
    var cursor = 0L
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

  private def nextLine(text: PdfBytes, from: Long): Long =
    var at = from
    while at < text.length && text.charAt(at) != '\r' && text.charAt(at) != '\n' do at += 1
    if at < text.length && text.charAt(at) == '\r' then at += 1
    if at < text.length && text.charAt(at) == '\n' then at += 1
    at

  private def skipTrivia(text: PdfBytes, start: Long): Either[(Long, String), Long] =
    var at = start
    var continue = true
    while at < text.length && continue do
      if text.charAt(at).isWhitespace || text.charAt(at) == 0 then at += 1
      else if text.charAt(at) == '%' && !text.startsWith("%%EOF", at) then
        val end = text.indexWhere(ch => ch == '\r' || ch == '\n', at)
        at = if end < 0 then text.length else end + 1
      else continue = false
    Right(at)

  private def containsName(text: PdfBytes, from: Long, until: Long, name: String): Boolean =
    val needle = "/" + name
    var at = text.indexOf(needle, from)
    while at >= 0 && at < until do
      if tokenBoundary(text, at + needle.length) then return true
      at = text.indexOf(needle, at + 1)
    false

  private def findKeyword(text: PdfBytes, from: Long, keyword: String): Option[Long] =
    var at = text.indexOf(keyword, from)
    while at >= 0 do
      if keywordAt(text, at, keyword) then return Some(at)
      at = text.indexOf(keyword, at + 1)
    None

  private def keywordAt(text: PdfBytes, at: Long, keyword: String): Boolean =
    at >= 0 && text.startsWith(keyword, at) &&
      (at == 0 || delimiter(text.charAt(at - 1))) && tokenBoundary(text, at + keyword.length)

  private def keywordStartsAt(text: PdfBytes, at: Long, keyword: String): Boolean =
    at >= 0 && text.startsWith(keyword, at) && tokenBoundary(text, at + keyword.length)

  private def tokenBoundary(text: PdfBytes, at: Long): Boolean = at >= text.length || delimiter(text.charAt(at))
  private def delimiter(ch: Char): Boolean = ch.isWhitespace || "()<>[]{}/%".contains(ch) || ch == 0
  private def skipWhitespace(text: PdfBytes, start: Long): Long =
    var at = start
    while at < text.length && (text.charAt(at).isWhitespace || text.charAt(at) == 0) do at += 1
    at
  private def digitsEnd(text: PdfBytes, start: Long): Long =
    var at = start
    while at < text.length && text.charAt(at).isDigit do at += 1
    at
  private def tokenEnd(text: PdfBytes, start: Long): Long =
    var at = start
    while at < text.length && !delimiter(text.charAt(at)) do at += 1
    if at == start then start + 1 else at
