package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_16BE}
import java.security.MessageDigest
import scala.collection.mutable
import kyo.*

/** One-source-pass packet stitching. Body objects precede the generated front
  * matter physically; the final page tree puts the cover/contents first.
  * Pulls own their chunks. Backpressure retains at most the current source metadata,
  * one output chunk and bounded page/xref metadata, never earlier payloads.
  */
object PdfPacket:
  import PdfValue.*

  final val ChunkBytes = 64 * 1024
  final val DefaultSourceBytes = 64L * 1024 * 1024 * 1024
  final val DefaultOutputBytes = 64L * 1024 * 1024 * 1024
  final val DefaultDocuments = 256
  final val DefaultPages = 8192
  final val DefaultObjects = 100000
  final val RowsPerPage = 18
  enum Delivery derives Schema:
    case Live, Staged
  final case class Settings(title: String = "Combined documents") derives Schema
  final case class Bounds(sourceBytes: Long = DefaultSourceBytes, outputBytes: Long = DefaultOutputBytes,
      documents: Int = DefaultDocuments, pages: Int = DefaultPages, objects: Int = DefaultObjects,
      memoryBytes: Long = 64L * 1024 * 1024, spoolBytes: Long = 128L * 1024 * 1024 * 1024,
      windowBytes: Int = ChunkBytes) derives Schema:
    // Conservative budgets for syntax and bookkeeping, independent of blobs.
    private[kyopdf] def metadataBytes: Long = memoryBytes / 16
    private[kyopdf] def retention: RetentionLimits = RetentionLimits(maxObjects = math.min(objects.toLong, memoryBytes / 1024).toInt,
      maxNodes = math.min(Int.MaxValue.toLong, memoryBytes / 1024).toInt, maxDepth = 64, maxPayloadBytes = metadataBytes)
  final case class Entry(title: String, firstPage: Int, pages: Int) derives Schema
  final case class Receipt(entries: Vector[Entry], coverPages: Int, pages: Int, bytes: Long, sha256: String) derives Schema

  /** The array is private to adapters; callers cannot mutate emitted bytes. */
  final class OutputChunk private[PdfPacket] (private[kyopdf] val array: scala.Array[Byte], val size: Int):
    def apply(index: Int): Byte =
      require(index >= 0 && index < size)
      array(index)

  private final case class Fault(error: PdfError) extends RuntimeException(error.message)
  private def fail(message: String): Nothing = throw Fault(PdfError.InvalidPdf(message))
  private def limit(message: String): Nothing = throw Fault(PdfError.RetentionExceeded(message))
  private val punctuation = Map(0x20ac -> 128, 0x201a -> 130, 0x0192 -> 131, 0x201e -> 132,
    0x2026 -> 133, 0x2020 -> 134, 0x2021 -> 135, 0x02c6 -> 136, 0x2030 -> 137, 0x0160 -> 138,
    0x2039 -> 139, 0x0152 -> 140, 0x017d -> 142, 0x2018 -> 145, 0x2019 -> 146, 0x201c -> 147,
    0x201d -> 148, 0x2022 -> 149, 0x2013 -> 150, 0x2014 -> 151, 0x02dc -> 152, 0x2122 -> 153,
    0x0161 -> 154, 0x203a -> 155, 0x0153 -> 156, 0x017e -> 158, 0x0178 -> 159)
  private def winAnsi(c: Char): Option[Byte] =
    if c >= ' ' && c <= '~' || c >= 160 && c <= 255 then Some(c.toByte)
    else punctuation.get(c.toInt).map(_.toByte)
  def validLabel(value: String): Boolean = value != null && value.nonEmpty && value.length <= 120 && value.forall(c => winAnsi(c).nonEmpty)
  def validBounds(b: Bounds): Boolean = b.sourceBytes > 0 && b.sourceBytes < Long.MaxValue && b.outputBytes > 0 && b.outputBytes < Long.MaxValue && b.spoolBytes > 0 &&
    b.memoryBytes >= 1024 * 1024 && b.memoryBytes <= Int.MaxValue.toLong * 16 &&
    b.windowBytes >= 4096 && b.windowBytes <= b.memoryBytes / 16 &&
    b.documents > 0 && b.documents.toLong * 1024 <= b.memoryBytes / 8 &&
    b.pages > 0 && b.pages.toLong * 128 <= b.memoryBytes / 8 &&
    b.objects >= 16 && b.objects < Int.MaxValue && (b.objects.toLong + 1) * 8 <= b.memoryBytes / 8
  private def text(value: String): Iterator[Byte] = value.getBytes(ISO_8859_1).iterator
  private def literal(value: String): Literal = Literal(value.iterator.map(c => winAnsi(c).get).toVector)
  private def documentText(value: String): PdfValue =
    if value.forall(_ <= '~') then literal(value)
    else Hex(Vector(0xfe.toByte, 0xff.toByte) ++ value.getBytes(UTF_16BE).toVector)
  private def destination(ref: ObjectRef): PdfValue.Array = PdfValue.Array(Vector(Ref(ref), Name("Fit")))

  /** Validate before allocating xref state or accepting an input. */
  def open(settings: Settings = Settings(), bounds: Bounds = Bounds()): Session < (Abort[PdfError] & Sync) =
    if !validLabel(settings.title) then Abort.fail(PdfError.InvalidPdf("Use a nonempty cover title up to 120 WinAnsi characters"))
    else if !validBounds(bounds) then Abort.fail(PdfError.InvalidPdf("Invalid packet bounds"))
    else Sync.defer(new Session(settings, bounds))

  final class Session private[PdfPacket] (settings: Settings, val bounds: Bounds) extends AutoCloseable:
    private case class Section(title: String, first: ObjectRef, pages: Int, offset: Int)
    private val offsets = new scala.Array[Long](bounds.objects + 1)
    private val digest = MessageDigest.getInstance("SHA-256")
    private var next = 3
    private var position = 0L
    private var bodyPages = Vector.empty[ObjectRef]
    private var sections = Vector.empty[Section]
    private var current: Option[Pull] = None
    private var closed = false
    private var started = false
    private var version2 = false
    private var completed: Option[Receipt] = None
    def receipt: Option[Receipt] = completed
    def bytesProduced: Long = position
    private def allocate(): ObjectRef =
      if next > bounds.objects then limit("Packet object limit exceeded")
      val ref = ObjectRef(next); next += 1; ref
    private def ready(): Unit =
      if closed || completed.nonEmpty then fail("Packet session is closed or finished")
      if current.nonEmpty then fail("Consume the current packet pull before adding another input")
    private def checked[A](body: => A): A < (Abort[PdfError] & Sync) =
      Abort.catching[Exception] { error =>
        close()
        error match
          case fault: Fault => fault.error
          case _: java.io.IOException => PdfError.InvalidPdf("Packet payload read failed")
          case other => throw other
      }(Sync.defer(body))
    private def pageEntries(document: PdfDocument): Vector[(ObjectRef, Dict)] < (Abort[PdfError] & Sync) =
      Abort.run[PdfError](PdfPages.pageEntries(document)).map {
        case Result.Success(pages) => pages
        case other => Sync.defer(close()).map(_ => Abort.get(other))
      }
    private def source(result: => Iterator[Byte], finish: () => Unit = () => ()): Pull =
      val value = new Pull(() => result, finish)
      current = Some(value)
      value

    private[kyopdf] def admit(title: String): Unit < (Abort[PdfError] & Sync) = checked {
      ready()
      if !validLabel(title) then fail("Invalid packet document title")
      if sections.length >= bounds.documents then limit("Packet document limit exceeded")
    }

    /** No previous input's payload is retained once its pull reaches EOF.
      * Page graphics and fonts are preserved; new document navigation replaces
      * source outlines/tags. Named local links are resolved before remapping.
      */
    def append(title: String, document: PdfDocument): Pull < (Abort[PdfError] & Sync) = appendDocument(title, document, None)

    def append(title: String, input: PdfPacketSource): Pull < (Abort[PdfError] & Sync) =
      if input.isClosed then Abort.fail(PdfError.InvalidPdf("Packet source is closed"))
      else if input.size > bounds.sourceBytes then Abort.fail(PdfError.TooLarge(bounds.sourceBytes, input.size))
      else appendDocument(title, input.document, Some(input))

    private def appendDocument(title: String, document: PdfDocument, input: Option[PdfPacketSource]): Pull < (Abort[PdfError] & Sync) = checked {
      ready()
      if !validLabel(title) then fail("Use a nonempty document title up to 120 WinAnsi characters")
      if sections.length >= bounds.documents then limit("Packet document limit exceeded")
      if !Set("1.0", "1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7", "2.0")(document.version) then fail("Unsupported packet PDF version")
    }.map { _ => pageEntries(document).map { pages => checked {
      ready()
      if pages.isEmpty then fail("Packet inputs must contain at least one page")
      val front = (sections.length + 1 + RowsPerPage - 1) / RowsPerPage
      if pages.length > bounds.pages - bodyPages.length - front then limit("Packet page limit exceeded")
      val catalog = document.root.flatMap(document.byRef.get).flatMap(_.dictionary).getOrElse(fail("Missing packet source catalog"))
      if catalog.get("AcroForm").nonEmpty then fail("Interactive PDF forms must be flattened before stitching")
      val link = new LocalLinks(document, catalog, pages.map(_._1).toSet)
      val pageValues = pages.map((ref, page) => ref -> page.removed("Parent").removed("StructParents")
        .updated("Resources", page.get("Resources").getOrElse(dict()))).toMap
      val found = mutable.LinkedHashMap.empty[ObjectRef, PdfObject]
      val pending = mutable.ArrayDeque.from(pages.map(_._1))
      var payload = 0L
      var retainedStreams = 0L
      var nodes = 0
      def references(value: PdfValue): Vector[ObjectRef] =
        val todo = mutable.ArrayDeque(value -> 0)
        val refs = Vector.newBuilder[ObjectRef]
        while todo.nonEmpty do
          val (v, depth) = todo.removeLast()
          nodes += 1
          if nodes.toLong > bounds.retention.maxNodes || depth > 64 then limit("Packet source metadata limit exceeded")
          v match
            case Ref(r) => refs += r
            case PdfValue.Array(values) => values.foreach(v => todo.append(v -> (depth + 1)))
            case Dict(fields) => fields.foreach { field =>
              payload += field.name.length.toLong * 2
              todo.append(field.value -> (depth + 1))
            }
            case Literal(bytes) => payload += bytes.length
            case Hex(bytes) => payload += bytes.length
            case Name(value) => payload += value.length.toLong * 2
            case _ => ()
          if payload > bounds.metadataBytes then limit("Packet source retention limit exceeded")
        refs.result()
      while pending.nonEmpty do
        val ref = pending.removeHead()
        if !found.contains(ref) then
          if found.size >= bounds.objects - next + 1 then limit("Packet object limit exceeded")
          val obj = document.byRef.getOrElse(ref, fail("Missing packet source reference"))
          val value = link.clean(pageValues.getOrElse(ref, obj.value))
          value match
            case d: Dict if d.get("Type").contains(Name("Catalog")) || d.get("Type").contains(Name("Pages")) =>
              fail("Page dependency refers to a source catalog or page-tree node")
            case d: Dict if d.get("Subtype").contains(Name("Widget")) => fail("Interactive PDF widgets must be flattened before stitching")
            case _ => ()
          retainedStreams += obj.stream.fold(0L)(_.length.toLong)
          if retainedStreams > bounds.memoryBytes / 2 then limit("Materialized packet streams exceed the memory budget")
          found(ref) = obj.copy(value = value)
          references(value).foreach(pending.append)
      if found.size.toLong + next + sections.size + 2L * front + 4 > bounds.objects then limit("Packet objects leave no room for front matter")
      val mapping = found.keysIterator.map(ref => ref -> allocate()).toMap
      def rewrite(value: PdfValue): PdfValue = value match
        case Ref(ref) => Ref(mapping.getOrElse(ref, fail("Unmapped packet source reference")))
        case PdfValue.Array(values) => PdfValue.Array(values.map(rewrite))
        case Dict(fields) => Dict(fields.map(field => field.copy(value = rewrite(field.value))))
        case other => other
      val first = mapping(pages.head._1)
      sections :+= Section(title, first, pages.length, bodyPages.length)
      bodyPages ++= pages.map(page => mapping(page._1))
      version2 ||= document.version == "2.0" || catalog.get("Version").contains(Name("2.0"))
      val header = if started then Iterator.empty else { started = true; text("%PDF-1.7\n%\u00e2\u00e3\u00cf\u00d3\n") }
      source(header ++ found.valuesIterator.flatMap { original =>
        val value = rewrite(original.value)
        val page = if pageValues.contains(original.index) then value match
          case d: Dict => d.updated("Parent", Ref(ObjectRef(2)))
          case _ => fail("Packet page is not a dictionary")
        else value
        objectBytes(original.copy(index = mapping(original.index), value = page), input.flatMap(source =>
          source.streams.get(original.index).map(span => span.length -> (() => source.bytes(span)))))
      })
    }}}

    /** Appends cover/TOC, outlines, page tree, catalog and a fresh classic xref.
      * The receipt becomes available only after this pull reaches EOF.
      */
    def finish(): Pull < (Abort[PdfError] & Sync) = checked {
      ready()
      if sections.isEmpty then fail("Stitch at least one PDF")
      val count = (sections.length + RowsPerPage - 1) / RowsPerPage
      val entries = sections.map(s => Entry(s.title, count + s.offset + 1, s.pages))
      val font = allocate(); val bold = allocate(); val outlines = allocate()
      val bookmarks = sections.map(_ => allocate())
      val covers = (0 until count).toVector.map(_ => allocate() -> allocate())
      val fonts = Vector(
        PdfObject(font, dict("Type" -> Name("Font"), "Subtype" -> Name("Type1"), "BaseFont" -> Name("Courier"), "Encoding" -> Name("WinAnsiEncoding"))),
        PdfObject(bold, dict("Type" -> Name("Font"), "Subtype" -> Name("Type1"), "BaseFont" -> Name("Courier-Bold"), "Encoding" -> Name("WinAnsiEncoding"))))
      val navigation = Vector(PdfObject(outlines, dict("Type" -> Name("Outlines"), "First" -> Ref(bookmarks.head),
        "Last" -> Ref(bookmarks.last), "Count" -> Number(sections.length)))) ++ sections.zipWithIndex.map { (entry, i) =>
        var value = dict("Title" -> documentText(entry.title), "Parent" -> Ref(outlines), "Dest" -> destination(entry.first))
        if i > 0 then value = value.updated("Prev", Ref(bookmarks(i - 1)))
        if i + 1 < bookmarks.length then value = value.updated("Next", Ref(bookmarks(i + 1)))
        PdfObject(bookmarks(i), value)
      }
      val coverObjects = covers.zipWithIndex.flatMap { case ((page, content), index) =>
        val rows = entries.slice(index * RowsPerPage, (index + 1) * RowsPerPage)
        val commands = new StringBuilder
        def line(value: String, x: Double, y: Double, size: Int, heading: Boolean = false): Unit =
          commands.append(s"BT /${if heading then "Bold" else "Text"} $size Tf 1 0 0 1 $x $y Tm ")
          val _ = commands.append(encoded(literal(value))).append(" Tj ET\n")
        wrap(settings.title, 40).zipWithIndex.foreach((value, i) => line(value, 48, 744 - 23 * i, 20, true))
        line(s"${sections.size} documents | ${bodyPages.size + count} pages", 48, 654, 10)
        line(if index == 0 then "TABLE OF CONTENTS" else "CONTENTS - CONTINUED", 48, 628, 12, true)
        commands.append("0.7 G 48 616 m 564 616 l S 0 G\n")
        val links = rows.zipWithIndex.map { (entry, row) =>
          val y = 596 - row * 30
          val n = index * RowsPerPage + row
          wrap(s"${n + 1}. ${entry.title}", 70).zipWithIndex.foreach((value, i) => line(value, 48, y - i * 12, 10))
          val range = if entry.pages == 1 then entry.firstPage.toString else s"${entry.firstPage}-${entry.firstPage + entry.pages - 1}"
          line(range, 564 - range.length * 5.4, y, 9)
          dict("Type" -> Name("Annot"), "Subtype" -> Name("Link"),
            "Rect" -> PdfValue.Array(Vector(Number(48), Number(y - 15), Number(564), Number(y + 11))),
            "Border" -> PdfValue.Array(Vector(Number(0), Number(0), Number(0))), "Dest" -> destination(sections(n).first))
        }
        line(s"Contents ${index + 1} of $count", 48, 38, 9)
        Vector(PdfObject(page, dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)),
          "MediaBox" -> PdfValue.Array(Vector(Number(0), Number(0), Number(612), Number(792))),
          "Resources" -> dict("Font" -> dict("Text" -> Ref(font), "Bold" -> Ref(bold))),
          "Contents" -> Ref(content), "Annots" -> PdfValue.Array(links))),
          PdfObject(content, dict(), Some(commands.result().getBytes(ISO_8859_1).toVector)))
      }
      val kids = covers.map(pair => Ref(pair._1)) ++ bodyPages.map(Ref.apply)
      val catalog = dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)), "Outlines" -> Ref(outlines),
        "PageMode" -> Name("UseOutlines"), "Version" -> Name(if version2 then "2.0" else "1.7"))
      val last = Vector(PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Kids" -> PdfValue.Array(kids), "Count" -> Number(kids.size))),
        PdfObject(ObjectRef(1), catalog))
      source((fonts ++ navigation ++ coverObjects ++ last).iterator.flatMap(objectBytes) ++ Iterator.single(()).flatMap { _ =>
        val xref = position
        if xref <= 9999999999L then
          text(s"xref\n0 $next\n0000000000 65535 f \n") ++ (1 until next).iterator.flatMap { id =>
            text(f"${offsets(id)}%010d 00000 n \n")
          } ++ text(s"trailer\n<< /Root 1 0 R /Size $next >>\nstartxref\n$xref\n%%EOF\n")
        else
          val ref = allocate()
          offsets(ref.number.toInt) = xref
          val length = next.toLong * 11
          text(s"${ref.number} 0 obj\n<< /Type /XRef /Root 1 0 R /Size $next /W [1 8 2] /Length $length >>\nstream\n") ++
            (0 until next).iterator.flatMap(id => xrefEntry(if id == 0 then 0 else 1, offsets(id), if id == 0 then 65535 else 0)) ++
            text(s"\nendstream\nendobj\nstartxref\n$xref\n%%EOF\n")
      }, () => completed = Some(Receipt(entries, count, kids.size, position,
        digest.digest().iterator.map(b => f"${b & 255}%02x").mkString)))
    }

    final class Pull private[Session] (make: () => Iterator[Byte], finish: () => Unit):
      private var factory: Option[() => Iterator[Byte]] = Some(make)
      private var iterator: Option[Iterator[Byte]] = None
      private var ended = false
      /** Each returned array is fresh and is never mutated again. */
      def read(): Option[OutputChunk] < (Abort[PdfError] & Sync) = checked {
        if closed then fail("Packet session is closed")
        if ended then None
        else
          val input = iterator.getOrElse { val it = factory.get.apply(); factory = None; iterator = Some(it); it }
          val block = new scala.Array[Byte](ChunkBytes)
          var n = 0
          while n < block.length && input.hasNext do
            if position >= bounds.outputBytes then throw Fault(PdfError.TooLarge(bounds.outputBytes, position + 1))
            block(n) = input.next()
            n += 1; position += 1
          if n == 0 then
            ended = true; iterator = None; current = None; finish(); None
          else
            digest.update(block, 0, n)
            Some(new OutputChunk(block, n))
      }
      private[Session] def discard(): Unit = { iterator = None; factory = None; ended = true }
    def close(): Unit =
      closed = true
      current.foreach(_.discard()); current = None
      bodyPages = Vector.empty; sections = Vector.empty

    private def objectBytes(obj: PdfObject): Iterator[Byte] = objectBytes(obj, None)
    private def objectBytes(obj: PdfObject, external: Option[(Long, () => Iterator[Byte])]): Iterator[Byte] =
      offsets(obj.index.number.toInt) = position
      val header = text(s"${obj.index.number} 0 obj\n")
      external match
        case Some((length, bytes)) => obj.value match
          case d: Dict => header ++ encode(d.updated("Length", Number(length))) ++ text("\nstream\n") ++ bytes() ++ text("\nendstream\nendobj\n")
          case _ => fail("PDF stream object requires a dictionary")
        case None => obj.stream match
          case None => header ++ encode(obj.value) ++ text("\nendobj\n")
          case Some(bytes) => obj.value match
            case d: Dict => header ++ encode(d.updated("Length", Number(bytes.length))) ++ text("\nstream\n") ++ bytes.iterator ++ text("\nendstream\nendobj\n")
            case _ => fail("PDF stream object requires a dictionary")

  private[kyopdf] def xrefEntry(kind: Int, offset: Long, generation: Int): Iterator[Byte] =
    Iterator.single(kind.toByte) ++ (7 to 0 by -1).iterator.map(i => (offset >>> (i * 8)).toByte) ++
      Iterator((generation >>> 8).toByte, generation.toByte)

  private def wrap(value: String, width: Int): Vector[String] =
    if value.length <= width then Vector(value)
    else
      val space = value.lastIndexOf(' ', width)
      val split = if space > width / 2 then space else width
      Vector(value.take(split)) ++ wrap(value.drop(split).stripLeading(), width)
  private def encoded(value: PdfValue): String = encode(value).map(b => (b & 255).toChar).mkString
  private def encode(value: PdfValue, depth: Int = 0): Iterator[Byte] =
    if depth > 64 then limit("Packet value nesting exceeds 64 levels")
    value match
      case Null => text("null")
      case Bool(value) => text(value.toString)
      case Number(value) =>
        val raw = value.bigDecimal
        if raw.precision.toLong + math.abs(raw.scale.toLong) > 65536 then limit("Packet number token exceeds 64 KiB")
        text(raw.stripTrailingZeros.toPlainString)
      case Name(value) => text("/") ++ value.iterator.flatMap { c =>
        if c > 255 then fail("PDF names must contain byte-valued characters")
        if c <= ' ' || c >= 127 || "#/%()<>[]{}".contains(c) then text(f"#${c.toInt}%02X") else Iterator.single(c.toByte)
      }
      case Literal(bytes) => text("(") ++ bytes.iterator.flatMap { byte => (byte & 255).toChar match
        case '(' => text("\\(")
        case ')' => text("\\)")
        case '\\' => text("\\\\")
        case c if c < ' ' || c > '~' => text(f"\\${byte & 255}%03o")
        case _ => Iterator.single(byte)
      } ++ text(")")
      case Hex(bytes) => text("<") ++ bytes.iterator.flatMap(b => text(f"${b & 255}%02X")) ++ text(">")
      case Ref(ref) => text(s"${ref.number} ${ref.generation} R")
      case PdfValue.Array(values) => text("[") ++ values.iterator.flatMap(v => encode(v, depth + 1) ++ text(" ")) ++ text("]")
      case Dict(fields) => text("<< ") ++ fields.iterator.flatMap(f => encode(Name(f.name), depth + 1) ++ text(" ") ++ encode(f.value, depth + 1) ++ text(" ")) ++ text(">>")

  private final class LocalLinks(document: PdfDocument, catalog: Dict, pages: Set[ObjectRef]):
    private def resolve(value: PdfValue, seen: Set[ObjectRef] = Set.empty): PdfValue = value match
      case Ref(ref) =>
        if seen(ref) || seen.size >= 64 then fail("Cyclic packet metadata reference")
        resolve(document.byRef.getOrElse(ref, fail("Missing packet metadata reference")).value, seen + ref)
      case other => other
    private def key(value: PdfValue): Vector[Byte] = value match
      case Name(value) => value.getBytes(ISO_8859_1).toVector
      case Literal(bytes) => bytes
      case Hex(bytes) => bytes
      case _ => fail("Invalid named destination")
    private lazy val named: (Map[Vector[Byte], PdfValue], Map[Vector[Byte], PdfValue]) =
      val legacy = mutable.Map.empty[Vector[Byte], PdfValue]
      val found = mutable.Map.empty[Vector[Byte], PdfValue]
      def add(table: mutable.Map[Vector[Byte], PdfValue], name: Vector[Byte], value: PdfValue): Unit =
        if table.contains(name) then fail("Duplicate source destination name")
        if table.size >= 8192 then limit("Source has too many named destinations")
        table(name) = value
      catalog.get("Dests").foreach(v => resolve(v) match
        case d: Dict => d.fields.foreach(f => add(legacy, key(Name(f.name)), f.value))
        case _ => fail("Invalid source destinations"))
      val pending = mutable.ArrayDeque.empty[(PdfValue, Int)]
      catalog.get("Names").foreach(v => resolve(v) match
        case d: Dict => d.get("Dests").foreach(v => pending.append(v -> 0))
        case _ => fail("Invalid source names"))
      var visited = 0
      while pending.nonEmpty do
        val (value, depth) = pending.removeHead(); visited += 1
        if visited > 8192 || depth > 64 then limit("Source destination tree exceeds packet bounds")
        resolve(value) match
          case d: Dict =>
            d.get("Names").foreach(v => resolve(v) match
              case PdfValue.Array(values) if values.length % 2 == 0 => values.grouped(2).foreach(pair => add(found, key(pair(0)), pair(1)))
              case _ => fail("Invalid destination name tree"))
            d.get("Kids").foreach(v => resolve(v) match
              case PdfValue.Array(values) => values.foreach(v => pending.append(v -> (depth + 1)))
              case _ => fail("Invalid destination name-tree children"))
          case _ => fail("Invalid source destination tree")
      legacy.toMap -> found.toMap
    private def target(value: PdfValue, depth: Int = 0): PdfValue =
      if depth > 64 then fail("Cyclic named packet destination")
      resolve(value) match
        case a @ PdfValue.Array(Ref(page) +: _) if pages(page) => a
        case d: Dict => target(d.get("D").getOrElse(fail("Destination has no D entry")), depth + 1)
        case name @ (Name(_) | Literal(_) | Hex(_)) =>
          val (legacy, strings) = named
          val (first, second) = name match
            case Name(_) => legacy -> strings
            case _ => strings -> legacy
          target(first.get(key(name)).orElse(second.get(key(name))).getOrElse(fail("Unresolved named packet destination")), depth + 1)
        case _ => fail("Packet link destination must target a source page")
    def clean(value: PdfValue, depth: Int = 0): PdfValue =
      if depth > 64 then limit("Packet value nesting exceeds 64 levels")
      value match
        case d: Dict => Dict(d.fields.filterNot(f => Set("StructParents", "StructParent")(f.name)).map { field =>
          val isTarget = field.name == "Dest" && d.get("Subtype").contains(Name("Link")) ||
            field.name == "D" && d.get("S").contains(Name("GoTo"))
          field.copy(value = clean(if isTarget then target(field.value) else field.value, depth + 1))
        })
        case PdfValue.Array(values) => PdfValue.Array(values.map(clean(_, depth + 1)))
        case other => other
