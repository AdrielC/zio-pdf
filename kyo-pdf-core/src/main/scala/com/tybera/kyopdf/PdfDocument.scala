package com.tybera.kyopdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.zip.InflaterInputStream
import kyo.*

final case class ObjectRef(number: Long, generation: Int = 0) derives Schema

final case class PdfField(name: String, value: PdfValue) derives Schema

enum PdfValue derives Schema:
  case Null
  case Bool(value: Boolean)
  case Number(value: BigDecimal)
  case Name(value: String)
  case Literal(bytes: Vector[Byte])
  case Hex(bytes: Vector[Byte])
  case Array(values: Vector[PdfValue])
  case Dict(fields: Vector[PdfField])
  case Ref(value: ObjectRef)

object PdfValue:
  def dict(fields: (String, PdfValue)*): PdfValue.Dict =
    PdfValue.Dict(fields.map(PdfField.apply).toVector)

  extension (dict: PdfValue.Dict)
    def get(name: String): Option[PdfValue] = dict.fields.reverseIterator.find(_.name == name).map(_.value)
    def removed(name: String): PdfValue.Dict = PdfValue.Dict(dict.fields.filterNot(_.name == name))
    def updated(name: String, value: PdfValue): PdfValue.Dict =
      val index = dict.fields.lastIndexWhere(_.name == name)
      if index < 0 then PdfValue.Dict(dict.fields :+ PdfField(name, value))
      else PdfValue.Dict(dict.fields.updated(index, PdfField(name, value)))

final case class PdfObject(index: ObjectRef, value: PdfValue, stream: Option[Vector[Byte]] = None) derives Schema:
  def dictionary: Option[PdfValue.Dict] = value match
    case value: PdfValue.Dict => Some(value)
    case _ => None

final case class PdfDocument(version: String, objects: Vector[PdfObject], trailer: PdfValue.Dict) derives Schema:
  lazy val byRef: Map[ObjectRef, PdfObject] = objects.map(value => value.index -> value).toMap
  def root: Option[ObjectRef] = trailer.get("Root").collect { case PdfValue.Ref(value) => value }
  def encrypted: Boolean = trailer.get("Encrypt").nonEmpty

/** Immutable bounded input accumulator for chunked Kyo ingestion.
  *
  * Chunks remain separate while input arrives. `finish` performs the only
  * contiguous allocation required by the random-access PDF decoder.
  */
final case class PdfInput private (limit: ByteLimit, chunks: Vector[Vector[Byte]], size: Long) derives Schema:
  def feed(bytes: Array[Byte]): PdfInput < Abort[PdfError] =
    if bytes.length.toLong > limit.toLong - size then Abort.fail(PdfError.TooLarge(limit.toLong, size + bytes.length))
    else copy(chunks = chunks :+ bytes.toVector, size = size + bytes.length)

  def finish(limits: RetentionLimits = RetentionLimits()): PdfDocument < Abort[PdfError] =
    if size > Int.MaxValue then Abort.fail(PdfError.TooLarge(Int.MaxValue.toLong, size))
    else
      val output = new Array[Byte](size.toInt)
      var offset = 0
      chunks.foreach { chunk =>
        chunk.copyToArray(output, offset)
        offset += chunk.length
      }
      PdfDocumentParser.decode(output, limit, limits)

object PdfInput:
  def empty(limit: ByteLimit): PdfInput = PdfInput(limit, Vector.empty, 0L)

object PdfDocument:
  def decode(bytes: Array[Byte], limit: ByteLimit, limits: RetentionLimits = RetentionLimits()): PdfDocument < Abort[PdfError] =
    PdfDocumentParser.decode(bytes, limit, limits)

  def decodedStream(obj: PdfObject, maxBytes: Int = 512 * 1024): Vector[Byte] < (Abort[PdfError] & Sync) =
    obj.stream match
      case None => Abort.fail(PdfError.InvalidPdf(s"Object ${obj.index.number} has no stream"))
      case Some(raw) =>
        val filters = obj.dictionary.toVector.flatMap(_.get("Filter").toVector).flatMap {
          case PdfValue.Name(value) => Vector(value)
          case PdfValue.Array(values) => values.collect { case PdfValue.Name(value) => value }
          case _ => Vector("unsupported")
        }
        if filters.isEmpty then
          if raw.length > maxBytes then Abort.fail(PdfError.TooLarge(maxBytes.toLong, raw.length.toLong)) else raw
        else if filters == Vector("FlateDecode") || filters == Vector("Fl") then
          Abort.catching[Throwable](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))) {
            Sync.defer(inflate(raw.toArray, maxBytes).toVector)
          }
        else Abort.fail(PdfError.InvalidPdf(s"Unsupported PDF stream filter chain: ${filters.mkString(",")}"))

  private def inflate(bytes: Array[Byte], maxBytes: Int): Array[Byte] =
    val input = new InflaterInputStream(new ByteArrayInputStream(bytes))
    val output = new ByteArrayOutputStream(math.min(maxBytes, math.max(32, bytes.length * 2)))
    val buffer = new Array[Byte](8192)
    try
      var total = 0
      var read = input.read(buffer)
      while read >= 0 do
        if read > 0 then
          total += read
          if total > maxBytes then throw IllegalArgumentException(s"Expanded PDF stream exceeds $maxBytes bytes")
          output.write(buffer, 0, read)
        read = input.read(buffer)
      output.toByteArray
    finally input.close()

object PdfPages:
  import PdfValue.*

  def pageRefs(document: PdfDocument): Vector[ObjectRef] < Abort[PdfError] =
    pageEntries(document).map(_.map(_._1))

  private def pageEntries(document: PdfDocument): Vector[(ObjectRef, Dict)] < Abort[PdfError] =
    val objects = document.byRef
    def value(ref: ObjectRef): Option[PdfValue] = objects.get(ref).map(_.value)
    def kind(dict: Dict): Option[String] = dict.get("Type").collect { case Name(name) => name }
    val inheritedKeys = Vector("Resources", "MediaBox", "CropBox", "Rotate")
    def walk(current: ObjectRef, inherited: Map[String, PdfValue], seen: Set[ObjectRef], depth: Int): Either[PdfError, Vector[(ObjectRef, Dict)]] =
      if depth > 64 then Left(PdfError.RetentionExceeded("PDF page tree exceeds 64 levels"))
      else if seen(current) then Left(PdfError.InvalidPdf("PDF page tree contains a cycle"))
      else value(current) match
        case Some(dict: Dict) if kind(dict).contains("Page") =>
          val materialized = inherited.foldLeft(dict) { case (page, (name, inheritedValue)) =>
            if page.get(name).isEmpty then page.updated(name, inheritedValue) else page
          }
          Right(Vector(current -> materialized))
        case Some(dict: Dict) if kind(dict).contains("Pages") =>
          val next = inherited ++ inheritedKeys.flatMap(name => dict.get(name).map(name -> _))
          dict.get("Kids") match
            case Some(Array(kids)) =>
              kids.foldLeft[Either[PdfError, Vector[(ObjectRef, Dict)]]](Right(Vector.empty)) {
                case (result, Ref(child)) => result.flatMap(found => walk(child, next, seen + current, depth + 1).map(found ++ _))
                case (_, _) => Left(PdfError.InvalidPdf("PDF page tree contains a non-reference child"))
              }
            case _ => Left(PdfError.InvalidPdf("PDF page tree has no Kids array"))
        case _ => Left(PdfError.InvalidPdf(s"Unreadable PDF page-tree object ${current.number}"))

    if document.encrypted then Abort.fail(PdfError.InvalidPdf("Encrypted PDFs cannot be rewritten"))
    else
      val pages = for
        root <- document.root.toRight(PdfError.InvalidPdf("PDF trailer has no Root reference"))
        catalog <- value(root).collect { case value: Dict => value }.toRight(PdfError.InvalidPdf("PDF catalog is unreadable"))
        pages <- catalog.get("Pages").collect { case Ref(value) => value }.toRight(PdfError.InvalidPdf("PDF catalog has no Pages reference"))
        found <- walk(pages, Map.empty, Set.empty, 0)
      yield found
      pages.fold(Abort.fail, identity)

  def select(document: PdfDocument, first: Int, last: Int): PdfDocument < Abort[PdfError] =
    pageEntries(document).map { pages =>
      if first < 1 || last < first || last > pages.length then Abort.fail(PdfError.InvalidPageRange(first, last, pages.length))
      else rebuild(document, pages.slice(first - 1, last)).fold(Abort.fail, identity)
    }

  private def rebuild(document: PdfDocument, selected: Vector[(ObjectRef, Dict)]): Either[PdfError, PdfDocument] =
    val objects = document.byRef
    val selectedRefs = selected.map(_._1)
    for
      originalRoot <- document.root.toRight(PdfError.InvalidPdf("PDF trailer has no Root reference"))
      catalog <- objects.get(originalRoot).flatMap(_.dictionary).toRight(PdfError.InvalidPdf("PDF catalog is unreadable"))
      retained <- dependencyClosure(objects, catalog, selected)
      dependencies = retained.filterNot(selectedRefs.contains)
      mapping = (selectedRefs ++ dependencies).zipWithIndex.map { (ref, index) => ref -> ObjectRef(index + 3L) }.toMap
      selectedObjects <- selected.foldLeft[Either[PdfError, Vector[PdfObject]]](Right(Vector.empty)) { case (result, (ref, page)) =>
        for
          found <- result
          original <- objects.get(ref).toRight(PdfError.InvalidPdf(s"Selected page object ${ref.number} is missing"))
          index <- mapping.get(ref).toRight(PdfError.InvalidPdf(s"Selected page object ${ref.number} was not mapped"))
        yield found :+ PdfObject(index, rewrite(page.removed("Parent").updated("Parent", Ref(ObjectRef(2))), mapping), original.stream)
      }
      dependencyObjects <- dependencies.foldLeft[Either[PdfError, Vector[PdfObject]]](Right(Vector.empty)) { (result, ref) =>
        for
          found <- result
          original <- objects.get(ref).toRight(PdfError.InvalidPdf(s"Referenced PDF object ${ref.number} is missing"))
          index <- mapping.get(ref).toRight(PdfError.InvalidPdf(s"Referenced PDF object ${ref.number} was not mapped"))
        yield found :+ original.copy(index = index, value = rewrite(original.value, mapping))
      }
      kids <- selectedRefs.foldLeft[Either[PdfError, Vector[PdfValue]]](Right(Vector.empty)) { (result, ref) =>
        for
          found <- result
          mapped <- mapping.get(ref).toRight(PdfError.InvalidPdf(s"Selected page object ${ref.number} was not mapped"))
        yield found :+ Ref(mapped)
      }
      rewrittenCatalog = rewrite(catalog.removed("Pages"), mapping) match
        case value: Dict => value
        case _ => catalog.removed("Pages")
      catalogObject = PdfObject(ObjectRef(1), rewrittenCatalog.updated("Pages", Ref(ObjectRef(2))))
      pagesObject = PdfObject(ObjectRef(2), dict(
        "Type" -> Name("Pages"),
        "Kids" -> Array(kids),
        "Count" -> Number(BigDecimal(selected.length))))
    yield PdfDocument(document.version, Vector(catalogObject, pagesObject) ++ selectedObjects ++ dependencyObjects,
      dict("Root" -> Ref(ObjectRef(1)), "Size" -> Number(BigDecimal(mapping.size + 3))))

  private def dependencyClosure(
    objects: Map[ObjectRef, PdfObject],
    catalog: Dict,
    selected: Vector[(ObjectRef, Dict)]
  ): Either[PdfError, Vector[ObjectRef]] =
    val retained = scala.collection.mutable.LinkedHashSet.empty[ObjectRef]
    def visit(ref: ObjectRef, replacement: Option[PdfValue] = None): Either[PdfError, Unit] =
      if retained(ref) then Right(())
      else objects.get(ref).toRight(PdfError.InvalidPdf(s"Referenced PDF object ${ref.number} is missing")).flatMap { obj =>
        retained += ref
        val source = replacement.getOrElse(obj.value)
        refs(source).foldLeft[Either[PdfError, Unit]](Right(()))((result, child) => result.flatMap(_ => visit(child)))
      }
    for
      _ <- selected.foldLeft[Either[PdfError, Unit]](Right(())) { case (result, (ref, page)) =>
        result.flatMap(_ => visit(ref, Some(page.removed("Parent"))))
      }
      _ <- refs(catalog.removed("Pages")).foldLeft[Either[PdfError, Unit]](Right(()))((result, ref) => result.flatMap(_ => visit(ref)))
    yield retained.toVector

  private def rewrite(value: PdfValue, mapping: Map[ObjectRef, ObjectRef]): PdfValue = value match
    case Ref(ref) => Ref(mapping.getOrElse(ref, ref))
    case Array(values) => Array(values.map(rewrite(_, mapping)))
    case Dict(fields) => Dict(fields.map(field => field.copy(value = rewrite(field.value, mapping))))
    case other => other

  private def refs(value: PdfValue): Vector[ObjectRef] = value match
    case Ref(value) => Vector(value)
    case Array(values) => values.flatMap(refs)
    case Dict(fields) => fields.flatMap(field => refs(field.value))
    case _ => Vector.empty

object PdfWriter:
  import PdfValue.*

  def write(document: PdfDocument): Vector[Byte] < (Abort[PdfError] & Sync) =
    if document.encrypted then Abort.fail(PdfError.InvalidPdf("Encrypted PDFs cannot be written"))
    else Abort.catching[Throwable](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))) {
      Sync.defer(render(document).toVector)
    }

  private def render(document: PdfDocument): scala.Array[Byte] =
    val out = new ByteArrayOutputStream()
    def text(value: String): Unit = out.write(value.getBytes(ISO_8859_1))
    text(s"%PDF-${document.version}\n%\u00e2\u00e3\u00cf\u00d3\n")
    val ordered = document.objects.sortBy(obj => (obj.index.number, obj.index.generation))
    val offsets = scala.collection.mutable.Map.empty[Long, (Int, Int)]
    ordered.foreach { obj =>
      if obj.index.number <= 0 || obj.index.number > Int.MaxValue || obj.index.generation < 0 || obj.index.generation > 65535 then
        throw IllegalArgumentException("PDF object number or generation is outside writer bounds")
      offsets(obj.index.number) = (out.size(), obj.index.generation)
      text(s"${obj.index.number} ${obj.index.generation} obj\n")
      obj.stream match
        case None => text(encode(obj.value)); text("\nendobj\n")
        case Some(bytes) =>
          val dict = obj.value match
            case value: Dict => value.updated("Length", Number(BigDecimal(bytes.length)))
            case _ => throw IllegalArgumentException("PDF stream object must contain a dictionary")
          text(encode(dict)); text("\nstream\n"); out.write(bytes.toArray); text("\nendstream\nendobj\n")
    }
    val maximum = ordered.map(_.index.number).maxOption.getOrElse(0L).toInt
    val xref = out.size()
    text(s"xref\n0 ${maximum + 1}\n0000000000 65535 f \n")
    (1 to maximum).foreach { number =>
      offsets.get(number.toLong) match
        case Some((offset, generation)) => text(f"$offset%010d $generation%05d n \n")
        case None => text("0000000000 00000 f \n")
    }
    val trailer = document.trailer.updated("Size", Number(BigDecimal(maximum + 1)))
    text(s"trailer\n${encode(trailer)}\nstartxref\n$xref\n%%EOF\n")
    out.toByteArray

  private def encode(value: PdfValue): String = value match
    case Null => "null"
    case Bool(value) => value.toString
    case Number(value) => value.bigDecimal.stripTrailingZeros.toPlainString
    case Name(value) => "/" + value.flatMap { char => if char.isWhitespace || "#/%()<>[]{}".contains(char) then f"#${char.toInt}%02X" else char.toString }
    case Literal(bytes) => "(" + bytes.map { byte =>
      (byte & 0xff).toChar match
        case '(' => "\\("
        case ')' => "\\)"
        case '\\' => "\\\\"
        case '\n' => "\\n"
        case '\r' => "\\r"
        case '\t' => "\\t"
        case char if char < ' ' || char > '~' => f"\\${byte & 0xff}%03o"
        case char => char.toString
    }.mkString + ")"
    case Hex(bytes) => "<" + bytes.map(byte => f"${byte & 0xff}%02X").mkString + ">"
    case Array(values) => values.map(encode).mkString("[", " ", "]")
    case Dict(fields) => fields.map(field => s"/${field.name} ${encode(field.value)}").mkString("<< ", " ", " >>")
    case Ref(value) => s"${value.number} ${value.generation} R"
