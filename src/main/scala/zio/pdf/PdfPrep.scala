/*
 * Serializable filing-prep programs.
 *
 * A [[PdfPrep.Program]] is a data-only operation list with a derived
 * `Schema`. Persist it as JSON (`toJson` / `fromJson`) or as a
 * `DynamicValue`, then apply it later to any unencrypted PDF.
 */

package zio.pdf

import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import _root_.scodec.bits.BitVector
import zio.{Chunk, ZIO}
import zio.blocks.chunk.Chunk as BlocksChunk
import zio.blocks.schema.{DynamicValue, Schema}
import zio.blocks.schema.json.{JsonCodec, JsonFormat}
import zio.pdf.content.{ContentOps, ContentToken}
import zio.stream.ZStream

object PdfPrep {

  sealed abstract class Error(message: String) extends Exception(message)

  case object EmptyProgram extends Error("prep program has no operations")

  final case class DecodeFailed(detail: String) extends Error(detail)

  final case class ApplyFailed(detail: String) extends Error(detail)

  final case class InvalidRange(fromPage: Int, toPage: Int, pageCount: Int)
      extends Error(s"page range $fromPage-$toPage is outside 1-$pageCount")

  case object NoPages extends Error("document has no pages")

  final case class UnknownFont(name: String) extends Error(s"embedded font '$name' is not in the program")

  final case class InvalidFont(detail: String) extends Error(detail)

  enum PageRange {
    case All
    case FromTo(fromPage: Int, toPage: Option[Int])
  }

  object PageRange {
    given Schema[PageRange] = Schema.derived[PageRange]
  }

  enum StandardFont {
    case Helvetica, HelveticaBold, HelveticaOblique, HelveticaBoldOblique
    case TimesRoman, TimesBold, TimesItalic, TimesBoldItalic
    case Courier, CourierBold, CourierOblique, CourierBoldOblique
  }

  object StandardFont {
    given Schema[StandardFont] = Schema.derived[StandardFont]
  }

  enum Placement {
    case Center, TopLeft, TopCenter, TopRight, MiddleLeft, MiddleRight, BottomLeft, BottomCenter, BottomRight
  }

  object Placement {
    given Schema[Placement] = Schema.derived[Placement]
  }

  enum Color {
    case Gray(value: Double)
    case Rgb(red: Double, green: Double, blue: Double)
  }

  object Color {
    given Schema[Color] = Schema.derived[Color]
  }

  enum FontRef {
    case Standard(font: StandardFont)
    case Embedded(name: String)
  }

  object FontRef {
    given Schema[FontRef] = Schema.derived[FontRef]
  }

  enum DateSource {
    case Fixed(isoDate: String)
    case Today
  }

  object DateSource {
    given Schema[DateSource] = Schema.derived[DateSource]
  }

  enum PageLabelStyle {
    case Decimal
    case UpperRoman
    case LowerRoman
    case UpperLetters
    case LowerLetters
  }

  object PageLabelStyle {
    given Schema[PageLabelStyle] = Schema.derived[PageLabelStyle]
  }

  final case class TextStyle(
    font: FontRef = FontRef.Standard(StandardFont.Helvetica),
    color: Color = Color.Gray(0.2),
    opacity: Double = 1.0,
    placement: Placement = Placement.BottomCenter,
    fontSize: Double = 10.0,
    rotationDegrees: Double = 0.0
  )

  object TextStyle {
    given Schema[TextStyle] = Schema.derived[TextStyle]
  }

  final case class WatermarkText(
    text: String,
    font: FontRef = FontRef.Standard(StandardFont.Helvetica),
    color: Color = Color.Gray(0.72),
    opacity: Double = 1.0,
    placement: Placement = Placement.Center,
    diagonal: Boolean = true,
    rotationDegrees: Double = 0.0,
    fontSize: Option[Double] = None,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  )

  object WatermarkText {
    given Schema[WatermarkText] = Schema.derived[WatermarkText]
  }

  final case class WatermarkImage(
    width: Int,
    height: Int,
    pixels: BlocksChunk[Byte],
    colorSpace: String = "DeviceGray",
    opacity: Double = 0.35,
    placement: Placement = Placement.Center,
    scale: Double = 0.25,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  )

  object WatermarkImage {
    given Schema[WatermarkImage] = Schema.derived[WatermarkImage]
  }

  final case class StampDate(
    source: DateSource = DateSource.Today,
    pattern: String = "yyyy-MM-dd",
    style: TextStyle = TextStyle(placement = Placement.TopRight, fontSize = 9.0),
    range: PageRange = PageRange.All
  )

  object StampDate {
    given Schema[StampDate] = Schema.derived[StampDate]
  }

  final case class BatesLabel(
    prefix: String = "",
    start: Long = 1L,
    width: Int = 6,
    suffix: String = "",
    style: TextStyle = TextStyle(placement = Placement.BottomRight, fontSize = 9.0),
    range: PageRange = PageRange.All
  )

  object BatesLabel {
    given Schema[BatesLabel] = Schema.derived[BatesLabel]
  }

  final case class PageLabels(
    style: PageLabelStyle = PageLabelStyle.Decimal,
    prefix: String = "",
    start: Int = 1,
    fromPage: Int = 1
  )

  object PageLabels {
    given Schema[PageLabels] = Schema.derived[PageLabels]
  }

  final case class RedactRect(
    page: Int,
    x: Double,
    y: Double,
    width: Double,
    height: Double
  )

  object RedactRect {
    given Schema[RedactRect] = Schema.derived[RedactRect]
  }

  /**
   * Overlay filled rects. `stripShowText` blanks every show-string on pages that
   * have a box, including earlier program stamps on those pages. Apply stamps
   * after redaction if they must remain extractable.
   */
  final case class Redact(
    boxes: List[RedactRect],
    fill: Color = Color.Rgb(1.0, 1.0, 1.0),
    stripShowText: Boolean = true
  )

  object Redact {
    given Schema[Redact] = Schema.derived[Redact]
  }

  final case class EmbedFont(
    name: String,
    baseFont: String,
    bytes: BlocksChunk[Byte]
  )

  object EmbedFont {
    given Schema[EmbedFont] = Schema.derived[EmbedFont]
  }

  final case class FieldValue(
    qualifiedName: String,
    value: String
  )

  object FieldValue {
    given Schema[FieldValue] = Schema.derived[FieldValue]
  }

  enum ThumbnailScope {
    case FirstPageOnly, AllPages, Off
  }

  object ThumbnailScope {
    given Schema[ThumbnailScope] = Schema.derived[ThumbnailScope]
  }

  enum Op {
    case Watermark(text: WatermarkText)
    case WatermarkImageStamp(image: WatermarkImage)
    case DateStamp(stamp: StampDate)
    case Bates(label: BatesLabel)
    case SetPageLabels(labels: PageLabels)
    case RedactBoxes(redact: Redact)
    case EmbedTrueType(font: EmbedFont)
    case SetFieldValues(values: List[FieldValue])
    case FlattenForms
    case AttachThumbnail(scope: ThumbnailScope = ThumbnailScope.FirstPageOnly)
    case Extract(fromPage: Int, toPage: Int)
    case Rotate(degrees: Int, fromPage: Int, toPage: Int)
    case Linearize
  }

  object Op {
    given Schema[Op] = Schema.derived[Op]
  }

  final case class Program(operations: List[Op] = Nil) {
    def andThen(that: Program): Program = Program(operations ++ that.operations)
    infix def >>>(that: Program): Program = andThen(that)
    def size: Int = operations.size
    def isEmpty: Boolean = operations.isEmpty
  }

  object Program {
    val empty: Program = Program()
    def of(ops: Op*): Program = Program(ops.toList)
    given Schema[Program] = Schema.derived[Program]
  }

  final case class Profile(operations: List[String], writesContent: Boolean, writesCatalog: Boolean)

  def profile(program: Program): Profile = {
    val names = program.operations.map {
      case Op.Watermark(_)            => "watermark-text"
      case Op.WatermarkImageStamp(_)  => "watermark-image"
      case Op.DateStamp(_)            => "date-stamp"
      case Op.Bates(_)                => "bates"
      case Op.SetPageLabels(_)        => "page-labels"
      case Op.RedactBoxes(_)          => "redact"
      case Op.EmbedTrueType(_)        => "embed-font"
      case Op.SetFieldValues(_)       => "set-field-values"
      case Op.FlattenForms            => "flatten"
      case Op.AttachThumbnail(_)      => "attach-thumbnail"
      case Op.Extract(_, _)           => "extract"
      case Op.Rotate(_, _, _)         => "rotate"
      case Op.Linearize               => "linearize"
    }
    val writesContent = program.operations.exists {
      case Op.Watermark(_) | Op.WatermarkImageStamp(_) | Op.DateStamp(_) | Op.Bates(_) |
          Op.RedactBoxes(_) | Op.EmbedTrueType(_) | Op.FlattenForms | Op.AttachThumbnail(_) =>
        true
      case _ => false
    }
    val writesCatalog = program.operations.exists {
      case Op.SetPageLabels(_) | Op.FlattenForms | Op.Extract(_, _) | Op.Linearize => true
      case _                                                                      => false
    }
    Profile(names, writesContent, writesCatalog)
  }

  private val codec: JsonCodec[Program] =
    summon[Schema[Program]].derive(JsonFormat.deriver)

  def toJson(program: Program): String =
    codec.encodeToString(program)

  def fromJson(json: String): Either[Error, Program] =
    codec.decode(json) match {
      case Right(program) => Right(program)
      case Left(error)    => Left(DecodeFailed(error.toString))
    }

  def toDynamicValue(program: Program): DynamicValue =
    summon[Schema[Program]].toDynamicValue(program)

  def fromDynamicValue(value: DynamicValue): Either[Error, Program] =
    summon[Schema[Program]].fromDynamicValue(value) match {
      case Right(program) => Right(program)
      case Left(error)    => Left(DecodeFailed(error.toString))
    }

  def apply(
    bytes: Chunk[Byte],
    program: Program,
    opts: PdfEngine.Options = PdfEngine.Options.default,
    today: LocalDate = LocalDate.now()
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    if bytes.size.toLong > opts.maxMaterializedDocumentBytes.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, bytes.size.toLong))
    else if program.isEmpty then ZIO.fail(EmptyProgram)
    else
      program.operations.foldLeft[ZIO[PdfEngine, Throwable, Chunk[Byte]]](ZIO.succeed(bytes)) { (acc, op) =>
        acc.flatMap(current => applyOp(current, op, opts, today))
      }

  private def applyOp(
    bytes: Chunk[Byte],
    op: Op,
    opts: PdfEngine.Options,
    today: LocalDate
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    op match {
      case Op.Watermark(text) =>
        text.font match {
          case FontRef.Embedded(_) =>
            applyStyledText(bytes, text.text, textStyleOf(text), rangeOf(text), opts)
          case FontRef.Standard(_) =>
            PdfEngine.watermark(bytes, toWatermarkText(text), opts)
        }
      case Op.WatermarkImageStamp(image) =>
        PdfEngine.watermark(bytes, toWatermarkImage(image), opts)
      case Op.DateStamp(stamp) =>
        applyDate(bytes, stamp, opts, today)
      case Op.Bates(label) =>
        applyBates(bytes, label, opts)
      case Op.SetPageLabels(labels) =>
        applyPageLabels(bytes, labels, opts)
      case Op.RedactBoxes(redact) =>
        applyRedact(bytes, redact, opts)
      case Op.EmbedTrueType(font) =>
        applyEmbedFont(bytes, font, opts)
      case Op.SetFieldValues(values) =>
        applyFieldValues(bytes, values, opts)
      case Op.FlattenForms =>
        PdfEngine.flattenForms(bytes, opts)
      case Op.AttachThumbnail(scope) =>
        PdfEngine.withThumbnailsBytes(bytes, toThumbnailOptions(scope))
      case Op.Extract(fromPage, toPage) =>
        PdfEngine.extractPages(bytes, fromPage, toPage, opts)
      case Op.Rotate(degrees, fromPage, toPage) =>
        PdfEngine.rotatePages(bytes, degrees, fromPage, toPage, opts)
      case Op.Linearize =>
        PdfEngine.linearize(bytes, opts)
    }

  private def toWatermarkText(text: WatermarkText): PdfWatermark.Text =
    PdfWatermark.Text(
      text = text.text,
      font = toStandardFont(text.font),
      color = toColor(text.color),
      opacity = text.opacity,
      placement = toPlacement(text.placement),
      diagonal = text.diagonal,
      rotationDegrees = text.rotationDegrees,
      fontSize = text.fontSize,
      fromPage = text.fromPage,
      toPage = text.toPage
    )

  private def toWatermarkImage(image: WatermarkImage): PdfWatermark.Stamp =
    image.colorSpace.toLowerCase match {
      case "devicergb" | "rgb" =>
        PdfWatermark.RgbImage(
          width = image.width,
          height = image.height,
          pixels = Chunk.fromIterable(image.pixels),
          opacity = image.opacity,
          placement = toPlacement(image.placement),
          scale = image.scale,
          fromPage = image.fromPage,
          toPage = image.toPage
        )
      case "jpeg" | "dct" =>
        PdfWatermark.JpegImage(
          width = image.width,
          height = image.height,
          jpeg = Chunk.fromIterable(image.pixels),
          opacity = image.opacity,
          placement = toPlacement(image.placement),
          scale = image.scale,
          fromPage = image.fromPage,
          toPage = image.toPage
        )
      case _ =>
        PdfWatermark.GrayImage(
          width = image.width,
          height = image.height,
          pixels = Chunk.fromIterable(image.pixels),
          opacity = image.opacity,
          placement = toPlacement(image.placement),
          scale = image.scale,
          fromPage = image.fromPage,
          toPage = image.toPage
        )
    }

  private def toStandardFont(font: FontRef): PdfWatermark.StandardFont =
    font match {
      case FontRef.Embedded(_) => PdfWatermark.StandardFont.Helvetica
      case FontRef.Standard(name) =>
        name match {
          case StandardFont.Helvetica            => PdfWatermark.StandardFont.Helvetica
          case StandardFont.HelveticaBold        => PdfWatermark.StandardFont.HelveticaBold
          case StandardFont.HelveticaOblique     => PdfWatermark.StandardFont.HelveticaOblique
          case StandardFont.HelveticaBoldOblique => PdfWatermark.StandardFont.HelveticaBoldOblique
          case StandardFont.TimesRoman           => PdfWatermark.StandardFont.TimesRoman
          case StandardFont.TimesBold            => PdfWatermark.StandardFont.TimesBold
          case StandardFont.TimesItalic          => PdfWatermark.StandardFont.TimesItalic
          case StandardFont.TimesBoldItalic      => PdfWatermark.StandardFont.TimesBoldItalic
          case StandardFont.Courier              => PdfWatermark.StandardFont.Courier
          case StandardFont.CourierBold          => PdfWatermark.StandardFont.CourierBold
          case StandardFont.CourierOblique       => PdfWatermark.StandardFont.CourierOblique
          case StandardFont.CourierBoldOblique   => PdfWatermark.StandardFont.CourierBoldOblique
        }
    }

  private def toPlacement(placement: Placement): PdfWatermark.Placement =
    placement match {
      case Placement.Center       => PdfWatermark.Placement.Center
      case Placement.TopLeft      => PdfWatermark.Placement.TopLeft
      case Placement.TopCenter    => PdfWatermark.Placement.TopCenter
      case Placement.TopRight     => PdfWatermark.Placement.TopRight
      case Placement.MiddleLeft   => PdfWatermark.Placement.MiddleLeft
      case Placement.MiddleRight  => PdfWatermark.Placement.MiddleRight
      case Placement.BottomLeft   => PdfWatermark.Placement.BottomLeft
      case Placement.BottomCenter => PdfWatermark.Placement.BottomCenter
      case Placement.BottomRight  => PdfWatermark.Placement.BottomRight
    }

  private def toColor(color: Color): PdfWatermark.Color =
    color match {
      case Color.Gray(value)           => PdfWatermark.Color.Gray(value)
      case Color.Rgb(red, green, blue) => PdfWatermark.Color.Rgb(red, green, blue)
    }

  private def applyDate(
    bytes: Chunk[Byte],
    stamp: StampDate,
    opts: PdfEngine.Options,
    today: LocalDate
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    resolveDate(stamp, today) match {
      case Left(error) => ZIO.fail(error)
      case Right(date) =>
        val formatted = formatDate(date, stamp.pattern)
        applyStyledText(bytes, formatted, stamp.style, stamp.range, opts)
    }

  private def resolveDate(stamp: StampDate, today: LocalDate): Either[Error, LocalDate] =
    stamp.source match {
      case DateSource.Today => Right(today)
      case DateSource.Fixed(isoDate) =>
        try Right(LocalDate.parse(isoDate))
        catch case _: Exception => Left(ApplyFailed(s"invalid ISO date: $isoDate"))
    }

  private def formatDate(date: LocalDate, pattern: String): String =
    try DateTimeFormatter.ofPattern(pattern).format(date)
    catch case _: IllegalArgumentException => date.toString

  private def applyBates(
    bytes: Chunk[Byte],
    label: BatesLabel,
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      val pages = TextExtract.orderedPageObjectNumbers(decoded)
      ZIO.fromEither(resolveRange(pages, label.range)).flatMap { targets =>
        val pad = math.max(1, label.width)
        val labels = targets.zipWithIndex.map { (pageNumber, index) =>
          val n = label.start + index
          pageNumber -> s"${label.prefix}${n.toString.reverse.padTo(pad, '0').reverse}${label.suffix}"
        }.toMap
        ZIO.fromEither(stampPerPage(decoded, labels, label.style)).flatMap(writeParts)
      }
    }

  private def applyPageLabels(
    bytes: Chunk[Byte],
    labels: PageLabels,
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(pageLabelsParts(decoded, labels)).flatMap(writeParts)
    }

  private def applyRedact(
    bytes: Chunk[Byte],
    redact: Redact,
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(redactParts(decoded, redact)).flatMap(writeParts)
    }

  private def applyEmbedFont(
    bytes: Chunk[Byte],
    font: EmbedFont,
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(embedFontParts(decoded, font)).flatMap(writeParts)
    }

  private def applyFieldValues(
    bytes: Chunk[Byte],
    values: List[FieldValue],
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    PdfEngine.setFieldValues(bytes, values.map(v => v.qualifiedName -> v.value).toMap, opts)

  private def toThumbnailOptions(scope: ThumbnailScope): PdfThumbnail.Options =
    scope match {
      case ThumbnailScope.FirstPageOnly => PdfThumbnail.Options(scope = PdfThumbnail.Scope.FirstPageOnly)
      case ThumbnailScope.AllPages      => PdfThumbnail.Options(scope = PdfThumbnail.Scope.AllPages)
      case ThumbnailScope.Off           => PdfThumbnail.Options(scope = PdfThumbnail.Scope.Off)
    }

  private def applyStyledText(
    bytes: Chunk[Byte],
    text: String,
    style: TextStyle,
    range: PageRange,
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      val pages = TextExtract.orderedPageObjectNumbers(decoded)
      ZIO.fromEither(resolveRange(pages, range)).flatMap { targets =>
        val labels = targets.map(_ -> text).toMap
        ZIO.fromEither(stampPerPage(decoded, labels, style)).flatMap(writeParts)
      }
    }

  private def textStyleOf(text: WatermarkText): TextStyle =
    TextStyle(
      font = text.font,
      color = text.color,
      opacity = text.opacity,
      placement = text.placement,
      fontSize = text.fontSize.getOrElse(18.0),
      rotationDegrees = if text.rotationDegrees != 0.0 then text.rotationDegrees else if text.diagonal then 45.0 else 0.0
    )

  private def rangeOf(text: WatermarkText): PageRange =
    PageRange.FromTo(text.fromPage, text.toPage)

  private def resolveRange(pages: List[Long], range: PageRange): Either[Error, List[Long]] =
    if pages.isEmpty then Left(NoPages)
    else
      range match {
        case PageRange.All => Right(pages)
        case PageRange.FromTo(from, to) =>
          val last = to.getOrElse(pages.size)
          if from < 1 || last < from || last > pages.size then Left(InvalidRange(from, last, pages.size))
          else Right(pages.slice(from - 1, last))
      }

  private def stampPerPage(
    decoded: Chunk[Decoded],
    labels: Map[Long, String],
    style: TextStyle
  ): Either[Error, Chunk[Part[Trailer]]] = {
    val objects = objectMap(decoded)
    val start   = nextObjectNumber(objects, decoded)
    val fresh   = Fresh(start)
    val extras  = Chunk.newBuilder[Part[Trailer]]
    val updates = scala.collection.mutable.Map.empty[Long, Prim.Dict]
    labels.foreach { (pageNumber, text) =>
      objects.get(pageNumber).flatMap(obj => dictAt(obj.data)).foreach { page =>
        val box    = mediaBoxOf(pageNumber, objects).getOrElse((0.0, 0.0, 612.0, 792.0))
        val stream = fresh.next()
        extras += Part.Obj(
          IndirectObj.stream(
            stream,
            Prim.dict(),
            BitVector(overlayText(text, box, style))
          )
        )
        var stamped = appendContents(page, Prim.Ref(stream, 0))
        stamped = addStampFont(stamped, objects, style.font)
        if style.opacity < 1.0 then stamped = addExtGState(stamped, objects, style.opacity, "GsWm")
        updates.update(pageNumber, stamped)
      }
    }
    Right(rewrite(decoded, updates.toMap, extras.result(), fresh.peek))
  }

  private def pageLabelsParts(decoded: Chunk[Decoded], labels: PageLabels): Either[Error, Chunk[Part[Trailer]]] = {
    val objects = objectMap(decoded)
    val catalog = objects.values.find(obj => dictAt(obj.data).exists(isCatalog))
    catalog match {
      case None => Left(ApplyFailed("document has no catalog"))
      case Some(root) =>
        val start   = nextObjectNumber(objects, decoded)
        val tree    = start
        val styleName = labels.style match {
          case PageLabelStyle.Decimal      => "D"
          case PageLabelStyle.UpperRoman   => "R"
          case PageLabelStyle.LowerRoman   => "r"
          case PageLabelStyle.UpperLetters => "A"
          case PageLabelStyle.LowerLetters => "a"
        }
        val entry = Prim.dict(
          "S" -> Prim.Name(styleName),
          "St" -> Prim.Number(math.max(1, labels.start).toDouble),
          "P" -> Prim.Str(_root_.scodec.bits.ByteVector(labels.prefix.getBytes(StandardCharsets.ISO_8859_1)))
        )
        val nums = Prim.Array(Prim.Number((math.max(1, labels.fromPage) - 1).toDouble), entry)
        val extra = Part.Obj(IndirectObj.nostream(tree, Prim.dict("Nums" -> nums)))
        val updated = dictAt(root.data).map { dict =>
          Prim.Dict(dict.data.updated("PageLabels", Prim.Ref(tree, 0)))
        }.get
        Right(rewrite(decoded, Map(root.index.number -> updated), Chunk(extra), tree + 1L))
    }
  }

  private def redactParts(decoded: Chunk[Decoded], redact: Redact): Either[Error, Chunk[Part[Trailer]]] = {
    val objects = objectMap(decoded)
    val streams = streamMap(decoded)
    val pages   = TextExtract.orderedPageObjectNumbers(decoded)
    val start   = nextObjectNumber(objects, decoded)
    val fresh   = Fresh(start)
    val extras  = Chunk.newBuilder[Part[Trailer]]
    val updates = scala.collection.mutable.Map.empty[Long, Prim.Dict]
    val streamUpdates = scala.collection.mutable.Map.empty[Long, BitVector]
    val byPage = redact.boxes.groupBy(_.page)
    byPage.foreach { (pageIndex, boxes) =>
      if pageIndex >= 1 && pageIndex <= pages.size then
        val pageNumber = pages(pageIndex - 1)
        objects.get(pageNumber).flatMap(obj => dictAt(obj.data)).foreach { page =>
          val stream = fresh.next()
          extras += Part.Obj(
            IndirectObj.stream(stream, Prim.dict(), BitVector(redactOverlay(boxes, redact.fill)))
          )
          updates.update(pageNumber, appendContents(page, Prim.Ref(stream, 0)))
          if redact.stripShowText then
            contentRefs(page).foreach { number =>
              streams.get(number).foreach { raw =>
                streamUpdates.update(number, blankShowText(raw))
              }
            }
        }
    }
    Right(rewrite(decoded, updates.toMap, extras.result(), fresh.peek, streamUpdates.toMap))
  }

  private def embedFontParts(decoded: Chunk[Decoded], font: EmbedFont): Either[Error, Chunk[Part[Trailer]]] = {
    val objects = objectMap(decoded)
    val pages   = TextExtract.orderedPageObjectNumbers(decoded)
    if pages.isEmpty then Left(NoPages)
    else if font.bytes.isEmpty then Left(InvalidFont("embedded font bytes are empty"))
    else
      TrueTypeFont.parse(font.bytes.toArray).flatMap { metrics =>
        FlateEncode(BitVector(font.bytes.toArray)) match {
          case _root_.scodec.Attempt.Failure(cause) =>
            Left(InvalidFont(s"font FlateEncode: ${cause.messageWithContext}"))
          case _root_.scodec.Attempt.Successful(compressed) =>
            val start   = nextObjectNumber(objects, decoded)
            val fresh   = Fresh(start)
            val fileId  = fresh.next()
            val descId  = fresh.next()
            val fontId  = fresh.next()
            val extras  = Chunk.newBuilder[Part[Trailer]]
            extras += Part.Obj(
              IndirectObj.stream(
                fileId,
                Prim.dict(
                  "Length1" -> Prim.Number(font.bytes.size.toDouble),
                  "Filter"  -> Prim.Name("FlateDecode")
                ),
                compressed
              )
            )
            extras += Part.Obj(
              IndirectObj.nostream(
                descId,
                Prim.dict(
                  "Type"        -> Prim.Name("FontDescriptor"),
                  "FontName"    -> Prim.Name(font.baseFont),
                  "Flags"       -> Prim.Number(32),
                  "ItalicAngle" -> Prim.Number(0),
                  "Ascent"      -> Prim.Number(metrics.ascent),
                  "Descent"     -> Prim.Number(metrics.descent),
                  "CapHeight"   -> Prim.Number(metrics.capHeight),
                  "StemV"       -> Prim.Number(80),
                  "FontBBox"    -> Prim.Array.nums(metrics.xMin, metrics.yMin, metrics.xMax, metrics.yMax),
                  "FontFile2"   -> Prim.Ref(fileId, 0)
                )
              )
            )
            extras += Part.Obj(
              IndirectObj.nostream(
                fontId,
                Prim.dict(
                  "Type"           -> Prim.Name("Font"),
                  "Subtype"        -> Prim.Name("TrueType"),
                  "BaseFont"       -> Prim.Name(font.baseFont),
                  "FirstChar"      -> Prim.Number(32),
                  "LastChar"       -> Prim.Number(126),
                  "Widths"         -> Prim.Array(metrics.widths.map(w => Prim.Number(w))*),
                  "Encoding"       -> Prim.Name("WinAnsiEncoding"),
                  "FontDescriptor" -> Prim.Ref(descId, 0)
                )
              )
            )
            val updates = pages.flatMap { pageNumber =>
              objects.get(pageNumber).flatMap(obj => dictAt(obj.data)).map { page =>
                pageNumber -> addNamedFont(page, objects, font.name, Prim.Ref(fontId, 0))
              }
            }.toMap
            Right(rewrite(decoded, updates, extras.result(), fresh.peek))
        }
      }
  }

  private def overlayText(text: String, box: (Double, Double, Double, Double), style: TextStyle): Array[Byte] = {
    val (x1, y1, x2, y2) = normalize(box)
    val width            = math.max(x2 - x1, 1.0)
    val height           = math.max(y2 - y1, 1.0)
    val size             = if style.fontSize > 0 then style.fontSize else math.max(18.0, math.min(width, height) * 0.12)
    val estimated        = math.max(size, text.length.toDouble * size * 0.5)
    val angle            = if style.rotationDegrees != 0.0 then math.toRadians(style.rotationDegrees) else 0.0
    val cos              = math.cos(angle)
    val sin              = math.sin(angle)
    val (ax, ay)         = textAnchor(style.placement, box, estimated, size)
    val tx               = ax - estimated / 2.0 * cos
    val ty               = ay - estimated / 2.0 * sin
    val gs               = if style.opacity < 1.0 then "/GsWm gs\n" else ""
    val color            = style.color match {
      case Color.Gray(value)           => s"${pdfNum(value)} g"
      case Color.Rgb(red, green, blue) => s"${pdfNum(red)} ${pdfNum(green)} ${pdfNum(blue)} rg"
    }
    s"""q
       |${gs}BT
       |/${fontResourceName(style.font)} ${pdfNum(size)} Tf
       |$color
       |${pdfNum(cos)} ${pdfNum(sin)} ${pdfNum(-sin)} ${pdfNum(cos)} ${pdfNum(tx)} ${pdfNum(ty)} Tm
       |${pdfLiteral(text)} Tj
       |ET
       |Q
       |""".stripMargin.getBytes(StandardCharsets.ISO_8859_1)
  }

  private def fontResourceName(font: FontRef): String =
    font match {
      case FontRef.Embedded(name) => name
      case FontRef.Standard(_)    => toStandardFont(font).resourceName
    }

  private def addStampFont(page: Prim.Dict, objects: Map[Long, Obj], font: FontRef): Prim.Dict =
    font match {
      case FontRef.Standard(_)    => addStandardFont(page, objects, toStandardFont(font))
      case FontRef.Embedded(name) =>
        resolvedDict(page, "Resources", objects)
          .flatMap(resources => resolvedDict(resources, "Font", objects))
          .flatMap(_.data.get(name)) match {
            case Some(_) => page
            case None    => addStandardFont(page, objects, PdfWatermark.StandardFont.Helvetica)
          }
    }

  private def textAnchor(
    placement: Placement,
    box: (Double, Double, Double, Double),
    textWidth: Double,
    textHeight: Double
  ): (Double, Double) = {
    val (x1, y1, x2, y2) = normalize(box)
    val margin             = math.min(x2 - x1, y2 - y1) * 0.08
    placement match {
      case Placement.Center       => (x1 + (x2 - x1) / 2.0, y1 + (y2 - y1) / 2.0)
      case Placement.TopLeft      => (x1 + margin + textWidth / 2.0, y2 - margin - textHeight / 2.0)
      case Placement.TopCenter    => (x1 + (x2 - x1) / 2.0, y2 - margin - textHeight / 2.0)
      case Placement.TopRight     => (x2 - margin - textWidth / 2.0, y2 - margin - textHeight / 2.0)
      case Placement.MiddleLeft   => (x1 + margin + textWidth / 2.0, y1 + (y2 - y1) / 2.0)
      case Placement.MiddleRight  => (x2 - margin - textWidth / 2.0, y1 + (y2 - y1) / 2.0)
      case Placement.BottomLeft   => (x1 + margin + textWidth / 2.0, y1 + margin + textHeight / 2.0)
      case Placement.BottomCenter => (x1 + (x2 - x1) / 2.0, y1 + margin + textHeight / 2.0)
      case Placement.BottomRight  => (x2 - margin - textWidth / 2.0, y1 + margin + textHeight / 2.0)
    }
  }

  private def redactOverlay(boxes: List[RedactRect], fill: Color): Array[Byte] = {
    val color = fill match {
      case Color.Gray(value)           => s"${pdfNum(value)} g"
      case Color.Rgb(red, green, blue) => s"${pdfNum(red)} ${pdfNum(green)} ${pdfNum(blue)} rg"
    }
    val body = boxes.map { box =>
      s"${pdfNum(box.x)} ${pdfNum(box.y)} ${pdfNum(box.width)} ${pdfNum(box.height)} re f"
    }.mkString("\n")
    s"q\n$color\n$body\nQ\n".getBytes(StandardCharsets.ISO_8859_1)
  }

  private def blankShowText(raw: BitVector): BitVector = {
    val tokens = ContentOps.tokenize(raw.toByteArray)
    val out    = new StringBuilder
    var i      = 0
    while i < tokens.length do
      tokens(i) match {
        case ContentToken.Literal(_) =>
          val next = tokens.lift(i + 1)
          if next.exists { case ContentToken.Op("Tj" | "'" | "\"") => true; case _ => false } then
            out.append("() ")
          else
            out.append(renderToken(tokens(i)))
            out.append(' ')
        case ContentToken.Hex(_) =>
          val next = tokens.lift(i + 1)
          if next.exists { case ContentToken.Op("Tj") => true; case _ => false } then out.append("() ")
          else
            out.append(renderToken(tokens(i)))
            out.append(' ')
        case ContentToken.Array(_) =>
          val next = tokens.lift(i + 1)
          if next.exists { case ContentToken.Op("TJ") => true; case _ => false } then out.append("[()] ")
          else
            out.append(renderToken(tokens(i)))
            out.append(' ')
        case other =>
          out.append(renderToken(other))
          out.append(' ')
      }
      i += 1
    BitVector(out.toString.getBytes(StandardCharsets.ISO_8859_1))
  }

  private def renderToken(token: ContentToken): String =
    token match {
      case ContentToken.Number(value) =>
        if value.isWhole then value.toLong.toString else value.bigDecimal.stripTrailingZeros.toPlainString
      case ContentToken.Name(value)    => s"/$value"
      case ContentToken.Literal(bytes) => pdfLiteral(new String(bytes.toArray, StandardCharsets.ISO_8859_1))
      case ContentToken.Hex(bytes)     => s"<${bytes.toHex}>"
      case ContentToken.Array(elems)   => elems.map(renderToken).mkString("[", " ", "]")
      case ContentToken.Dict(entries)  =>
        entries.map((k, v) => s"/$k ${renderToken(v)}").mkString("<<", " ", ">>")
      case ContentToken.Op(name)       => name
      case ContentToken.Null           => "null"
      case ContentToken.Bool(value)    => if value then "true" else "false"
    }

  private def contentRefs(page: Prim.Dict): List[Long] =
    page.data.get("Contents") match {
      case Some(Prim.Ref(number, _)) => List(number)
      case Some(Prim.Array(entries)) =>
        entries.toList.collect { case Prim.Ref(number, _) => number }
      case _ => Nil
    }

  private def decodeUnencrypted(
    bytes: Chunk[Byte],
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    if bytes.size.toLong > opts.maxMaterializedDocumentBytes.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, bytes.size.toLong))
    else
      PdfEngine.decode(bytes, opts).tap { decoded =>
        ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded))
      }

  private final class Fresh(start: Long) {
    private var current = start
    def next(): Long = {
      val number = current
      current += 1L
      number
    }
    def peek: Long = current
  }

  private def rewrite(
    decoded: Chunk[Decoded],
    pages: Map[Long, Prim.Dict],
    extra: Chunk[Part[Trailer]],
    nextNumber: Long,
    streams: Map[Long, BitVector] = Map.empty
  ): Chunk[Part[Trailer]] = {
    val rewritten = decoded.flatMap {
      case Decoded.Meta(_, trailer, _) =>
        trailer.toList.map { meta =>
          val sized =
            if nextNumber > meta.size.toLong then meta.copy(size = BigDecimal(nextNumber))
            else meta
          Part.Meta(sized)
        }
      case Decoded.DataObj(obj) =>
        Chunk(Part.Obj(IndirectObj(applyPageUpdate(obj, pages), None)))
      case Decoded.ContentObj(obj, rawStream, _) =>
        val stream  = streams.getOrElse(obj.index.number, rawStream)
        val updated = applyPageUpdate(obj, pages)
        if streams.contains(obj.index.number) then
          Chunk(Part.Obj(IndirectObj.stream(updated.index.number, IndirectObj.addLength(stream)(updated.data), stream)))
        else Chunk(Part.Obj(IndirectObj(updated, Some(stream))))
    }
    rewritten ++ extra
  }

  private def applyPageUpdate(obj: Obj, pages: Map[Long, Prim.Dict]): Obj =
    pages.get(obj.index.number) match {
      case Some(dict) => Obj(obj.index, dict)
      case None       => obj
    }

  private def appendContents(page: Prim.Dict, overlay: Prim.Ref): Prim.Dict =
    page.data.get("Contents") match {
      case Some(existing: Prim.Ref) =>
        Prim.Dict(page.data.updated("Contents", Prim.Array(existing, overlay)))
      case Some(Prim.Array(entries)) =>
        Prim.Dict(page.data.updated("Contents", Prim.Array((entries.iterator.toSeq :+ overlay)*)))
      case _ =>
        Prim.Dict(page.data.updated("Contents", overlay))
    }

  private def addStandardFont(page: Prim.Dict, objects: Map[Long, Obj], font: PdfWatermark.StandardFont): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val fonts     = resolvedDict(resources, "Font", objects).getOrElse(Prim.dict())
    val name      = font.resourceName
    if fonts.data.contains(name) then
      val updatedR = Prim.Dict(resources.data.updated("Font", fonts))
      Prim.Dict(page.data.updated("Resources", updatedR))
    else
      val entry = Prim.dict(
        "Type"     -> Prim.Name("Font"),
        "Subtype"  -> Prim.Name("Type1"),
        "BaseFont" -> Prim.Name(font.baseFont)
      )
      val updatedFonts = Prim.Dict(fonts.data.updated(name, entry))
      val updatedR     = Prim.Dict(resources.data.updated("Font", updatedFonts))
      Prim.Dict(page.data.updated("Resources", updatedR))
  }

  private def addNamedFont(page: Prim.Dict, objects: Map[Long, Obj], name: String, ref: Prim.Ref): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val fonts     = resolvedDict(resources, "Font", objects).getOrElse(Prim.dict())
    val updatedFonts = Prim.Dict(fonts.data.updated(name, ref))
    val updatedR     = Prim.Dict(resources.data.updated("Font", updatedFonts))
    Prim.Dict(page.data.updated("Resources", updatedR))
  }

  private def addExtGState(page: Prim.Dict, objects: Map[Long, Obj], opacity: Double, name: String): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val states    = resolvedDict(resources, "ExtGState", objects).getOrElse(Prim.dict())
    val entry = Prim.dict(
      "Type" -> Prim.Name("ExtGState"),
      "ca"   -> Prim.Number(opacity),
      "CA"   -> Prim.Number(opacity)
    )
    val updatedStates = Prim.Dict(states.data.updated(name, entry))
    val updatedR      = Prim.Dict(resources.data.updated("ExtGState", updatedStates))
    Prim.Dict(page.data.updated("Resources", updatedR))
  }

  private def resolvedDict(owner: Prim.Dict, key: String, objects: Map[Long, Obj]): Option[Prim.Dict] =
    owner.data.get(key) match {
      case Some(dict: Prim.Dict)     => Some(dict)
      case Some(Prim.Ref(number, _)) => objects.get(number).flatMap(obj => dictAt(obj.data))
      case _                         => None
    }

  private def mediaBoxOf(pageNumber: Long, objects: Map[Long, Obj]): Option[(Double, Double, Double, Double)] = {
    var current = objects.get(pageNumber)
    val seen    = scala.collection.mutable.Set.empty[Long]
    while current.nonEmpty do
      val obj = current.get
      seen += obj.index.number
      dictAt(obj.data).flatMap(rectAt(_, "MediaBox")) match {
        case found @ Some(_) =>
          return found
        case None =>
          val parent = dictAt(obj.data).flatMap(_.data.get("Parent")).collect { case Prim.Ref(number, _) => number }
          current = parent.filterNot(seen.contains).flatMap(objects.get)
      }
    None
  }

  private def rectAt(dict: Prim.Dict, key: String): Option[(Double, Double, Double, Double)] =
    dict.data.get(key).collect {
      case Prim.Array(entries) if entries.length >= 4 =>
        (asDouble(entries(0)), asDouble(entries(1)), asDouble(entries(2)), asDouble(entries(3)))
    }.collect { case (Some(a), Some(b), Some(c), Some(d)) =>
      (a, b, c, d)
    }

  private def asDouble(value: Prim): Option[Double] =
    value match {
      case Prim.Number(number) => Some(number.toDouble)
      case _                   => None
    }

  private def normalize(box: (Double, Double, Double, Double)): (Double, Double, Double, Double) = {
    val (a, b, c, d) = box
    (math.min(a, c), math.min(b, d), math.max(a, c), math.max(b, d))
  }

  private def pdfNum(value: Double): String =
    if value.isWhole && value.abs <= Long.MaxValue.toDouble then value.toLong.toString
    else BigDecimal(value).bigDecimal.stripTrailingZeros.toPlainString

  private def pdfLiteral(value: String): String = {
    val escaped = value.flatMap {
      case '\\' => "\\\\"
      case '('  => "\\("
      case ')'  => "\\)"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case char => char.toString
    }
    s"($escaped)"
  }

  private def isCatalog(dict: Prim.Dict): Boolean =
    dict.data.get("Type").contains(Prim.Name("Catalog"))

  private def dictAt(data: Prim): Option[Prim.Dict] =
    data match {
      case dict: Prim.Dict => Some(dict)
      case _               => None
    }

  private def objectMap(decoded: Chunk[Decoded]): Map[Long, Obj] =
    decoded.collect {
      case Decoded.DataObj(obj)          => obj.index.number -> obj
      case Decoded.ContentObj(obj, _, _) => obj.index.number -> obj
    }.toMap

  private def streamMap(decoded: Chunk[Decoded]): Map[Long, BitVector] =
    decoded.collect { case Decoded.ContentObj(obj, rawStream, _) =>
      obj.index.number -> rawStream
    }.toMap

  private def nextObjectNumber(objects: Map[Long, Obj], decoded: Chunk[Decoded]): Long =
    (objects.keys.iterator ++ streamMap(decoded).keys.iterator).maxOption.getOrElse(0L) + 1L

  private def writeParts(parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    ZStream
      .fromChunk(parts)
      .via(WritePdf.parts)
      .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
}
