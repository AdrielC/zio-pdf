/*
 * Text and image watermark stamps painted into page content streams.
 *
 * Text uses standard-14 Type1 fonts; images are embedded as Image XObjects.
 * Encrypted filings are rejected; custom font embedding is not supported.
 */

package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.bits.BitVector
import zio.{Chunk, ZIO}
import zio.stream.ZStream

object PdfWatermark {

  sealed abstract class Error(message: String) extends Exception(message)

  case object NoPages extends Error("document has no pages")

  case object EmptyText extends Error("watermark text must not be empty")

  final case class InvalidRange(fromPage: Int, toPage: Int, pageCount: Int)
      extends Error(s"page range $fromPage-$toPage is outside 1-$pageCount")

  final case class InvalidOpacity(value: Double) extends Error(s"opacity must be between 0 and 1: $value")

  final case class InvalidImageSize(width: Int, height: Int)
      extends Error(s"image width and height must be positive: $width x $height")

  final case class InvalidImagePixels(expected: Int, actual: Int)
      extends Error(s"image pixels: expected $expected bytes, got $actual")

  final case class EncodeFailed(detail: String) extends Error(detail)

  sealed trait Stamp {
    def fromPage: Int
    def toPage: Option[Int]
  }

  enum StandardFont {
    case Helvetica, HelveticaBold, HelveticaOblique, HelveticaBoldOblique
    case TimesRoman, TimesBold, TimesItalic, TimesBoldItalic
    case Courier, CourierBold, CourierOblique, CourierBoldOblique

    private[pdf] def resourceName: String =
      this match {
        case Helvetica               => "Helv"
        case HelveticaBold           => "HeBo"
        case HelveticaOblique        => "HeOb"
        case HelveticaBoldOblique    => "HeBO"
        case TimesRoman              => "TiRo"
        case TimesBold               => "TiBo"
        case TimesItalic             => "TiIt"
        case TimesBoldItalic         => "TiBI"
        case Courier                 => "Cour"
        case CourierBold             => "CoBo"
        case CourierOblique          => "CoOb"
        case CourierBoldOblique      => "CoBO"
      }

    private[pdf] def baseFont: String =
      this match {
        case Helvetica            => "Helvetica"
        case HelveticaBold        => "Helvetica-Bold"
        case HelveticaOblique     => "Helvetica-Oblique"
        case HelveticaBoldOblique => "Helvetica-BoldOblique"
        case TimesRoman           => "Times-Roman"
        case TimesBold            => "Times-Bold"
        case TimesItalic          => "Times-Italic"
        case TimesBoldItalic      => "Times-BoldItalic"
        case Courier              => "Courier"
        case CourierBold          => "Courier-Bold"
        case CourierOblique       => "Courier-Oblique"
        case CourierBoldOblique   => "Courier-BoldOblique"
      }
  }

  enum Placement {
    case Center, TopLeft, TopCenter, TopRight, MiddleLeft, MiddleRight, BottomLeft, BottomCenter, BottomRight

    private[pdf] def marginFactor: Double = 0.08
  }

  sealed trait Color

  object Color {
    final case class Gray(value: Double) extends Color
    final case class Rgb(red: Double, green: Double, blue: Double) extends Color
  }

  final case class Text(
    text: String,
    font: StandardFont = StandardFont.Helvetica,
    color: Color = Color.Gray(0.72),
    opacity: Double = 1.0,
    placement: Placement = Placement.Center,
    diagonal: Boolean = true,
    rotationDegrees: Double = 0.0,
    fontSize: Option[Double] = None,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  ) extends Stamp

  final case class GrayImage(
    width: Int,
    height: Int,
    pixels: Chunk[Byte],
    opacity: Double = 0.35,
    placement: Placement = Placement.Center,
    scale: Double = 0.25,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  ) extends Stamp

  final case class RgbImage(
    width: Int,
    height: Int,
    pixels: Chunk[Byte],
    opacity: Double = 0.35,
    placement: Placement = Placement.Center,
    scale: Double = 0.25,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  ) extends Stamp

  final case class JpegImage(
    width: Int,
    height: Int,
    jpeg: Chunk[Byte],
    opacity: Double = 0.35,
    placement: Placement = Placement.Center,
    scale: Double = 0.25,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  ) extends Stamp

  def fromBytes(
    bytes: Chunk[Byte],
    stamp: Stamp,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    if bytes.size.toLong > opts.maxMaterializedDocumentBytes.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, bytes.size.toLong))
    else
      PdfEngine.decode(bytes, opts).flatMap { decoded =>
        ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded)) *>
          ZIO.fromEither(parts(decoded, stamp)).flatMap(writeParts)
      }

  def parts(decoded: Chunk[Decoded], stamp: Stamp): Either[Error, Chunk[Part[Trailer]]] =
    stamp match {
      case text: Text      => textParts(decoded, text)
      case gray: GrayImage => imageParts(decoded, gray, buildGrayImage(gray))
      case rgb: RgbImage   => imageParts(decoded, rgb, buildRgbImage(rgb))
      case jpeg: JpegImage => imageParts(decoded, jpeg, buildJpegImage(jpeg))
    }

  private def textParts(decoded: Chunk[Decoded], stamp: Text): Either[Error, Chunk[Part[Trailer]]] = {
    val label = stamp.text.trim
    if label.isEmpty then Left(EmptyText)
    else if stamp.opacity < 0.0 || stamp.opacity > 1.0 then Left(InvalidOpacity(stamp.opacity))
    else
      selectedPages(decoded, stamp.fromPage, stamp.toPage).flatMap { case (targets, objects) =>
        val start  = nextObjectNumber(objects, decoded)
        val fresh  = Fresh(start)
        val extras = Chunk.newBuilder[Part[Trailer]]
        val updates = scala.collection.mutable.Map.empty[Long, Prim.Dict]
        targets.foreach { pageNumber =>
          objects.get(pageNumber).flatMap(obj => dictAt(obj.data)).foreach { page =>
            val box    = mediaBoxOf(pageNumber, objects).getOrElse((0.0, 0.0, 612.0, 792.0))
            val stream = fresh.next()
            extras += Part.Obj(
              IndirectObj.stream(stream, Prim.dict(), BitVector(textContent(label, box, stamp)))
            )
            var stamped = appendContents(page, Prim.Ref(stream, 0))
            stamped = addFont(stamped, objects, stamp.font)
            if stamp.opacity < 1.0 then stamped = addExtGState(stamped, objects, stamp.opacity, "GsWm")
            updates.update(pageNumber, stamped)
          }
        }
        Right(rewrite(decoded, updates.toMap, extras.result(), fresh.peek))
      }
  }

  private def imageParts(
    decoded: Chunk[Decoded],
    stamp: Stamp,
    build: Fresh => Either[Error, IndirectObj]
  ): Either[Error, Chunk[Part[Trailer]]] =
    imageStamp(stamp).flatMap { spec =>
      selectedPages(decoded, spec.fromPage, spec.toPage).flatMap { case (targets, objects) =>
        val start   = nextObjectNumber(objects, decoded)
        val fresh   = Fresh(start)
        val extras  = Chunk.newBuilder[Part[Trailer]]
        val updates = scala.collection.mutable.Map.empty[Long, Prim.Dict]
        val stamped = targets.foldLeft[Either[Error, Unit]](Right(())) { (acc, pageNumber) =>
          acc.flatMap { _ =>
            objects.get(pageNumber).flatMap(obj => dictAt(obj.data)) match {
              case None => Right(())
              case Some(page) =>
                build(fresh).flatMap { imageObj =>
                  val box       = mediaBoxOf(pageNumber, objects).getOrElse((0.0, 0.0, 612.0, 792.0))
                  val imageName = unusedName(xobjectNames(page, objects), "WmImg")
                  extras += Part.Obj(imageObj)
                  val stream = fresh.next()
                  extras += Part.Obj(
                    IndirectObj.stream(
                      stream,
                      Prim.dict(),
                      BitVector(imageContent(box, imageName, spec))
                    )
                  )
                  var updated = appendContents(page, Prim.Ref(stream, 0))
                  updated = addXObject(updated, objects, imageName, Prim.Ref(imageObj.obj.index.number, 0))
                  if spec.opacity < 1.0 then updated = addExtGState(updated, objects, spec.opacity, "GsWm")
                  updates.update(pageNumber, updated)
                  Right(())
                }
            }
          }
        }
        stamped.map(_ => rewrite(decoded, updates.toMap, extras.result(), fresh.peek))
      }
    }

  private final case class ImageSpec(
    width: Int,
    height: Int,
    opacity: Double,
    placement: Placement,
    scale: Double,
    fromPage: Int,
    toPage: Option[Int]
  )

  private def imageStamp(stamp: Stamp): Either[Error, ImageSpec] =
    stamp match {
      case gray: GrayImage =>
        validateImage(gray.width, gray.height, gray.pixels.size, gray.opacity).map { _ =>
          ImageSpec(gray.width, gray.height, gray.opacity, gray.placement, gray.scale, gray.fromPage, gray.toPage)
        }
      case rgb: RgbImage =>
        validateImage(rgb.width, rgb.height, rgb.pixels.size, rgb.opacity, bytesPerPixel = 3).map { _ =>
          ImageSpec(rgb.width, rgb.height, rgb.opacity, rgb.placement, rgb.scale, rgb.fromPage, rgb.toPage)
        }
      case jpeg: JpegImage =>
        if jpeg.width <= 0 || jpeg.height <= 0 then Left(InvalidImageSize(jpeg.width, jpeg.height))
        else if jpeg.jpeg.isEmpty then Left(InvalidImagePixels(1, 0))
        else if jpeg.opacity < 0.0 || jpeg.opacity > 1.0 then Left(InvalidOpacity(jpeg.opacity))
        else
          Right(
            ImageSpec(jpeg.width, jpeg.height, jpeg.opacity, jpeg.placement, jpeg.scale, jpeg.fromPage, jpeg.toPage)
          )
      case _: Text =>
        Left(EmptyText)
    }

  private def validateImage(
    width: Int,
    height: Int,
    pixelBytes: Int,
    opacity: Double,
    bytesPerPixel: Int = 1
  ): Either[Error, Unit] =
    if width <= 0 || height <= 0 then Left(InvalidImageSize(width, height))
    else if opacity < 0.0 || opacity > 1.0 then Left(InvalidOpacity(opacity))
    else
      val expected = width * height * bytesPerPixel
      if pixelBytes < expected then Left(InvalidImagePixels(expected, pixelBytes))
      else Right(())

  private def buildGrayImage(stamp: GrayImage): Fresh => Either[Error, IndirectObj] =
    fresh =>
      val expected = stamp.width * stamp.height
      FlateEncode(BitVector.view(stamp.pixels.take(expected).toArray)) match {
        case _root_.scodec.Attempt.Successful(compressed) =>
          Right(
            IndirectObj.stream(
              fresh.next(),
              Prim.dict(
                "Type"             -> Prim.Name("XObject"),
                "Subtype"          -> Prim.Name("Image"),
                "Width"            -> Prim.Number(stamp.width),
                "Height"           -> Prim.Number(stamp.height),
                "ColorSpace"       -> Prim.Name("DeviceGray"),
                "BitsPerComponent" -> Prim.Number(8),
                "Filter"           -> Prim.Name("FlateDecode")
              ),
              compressed
            )
          )
        case _root_.scodec.Attempt.Failure(cause) =>
          Left(EncodeFailed(s"image FlateEncode: ${cause.messageWithContext}"))
      }

  private def buildRgbImage(stamp: RgbImage): Fresh => Either[Error, IndirectObj] =
    fresh =>
      val expected = stamp.width * stamp.height * 3
      FlateEncode(BitVector.view(stamp.pixels.take(expected).toArray)) match {
        case _root_.scodec.Attempt.Successful(compressed) =>
          Right(
            IndirectObj.stream(
              fresh.next(),
              Prim.dict(
                "Type"             -> Prim.Name("XObject"),
                "Subtype"          -> Prim.Name("Image"),
                "Width"            -> Prim.Number(stamp.width),
                "Height"           -> Prim.Number(stamp.height),
                "ColorSpace"       -> Prim.Name("DeviceRGB"),
                "BitsPerComponent" -> Prim.Number(8),
                "Filter"           -> Prim.Name("FlateDecode")
              ),
              compressed
            )
          )
        case _root_.scodec.Attempt.Failure(cause) =>
          Left(EncodeFailed(s"image FlateEncode: ${cause.messageWithContext}"))
      }

  private def buildJpegImage(stamp: JpegImage): Fresh => Either[Error, IndirectObj] =
    fresh =>
      Right(
        IndirectObj.stream(
          fresh.next(),
          Prim.dict(
            "Type"             -> Prim.Name("XObject"),
            "Subtype"          -> Prim.Name("Image"),
            "Width"            -> Prim.Number(stamp.width),
            "Height"           -> Prim.Number(stamp.height),
            "ColorSpace"       -> Prim.Name("DeviceRGB"),
            "BitsPerComponent" -> Prim.Number(8),
            "Filter"           -> Prim.Name("DCTDecode")
          ),
          BitVector(stamp.jpeg.toArray)
        )
      )

  private def selectedPages(
    decoded: Chunk[Decoded],
    fromPage: Int,
    toPage: Option[Int]
  ): Either[Error, (Set[Long], Map[Long, Obj])] = {
    val objects = objectMap(decoded)
    val pages   = TextExtract.orderedPageObjectNumbers(decoded)
    if pages.isEmpty then Left(NoPages)
    else
      val last = toPage.getOrElse(pages.size)
      if fromPage < 1 || last < fromPage || last > pages.size then Left(InvalidRange(fromPage, last, pages.size))
      else Right((pages.slice(fromPage - 1, last).toSet, objects))
  }

  private def nextObjectNumber(objects: Map[Long, Obj], decoded: Chunk[Decoded]): Long =
    (objects.keys.iterator ++ streamMap(decoded).keys.iterator).maxOption.getOrElse(0L) + 1L

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
    nextNumber: Long
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
        Chunk(Part.Obj(IndirectObj(applyPageUpdate(obj, pages), Some(rawStream))))
    }
    rewritten ++ extra
  }

  private def applyPageUpdate(obj: Obj, pages: Map[Long, Prim.Dict]): Obj =
    pages.get(obj.index.number) match {
      case Some(dict) => Obj(obj.index, dict)
      case None       => obj
    }

  private def textContent(text: String, box: (Double, Double, Double, Double), stamp: Text): Array[Byte] = {
    val (x1, y1, x2, y2) = normalize(box)
    val width            = math.max(x2 - x1, 1.0)
    val height           = math.max(y2 - y1, 1.0)
    val size             = stamp.fontSize.getOrElse(math.max(18.0, math.min(width, height) * 0.12))
    val estimated        = math.max(size, text.length.toDouble * size * 0.5)
    val angle =
      if stamp.rotationDegrees != 0.0 then math.toRadians(stamp.rotationDegrees)
      else if stamp.diagonal && stamp.placement == Placement.Center then math.atan2(height, width)
      else 0.0
    val cos = math.cos(angle)
    val sin = math.sin(angle)
    val (anchorX, anchorY) = anchor(stamp.placement, box, estimated, size, angle)
    val tx               = anchorX - estimated / 2.0 * cos
    val ty               = anchorY - estimated / 2.0 * sin
    val gs               = if stamp.opacity < 1.0 then "/GsWm gs\n" else ""
    val body =
      s"""q
         |${gs}BT
         |/${stamp.font.resourceName} ${pdfNum(size)} Tf
         |${colorOperator(stamp.color)}
         |${pdfNum(cos)} ${pdfNum(sin)} ${pdfNum(-sin)} ${pdfNum(cos)} ${pdfNum(tx)} ${pdfNum(ty)} Tm
         |${pdfLiteral(text)} Tj
         |ET
         |Q
         |""".stripMargin
    body.getBytes(StandardCharsets.ISO_8859_1)
  }

  private def imageContent(
    box: (Double, Double, Double, Double),
    imageName: String,
    spec: ImageSpec
  ): Array[Byte] = {
    val (x1, y1, x2, y2) = normalize(box)
    val pageW              = math.max(x2 - x1, 1.0)
    val pageH              = math.max(y2 - y1, 1.0)
    val maxW               = pageW * spec.scale
    val maxH               = pageH * spec.scale
    val aspect             = spec.width.toDouble / math.max(spec.height.toDouble, 1.0)
    val (drawW, drawH) =
      if maxW / aspect <= maxH then (maxW, maxW / aspect)
      else (maxH * aspect, maxH)
    val (tx, ty) = imageAnchor(spec.placement, box, drawW, drawH)
    val gs       = if spec.opacity < 1.0 then "/GsWm gs\n" else ""
    val body =
      s"""q
         |${gs}${pdfNum(drawW)} 0 0 ${pdfNum(drawH)} ${pdfNum(tx)} ${pdfNum(ty)} cm
         |/$imageName Do
         |Q
         |""".stripMargin
    body.getBytes(StandardCharsets.ISO_8859_1)
  }

  private def anchor(
    placement: Placement,
    box: (Double, Double, Double, Double),
    textWidth: Double,
    textHeight: Double,
    angle: Double
  ): (Double, Double) = {
    val (x1, y1, x2, y2) = normalize(box)
    val margin             = math.min(x2 - x1, y2 - y1) * placement.marginFactor
    placement match {
      case Placement.Center =>
        (x1 + (x2 - x1) / 2.0, y1 + (y2 - y1) / 2.0)
      case Placement.TopLeft =>
        (x1 + margin + textWidth / 2.0 * math.cos(angle), y2 - margin - textHeight / 2.0)
      case Placement.TopCenter =>
        (x1 + (x2 - x1) / 2.0, y2 - margin - textHeight / 2.0)
      case Placement.TopRight =>
        (x2 - margin - textWidth / 2.0 * math.cos(angle), y2 - margin - textHeight / 2.0)
      case Placement.MiddleLeft =>
        (x1 + margin + textWidth / 2.0, y1 + (y2 - y1) / 2.0)
      case Placement.MiddleRight =>
        (x2 - margin - textWidth / 2.0, y1 + (y2 - y1) / 2.0)
      case Placement.BottomLeft =>
        (x1 + margin + textWidth / 2.0 * math.cos(angle), y1 + margin + textHeight / 2.0)
      case Placement.BottomCenter =>
        (x1 + (x2 - x1) / 2.0, y1 + margin + textHeight / 2.0)
      case Placement.BottomRight =>
        (x2 - margin - textWidth / 2.0 * math.cos(angle), y1 + margin + textHeight / 2.0)
    }
  }

  private def imageAnchor(
    placement: Placement,
    box: (Double, Double, Double, Double),
    drawW: Double,
    drawH: Double
  ): (Double, Double) = {
    val (x1, y1, x2, y2) = normalize(box)
    val margin             = math.min(x2 - x1, y2 - y1) * placement.marginFactor
    placement match {
      case Placement.Center       => (x1 + (x2 - x1 - drawW) / 2.0, y1 + (y2 - y1 - drawH) / 2.0)
      case Placement.TopLeft      => (x1 + margin, y2 - margin - drawH)
      case Placement.TopCenter    => (x1 + (x2 - x1 - drawW) / 2.0, y2 - margin - drawH)
      case Placement.TopRight     => (x2 - margin - drawW, y2 - margin - drawH)
      case Placement.MiddleLeft   => (x1 + margin, y1 + (y2 - y1 - drawH) / 2.0)
      case Placement.MiddleRight  => (x2 - margin - drawW, y1 + (y2 - y1 - drawH) / 2.0)
      case Placement.BottomLeft   => (x1 + margin, y1 + margin)
      case Placement.BottomCenter => (x1 + (x2 - x1 - drawW) / 2.0, y1 + margin)
      case Placement.BottomRight  => (x2 - margin - drawW, y1 + margin)
    }
  }

  private def colorOperator(color: Color): String =
    color match {
      case Color.Gray(value)           => s"${pdfNum(value)} g"
      case Color.Rgb(red, green, blue) => s"${pdfNum(red)} ${pdfNum(green)} ${pdfNum(blue)} rg"
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

  private def appendContents(page: Prim.Dict, overlay: Prim.Ref): Prim.Dict =
    page.data.get("Contents") match {
      case Some(existing: Prim.Ref) =>
        Prim.Dict(page.data.updated("Contents", Prim.Array(existing, overlay)))
      case Some(Prim.Array(entries)) =>
        Prim.Dict(page.data.updated("Contents", Prim.Array((entries.iterator.toSeq :+ overlay)*)))
      case _ =>
        Prim.Dict(page.data.updated("Contents", overlay))
    }

  private def addFont(page: Prim.Dict, objects: Map[Long, Obj], font: StandardFont): Prim.Dict = {
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

  private def addXObject(
    page: Prim.Dict,
    objects: Map[Long, Obj],
    name: String,
    ref: Prim.Ref
  ): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val xobjects  = resolvedDict(resources, "XObject", objects).getOrElse(Prim.dict())
    val updatedX  = Prim.Dict(xobjects.data.updated(name, ref))
    val updatedR  = Prim.Dict(resources.data.updated("XObject", updatedX))
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

  private def xobjectNames(page: Prim.Dict, objects: Map[Long, Obj]): Set[String] =
    resolvedDict(page, "Resources", objects)
      .flatMap(resources => resolvedDict(resources, "XObject", objects))
      .map(_.data.keys.toSet)
      .getOrElse(Set.empty)

  private def unusedName(used: Set[String], candidate: String): String =
    if !used.contains(candidate) then candidate
    else
      Iterator
        .from(2)
        .map(index => s"${candidate}_$index")
        .find(name => !used.contains(name))
        .getOrElse(candidate)

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

  private def writeParts(parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    ZStream
      .fromChunk(parts)
      .via(WritePdf.parts)
      .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
}
