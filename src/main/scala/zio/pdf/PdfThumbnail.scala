/*
 * Placeholder /Thumb image generation without a page renderer.
 *
 * Builds small DeviceGray Flate-compressed image XObjects and attaches them to
 * page dictionaries for linearized web previews and inspection.
 */

package zio.pdf

import _root_.scodec.bits.BitVector
import zio.*
import zio.stream.ZStream

object PdfThumbnail {

  enum Scope:
    case AllPages, FirstPageOnly, Off

  /** Supplies DeviceGray pixels (`width * height` bytes) for a page index. */
  type PixelSource = (Long, Int, Int) => Either[String, Array[Byte]]

  final case class Options(
    width: Int = 64,
    height: Int = 64,
    scope: Scope = Scope.FirstPageOnly,
    largeDocPageThreshold: Int = 50,
    pixelSource: Option[PixelSource] = None
  )

  /** Deterministic placeholder tiles — works on JVM and Scala.js without a renderer. */
  def placeholderOptions(
    width: Int = 64,
    height: Int = 64,
    scope: Scope = Scope.FirstPageOnly
  ): Options =
    Options(width = width, height = height, scope = scope)

  /** Caller-supplied DeviceGray pixels (PDFBox on JVM, PDF.js canvas in the browser). */
  def renderedOptions(
    pixelSource: PixelSource,
    width: Int = 64,
    height: Int = 64,
    scope: Scope = Scope.FirstPageOnly
  ): Options =
    Options(width = width, height = height, scope = scope, pixelSource = Some(pixelSource))

  /** Apply thumbnails to an existing PDF without rewriting the body. */
  def enrichBytes(bytes: Chunk[Byte], options: Options = Options()): ZIO[Any, Throwable, Chunk[Byte]] =
    options.scope match {
      case Scope.Off =>
        ZIO.succeed(bytes)
      case Scope.FirstPageOnly =>
        appendFirstPage(bytes, options)
      case Scope.AllPages =>
        for {
          decoded <- ZStream.fromChunk(bytes).via(PdfStream.decode()).runCollect
          parts   <- ZStream.fromChunk(decoded).via(Decoded.parts).runCollect
          maxObj   = decoded.foldLeft(0L) {
                       case (max, Decoded.DataObj(obj))           => math.max(max, obj.index.number)
                       case (max, Decoded.ContentObj(obj, _, _)) => math.max(max, obj.index.number)
                       case (max, _)                             => max
                     }
          enriched <- ZIO.fromEither(
                        enrichParts(
                          parts,
                          maxObj + 1L,
                          options.copy(scope = Scope.AllPages, largeDocPageThreshold = Int.MaxValue)
                        ).left.map(new RuntimeException(_))
                      )
          rewritten <- ZStream
                         .fromChunk(enriched)
                         .via(WritePdf.parts)
                         .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
        } yield rewritten
    }

  /** Incrementally attach a `/Thumb` to the first page (preserves the original prefix). */
  def appendFirstPage(bytes: Chunk[Byte], options: Options = Options()): ZIO[Any, Throwable, Chunk[Byte]] =
    for {
      decoded <- ZStream.fromChunk(bytes).via(PdfStream.decode()).runCollect
      trailer <- ZIO.fromOption(PdfAppend.latestTrailer(decoded)).orElseFail(PdfAppend.NoTrailer)
      pageNumber <- ZIO.fromOption(TextExtract.orderedPageObjectNumbers(decoded).headOption)
                      .orElseFail(new RuntimeException("thumbnail append: no pages found"))
      pageObj <- ZIO.fromOption(findPageObject(decoded, pageNumber))
                   .orElseFail(new RuntimeException(s"thumbnail append: missing page object $pageNumber"))
      thumbNumber = PdfAppend.nextObjectNumber(decoded, trailer)
      thumb <- ZIO.fromEither(imageObject(thumbNumber, 0L, options).left.map(new RuntimeException(_)))
      updatedPage = attach(pageObj, thumbNumber)
      revision = Chunk(
        Part.Obj(thumb),
        Part.Obj(updatedPage),
        Part.Meta(Trailer(BigDecimal(thumbNumber + 1L), Prim.dict(), None))
      )
      appended <- PdfAppend.append(bytes, revision, preserveNumbers = Set(pageNumber))
    } yield appended

  private def findPageObject(decoded: Chunk[Decoded], pageNumber: Long): Option[IndirectObj] =
    decoded.collectFirst {
      case Decoded.DataObj(obj) if obj.index.number == pageNumber =>
        IndirectObj(obj, None)
      case Decoded.ContentObj(obj, rawStream, _) if obj.index.number == pageNumber =>
        IndirectObj(obj, Some(rawStream))
    }

  /** Build a grayscale thumbnail image XObject for a page. */
  def imageObject(number: Long, pageNumber: Long, options: Options = Options()): Either[String, IndirectObj] = {
    val width  = math.max(1, options.width)
    val height = math.max(1, options.height)
    options.pixelSource match {
      case Some(render) =>
        render(pageNumber, width, height).flatMap(pixels => grayImageObject(number, width, height, pixels))
      case None =>
        grayImageObject(number, width, height, patternPixels(pageNumber, width, height))
    }
  }

  /** Encode raw DeviceGray pixels as a Flate-compressed `/Thumb` image XObject. */
  def grayImageObject(number: Long, width: Int, height: Int, pixels: Array[Byte]): Either[String, IndirectObj] =
    if pixels.length < width * height then Left(s"thumbnail pixels: expected ${width * height} bytes, got ${pixels.length}")
    else
      FlateEncode(BitVector.view(pixels.take(width * height))) match {
        case _root_.scodec.Attempt.Successful(compressed) =>
          Right(
            IndirectObj.stream(
              number,
              Prim.dict(
                "Type"             -> Prim.Name("XObject"),
                "Subtype"          -> Prim.Name("Image"),
                "Width"            -> Prim.Number(width),
                "Height"           -> Prim.Number(height),
                "ColorSpace"       -> Prim.Name("DeviceGray"),
                "BitsPerComponent" -> Prim.Number(8),
                "Filter"           -> Prim.Name("FlateDecode")
              ),
              compressed
            )
          )
        case _root_.scodec.Attempt.Failure(cause) =>
          Left(s"thumbnail FlateEncode: ${cause.messageWithContext}")
      }

  /** Attach `/Thumb` to a page indirect object. */
  def attach(page: IndirectObj, thumbNumber: Long): IndirectObj =
    page.obj.data match {
      case dict: Prim.Dict =>
        IndirectObj(
          page.obj.copy(data = Prim.Dict(dict.data.updated("Thumb", Prim.Ref(thumbNumber, 0)))),
          page.stream
        )
      case _ =>
        page
    }

  def attachPreencoded(page: Part.Preencoded, thumbNumber: Long): Either[String, Part.Preencoded] =
    attachBytes(page.bytes, thumbNumber).map(bytes => Part.Preencoded(page.index, bytes))

  /**
   * Insert thumbnail image objects and rewrite page dictionaries in a part
   * stream. New objects are numbered from `thumbStartNumber` upward.
   */
  def enrichParts(
    parts: Chunk[Part[Trailer]],
    thumbStartNumber: Long,
    options: Options = Options()
  ): Either[String, Chunk[Part[Trailer]]] =
    if options.scope == Scope.Off then Right(parts)
    else {
      val pageCount = countPages(parts)
      val effectiveScope =
        if options.scope == Scope.AllPages || pageCount <= options.largeDocPageThreshold then options.scope
        else Scope.FirstPageOnly
      var nextNumber = thumbStartNumber
      val out        = Chunk.newBuilder[Part[Trailer]]
      var pageIndex  = 0L
      parts.foldLeft[Either[String, Unit]](Right(())) {
        case (Left(error), _) =>
          Left(error)
        case (Right(_), part) =>
          part match {
            case Part.Obj(obj) if isPage(obj) =>
              val attachThumb = effectiveScope == Scope.AllPages || pageIndex == 0L
              if attachThumb then
                val thumbNumber = nextNumber
                nextNumber += 1L
                imageObject(thumbNumber, pageIndex, options) match {
                  case Left(error) =>
                    Left(error)
                  case Right(thumbnail) =>
                    out += Part.Obj(thumbnail)
                    out += Part.Obj(attach(obj, thumbNumber))
                    pageIndex += 1L
                    Right(())
                }
              else
                out += part
                pageIndex += 1L
                Right(())
            case preencoded @ Part.Preencoded(_, bytes) if isPageBytes(bytes) =>
              val attachThumb = effectiveScope == Scope.AllPages || pageIndex == 0L
              if attachThumb then
                val thumbNumber = nextNumber
                nextNumber += 1L
                imageObject(thumbNumber, pageIndex, options) match {
                  case Left(error) =>
                    Left(error)
                  case Right(thumbnail) =>
                    attachPreencoded(preencoded, thumbNumber) match {
                      case Left(err)     => Left(err)
                      case Right(pagePe) =>
                        out += Part.Obj(thumbnail)
                        out += pagePe
                        pageIndex += 1L
                        Right(())
                    }
                }
              else
                out += part
                pageIndex += 1L
                Right(())
            case other =>
              out += other
              Right(())
          }
      }.map(_ => out.result())
    }

  private def countPages(parts: Chunk[Part[Trailer]]): Int =
    parts.count {
      case Part.Obj(obj)                 => isPage(obj)
      case Part.Preencoded(_, bytes)     => isPageBytes(bytes)
      case _                             => false
    }

  private def isPage(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case Prim.tpe("Page", _) => true
      case _                   => false
    }

  private def isPageBytes(bytes: _root_.scodec.bits.ByteVector): Boolean = {
    val sample = new String(bytes.toArray.take(4096), java.nio.charset.StandardCharsets.ISO_8859_1)
    sample.contains("/Type") && sample.contains("/Page") && !sample.contains("/Pages")
  }

  private def attachBytes(pageBytes: _root_.scodec.bits.ByteVector, thumbNumber: Long): Either[String, _root_.scodec.bits.ByteVector] = {
    val text = new String(pageBytes.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
    val end  = text.lastIndexOf(">>")
    if end < 0 then Left("thumbnail attach: page dictionary not found")
    else
      val insertion = s"/Thumb ${thumbNumber} 0 R "
      val updated   = text.patch(end, insertion, 0)
      Right(_root_.scodec.bits.ByteVector.view(updated.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1)))
  }

  /** Deterministic mini-page placeholder: white border with a shaded interior. */
  private def patternPixels(pageNumber: Long, width: Int, height: Int): Array[Byte] = {
    val border = math.max(1, math.min(width, height) / 8)
    Array.tabulate(width * height) { index =>
      val x = index % width
      val y = index / width
      if x < border || y < border || x >= width - border || y >= height - border then 235.toByte
      else
        val shade = ((pageNumber * 37L + x * 11L + y * 19L) % 160L + 50L).toByte
        shade
    }
  }
}
