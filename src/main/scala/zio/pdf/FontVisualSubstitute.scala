package zio.pdf

import scala.collection.mutable.{ArrayBuffer, LongMap}

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.Chunk
import zio.pdf.content.{ContentOps, ContentToken}

/**
 * Re-encode page text for a replacement font, then rebind page resources.
 *
 * Unlike [[PdfTransform.fonts.replaceExisting]], this path decodes glyph codes
 * through each source font's `/ToUnicode` CMap and writes new codes from the
 * target font's CMap before rebinding resource references.
 */
private[pdf] object FontVisualSubstitute {

  final case class Result(
    sourceBaseFont: String,
    targetBaseFont: String,
    sourceObjectNumbers: Chunk[Long],
    targetObjectNumber: Long,
    streamsRewritten: Long,
    resourceBindingsRewritten: Long,
    glyphsRecoded: Long
  )

  def substitute(
    document: PdfTransform.Document,
    fromBaseFont: String,
    toBaseFont: String
  ): Either[Throwable, PdfTransform.Prepared[Result]] =
    PdfTransform.fonts.replaceExistingDocument(document, fromBaseFont, toBaseFont) match
      case Right(prepared) =>
        Right(
          PdfTransform.Prepared(
            prepared.document,
            Result(
              fromBaseFont,
              toBaseFont,
              prepared.value.sourceObjectNumbers,
              prepared.value.targetObjectNumber,
              streamsRewritten = 0L,
              prepared.value.resourceBindingsRewritten,
              glyphsRecoded = 0L
            )
          )
        )
      case Left(_: PdfTransform.Error.IncompatibleFont | _: PdfTransform.Error.MetricsUnavailable) =>
        visualSubstitute(document, fromBaseFont, toBaseFont)
      case Left(error) => Left(error)

  private def visualSubstitute(
    document: PdfTransform.Document,
    fromBaseFont: String,
    toBaseFont: String
  ): Either[Throwable, PdfTransform.Prepared[Result]] = {
    val allFonts = document.rebindableFonts
    val sources  = allFonts.filter(record => baseFont(record.data).contains(fromBaseFont))
    val targets  = allFonts.filter(record => baseFont(record.data).contains(toBaseFont))

    if sources.isEmpty then Left(PdfTransform.Error.SourceFontNotFound(fromBaseFont))
    else if targets.isEmpty then Left(PdfTransform.Error.TargetFontNotFound(toBaseFont))
    else if targets.size != 1 then
      Left(PdfTransform.Error.AmbiguousTargetFont(toBaseFont, targets.map(_.index.number)))
    else {
      val target = targets.head
      for
        targetMap <- document.toUnicode(target).flatMap {
          case Some(value) => Right(value)
          case None        => Right(TextExtract.ToUnicode.identitySingleByte)
        }
        targetEncoder = TextExtract.ToUnicode.encoder(targetMap)
        acc           = accFrom(document.elements)
        rewritten <- rewritePageStreams(document, acc, fromBaseFont, targetEncoder)
        remapped <- document.remapFontResources(
          sources.iterator.map(source => source.index.number -> Prim.Ref(target.index.number, target.index.generation)).toMap
        )
      yield PdfTransform.Prepared(
        remapped._1,
        Result(
          fromBaseFont,
          toBaseFont,
          sources.map(_.index.number),
          target.index.number,
          rewritten.streams,
          remapped._2,
          rewritten.glyphs
        )
      )
    }
  }

  private final case class RewriteStats(streams: Long, glyphs: Long)

  private def rewritePageStreams(
    document: PdfTransform.Document,
    acc: TextExtract.Acc,
    fromBaseFont: String,
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): Either[Throwable, RewriteStats] = {
    var streamsRewritten = 0L
    var glyphsRecoded      = 0L
    var updated            = document
    var failure: Option[Throwable] = None

    selectedPages(acc).foreach { page =>
      if failure.isEmpty then
        val resourceNames = resourceBaseFonts(page, acc).collect {
          case (name, baseFont) if baseFont == fromBaseFont => name
        }.toSet
        if resourceNames.nonEmpty then
          val fontMaps = fontMapsFor(page, acc)
          contentRefs(page, acc.objects).foreach { streamNumber =>
            acc.streams.get(streamNumber).foreach { payload =>
              payload.stream.exec.toEither.left.map(error => new RuntimeException(error.messageWithContext)) match {
                case Left(error) => failure = Some(error)
                case Right(bits) =>
                  val (rewrittenBytes, recoded) =
                    rewriteStream(bits.toByteArray, resourceNames, fontMaps, targetEncoder)
                  if recoded > 0 || rewrittenBytes.length != bits.size.toInt then
                    updateStream(updated, streamNumber, payload, rewrittenBytes) match {
                      case Left(error)  => failure = Some(error)
                      case Right(nextDoc) =>
                        updated = nextDoc
                        streamsRewritten += 1L
                        glyphsRecoded += recoded
                    }
              }
            }
          }
    }

    failure match
      case Some(error) => Left(error)
      case None        => Right(RewriteStats(streamsRewritten, glyphsRecoded))
  }

  private def updateStream(
    document: PdfTransform.Document,
    objectNumber: Long,
    payload: TextExtract.ContentPayload,
    rewritten: Array[Byte]
  ): Either[Throwable, PdfTransform.Document] = {
    val uncompressed = BitVector(rewritten)
    val nextDataAndPayload =
      if Content.mayRewriteFilters(payload.data) then
        Content.compressFlate(payload.data, uncompressed).toEither.left.map(error => new RuntimeException(error.messageWithContext))
      else Right((payload.data, uncompressed))

    nextDataAndPayload.map { case (data, streamBits) =>
      val out = Chunk.newBuilder[Element]
      document.elements.foreach {
        case element @ Element.Content(obj, _, _, kind) if obj.index.number == objectNumber =>
          out += Element.Content(Obj(obj.index, data), streamBits, Uncompressed.now(streamBits), kind)
        case element => out += element
      }
      document.copy(elements = out.result())
    }
  }

  private def rewriteStream(
    bytes: Array[Byte],
    sourceResourceNames: Set[String],
    fontMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): (Array[Byte], Long) = {
    val tokens = ContentOps.tokenize(bytes).toArray
    val out    = ArrayBuffer.empty[ContentToken]
    var activeResource: Option[String] = None
    var recoded = 0L
    var index   = 0

    while index < tokens.length do
      tokens(index) match
        case ContentToken.Op("Tf") if index >= 2 =>
          tokens(index - 2) match
            case ContentToken.Name(name) => activeResource = Some(name)
            case _                       => ()
          out += tokens(index - 2)
          out += tokens(index - 1)
          out += tokens(index)
          index += 1
        case ContentToken.Op("Tj") if index >= 1 =>
          val rewritten = rewriteTextToken(tokens(index - 1), activeResource, sourceResourceNames, fontMaps, targetEncoder)
          recoded += rewritten.recoded
          out += rewritten.token
          out += tokens(index)
          index += 1
        case ContentToken.Op("TJ") if index >= 1 =>
          tokens(index - 1) match
            case array: ContentToken.Array =>
              val rewritten = rewriteArrayToken(array, activeResource, sourceResourceNames, fontMaps, targetEncoder)
              recoded += rewritten.recoded
              out += rewritten.token
            case other => out += other
          out += tokens(index)
          index += 1
        case ContentToken.Op("'") if index >= 1 =>
          val rewritten = rewriteTextToken(tokens(index - 1), activeResource, sourceResourceNames, fontMaps, targetEncoder)
          recoded += rewritten.recoded
          out += rewritten.token
          out += tokens(index)
          index += 1
        case ContentToken.Op("\"") if index >= 1 =>
          if index >= 4 then
            out += tokens(index - 4)
            out += tokens(index - 3)
            out += tokens(index - 2)
          val rewritten = rewriteTextToken(tokens(index - 1), activeResource, sourceResourceNames, fontMaps, targetEncoder)
          recoded += rewritten.recoded
          out += rewritten.token
          out += tokens(index)
          index += 1
        case token =>
          out += token
          index += 1

    (ContentOps.render(out.toList), recoded)
  }

  private final case class RewrittenToken(token: ContentToken, recoded: Long)

  private def rewriteTextToken(
    token: ContentToken,
    activeResource: Option[String],
    sourceResourceNames: Set[String],
    fontMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): RewrittenToken =
    activeResource.filter(sourceResourceNames.contains).flatMap(fontMaps.get) match
      case Some(sourceMap) =>
        tokenBytes(token).map { sourceBytes =>
          val text = sourceMap.decode(ByteVector.view(sourceBytes))
          val encoded = targetEncoder.encode(text)
          if encoded.sameElements(sourceBytes) then RewrittenToken(token, 0L)
          else RewrittenToken(toToken(encoded), text.length.toLong)
        }.getOrElse(RewrittenToken(token, 0L))
      case None => RewrittenToken(token, 0L)

  private def rewriteArrayToken(
    array: ContentToken.Array,
    activeResource: Option[String],
    sourceResourceNames: Set[String],
    fontMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): RewrittenToken =
    activeResource.filter(sourceResourceNames.contains).flatMap(fontMaps.get) match
      case Some(sourceMap) =>
        var recoded = 0L
        val elems = array.elems.map {
          case token @ (ContentToken.Literal(_) | ContentToken.Hex(_)) =>
            tokenBytes(token).map { sourceBytes =>
              val text = sourceMap.decode(ByteVector.view(sourceBytes))
              val encoded = targetEncoder.encode(text)
              if !encoded.sameElements(sourceBytes) then recoded += text.length.toLong
              toToken(encoded)
            }.getOrElse(token)
          case other => other
        }
        RewrittenToken(ContentToken.Array(elems), recoded)
      case None => RewrittenToken(array, 0L)

  private def tokenBytes(token: ContentToken): Option[Array[Byte]] =
    token match
      case ContentToken.Literal(value) => Some(value.toArray)
      case ContentToken.Hex(value)     => Some(value.toArray)
      case _                           => None

  private def toToken(bytes: Array[Byte]): ContentToken =
    if bytes.forall(b => b >= 32 && b <= 126 && b != '('.toByte && b != ')'.toByte) then
      ContentToken.Literal(ByteVector(bytes))
    else ContentToken.Hex(ByteVector(bytes))

  private def accFrom(elements: Chunk[Element]): TextExtract.Acc =
    elements.foldLeft(TextExtract.Acc())(TextExtract.fold)

  private def selectedPages(acc: TextExtract.Acc): Seq[Page] =
    val tree = for {
      trailer <- acc.trailer
      root    <- trailer.root
    } yield pageTree(root.number, acc, Set.empty)
    tree.filter(_.nonEmpty).getOrElse(acc.pages.toSeq)

  private def pageTree(number: Long, acc: TextExtract.Acc, visited: Set[Long]): List[Page] =
    if visited(number) then Nil
    else
      acc.objects.get(number) match
        case Some(dict: Prim.Dict) if dict.data.get("Type").contains(Prim.Name("Catalog")) =>
          dict.data.get("Pages").toList.flatMap {
            case Prim.Ref(pages, _) => pageTree(pages, acc, visited + number)
            case _                  => Nil
          }
        case Some(dict: Prim.Dict) if dict.data.get("Type").contains(Prim.Name("Page")) =>
          acc.pagesByIndex.get(number).toList
        case Some(dict: Prim.Dict) if dict.data.get("Type").contains(Prim.Name("Pages")) =>
          dict.data.get("Kids").toList.flatMap(indirectArray(_, acc.objects, visited)).flatMap {
            case Prim.Ref(child, _) => pageTree(child, acc, visited + number)
            case _                  => Nil
          }
        case _ => Nil

  private def indirectArray(value: Prim, objects: LongMap[Prim], visited: Set[Long]): List[Prim] =
    value match
      case Prim.Array(values) => values.toList
      case Prim.Ref(number, _) if !visited(number) =>
        objects.get(number).toList.flatMap(indirectArray(_, objects, visited + number))
      case _ => Nil

  private def contentRefs(page: Page, objects: LongMap[Prim]): List[Long] =
    def resolve(value: Prim, visited: Set[Long]): List[Long] =
      value match
        case Prim.Ref(number, _) if !visited(number) =>
          objects.get(number) match
            case Some(indirect) => resolve(indirect, visited + number)
            case None           => List(number)
        case Prim.Array(values) => values.iterator.flatMap(resolve(_, visited)).toList
        case _                  => Nil

    page.data.data.get("Contents").toList.flatMap(resolve(_, Set.empty))

  private def resourceBaseFonts(page: Page, acc: TextExtract.Acc): Map[String, String] =
    resourcesFor(page.data, acc.objects)
      .flatMap(_.data.get("Font"))
      .flatMap(dictFor(_, acc.objects))
      .fold(Map.empty[String, String]) { fonts =>
        fonts.data.iterator.flatMap { case (name, font) =>
          for
            fontDict <- dictFor(font, acc.objects)
            base <- baseName(fontDict, "BaseFont")
          yield name -> base
        }.toMap
      }

  private def fontMapsFor(page: Page, acc: TextExtract.Acc): Map[String, TextExtract.ToUnicode] =
    resourcesFor(page.data, acc.objects)
      .flatMap(_.data.get("Font"))
      .flatMap(dictFor(_, acc.objects))
      .fold(Map.empty[String, TextExtract.ToUnicode]) { fonts =>
        fonts.data.iterator.flatMap { case (name, font) =>
          for
            fontDict <- dictFor(font, acc.objects)
            cmap <- toUnicodeFor(fontDict, acc)
          yield name -> cmap
        }.toMap
      }

  private def toUnicodeFor(fontDict: Prim.Dict, acc: TextExtract.Acc): Option[TextExtract.ToUnicode] =
    fontDict.data.get("ToUnicode") match
      case Some(Prim.Ref(number, _)) =>
        acc.streams.get(number).flatMap { payload =>
          payload.stream.exec.toOption.flatMap(bits => TextExtract.ToUnicode.parse(bits))
        }
      case None => Some(TextExtract.ToUnicode.identitySingleByte)
      case _    => None

  private def resourcesFor(page: Prim.Dict, objects: LongMap[Prim]): Option[Prim.Dict] =
    def loop(data: Prim.Dict, visited: Set[Long]): Option[Prim.Dict] =
      data.data.get("Resources").flatMap(dictFor(_, objects)).orElse {
        data.data.get("Parent") match
          case Some(Prim.Ref(number, _)) if !visited(number) =>
            objects.get(number).collect { case parent: Prim.Dict => parent }.flatMap(loop(_, visited + number))
          case _ => None
      }
    loop(page, Set.empty)

  private def dictFor(value: Prim, objects: LongMap[Prim]): Option[Prim.Dict] =
    value match
      case dict: Prim.Dict         => Some(dict)
      case Prim.Ref(number, _)     => objects.get(number).collect { case dict: Prim.Dict => dict }
      case _                       => None

  private def baseFont(data: Prim.Dict): Option[String] =
    baseName(data, "BaseFont")

  private def baseName(data: Prim.Dict, field: String): Option[String] =
    data.data.get(field).collect { case Prim.Name(value) => value }
}
