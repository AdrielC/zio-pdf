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
 *
 * The rewrite is fail-closed: if any source glyph cannot be decoded or any
 * Unicode scalar cannot be encoded for the target font, the transform aborts
 * instead of writing corrupt content streams.
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

  /** Dry-run the visual rewrite and report the first blocking issue. */
  def preflight(
    document: PdfTransform.Document,
    fromBaseFont: String,
    toBaseFont: String
  ): Either[PdfTransform.Error.VisualRecodingFailed, Unit] =
    prepareVisual(document, fromBaseFont, toBaseFont).flatMap { prepared =>
      scanPageStreams(document, prepared.acc, fromBaseFont, toBaseFont, prepared.sourceResourceNames, prepared.sourceMaps, prepared.targetEncoder, write = false).map(_ => ())
    }

  private def visualSubstitute(
    document: PdfTransform.Document,
    fromBaseFont: String,
    toBaseFont: String
  ): Either[Throwable, PdfTransform.Prepared[Result]] =
    prepareVisual(document, fromBaseFont, toBaseFont).flatMap { prepared =>
      scanPageStreams(document, prepared.acc, fromBaseFont, toBaseFont, prepared.sourceResourceNames, prepared.sourceMaps, prepared.targetEncoder, write = true).flatMap { case (stats, rewrittenDocument) =>
        val sources = document.rebindableFonts.filter(record => baseFont(record.data).contains(fromBaseFont))
        val replacements = sources.iterator.map(source => source.index.number -> Prim.Ref(prepared.target.index.number, prepared.target.index.generation)).toMap
        rewrittenDocument.remapFontResources(replacements).map { case (remapped, bindings) =>
          PdfTransform.Prepared(
            remapped,
            Result(
              fromBaseFont,
              toBaseFont,
              sources.map(_.index.number),
              prepared.target.index.number,
              stats.streams,
              bindings,
              stats.glyphs
            )
          )
        }
      }
    }

  private final case class PreparedVisual(
    acc: TextExtract.Acc,
    target: PdfTransform.FontRecord,
    sourceResourceNames: Set[String],
    sourceMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  )

  private def prepareVisual(
    document: PdfTransform.Document,
    fromBaseFont: String,
    toBaseFont: String
  ): Either[PdfTransform.Error.VisualRecodingFailed, PreparedVisual] = {
    val allFonts = document.rebindableFonts
    val sources  = allFonts.filter(record => baseFont(record.data).contains(fromBaseFont))
    val targets  = allFonts.filter(record => baseFont(record.data).contains(toBaseFont))

    if sources.isEmpty then Left(visualFailure(fromBaseFont, toBaseFont, s"no document font has /BaseFont /$fromBaseFont"))
    else if targets.isEmpty then Left(visualFailure(fromBaseFont, toBaseFont, s"no replacement document font has /BaseFont /$toBaseFont"))
    else if targets.size != 1 then
      Left(visualFailure(fromBaseFont, toBaseFont, s"/BaseFont /$toBaseFont resolves to more than one document font"))
    else {
      val target = targets.head
      val acc    = accFrom(document.elements)
      for
        targetMap <- resolveToUnicode(target.data, acc, target.index.number, fromBaseFont, toBaseFont, role = "replacement")
        targetEncoder = TextExtract.ToUnicode.encoder(targetMap)
        sourceMaps <- collectSourceMaps(acc, sources, fromBaseFont, toBaseFont)
        sourceResourceNames = sourceMaps.keySet
      yield PreparedVisual(acc, target, sourceResourceNames, sourceMaps, targetEncoder)
    }
  }

  private def collectSourceMaps(
    acc: TextExtract.Acc,
    sources: Chunk[PdfTransform.FontRecord],
    fromBaseFont: String,
    toBaseFont: String
  ): Either[PdfTransform.Error.VisualRecodingFailed, Map[String, TextExtract.ToUnicode]] = {
    val out = scala.collection.mutable.Map.empty[String, TextExtract.ToUnicode]
    var issue: Option[PdfTransform.Error.VisualRecodingFailed] = None
    selectedPages(acc).foreach { page =>
      if issue.isEmpty then
        resourceBaseFonts(page, acc).foreach { case (name, baseFontName) =>
          if issue.isEmpty && baseFontName == fromBaseFont && !out.contains(name) then
            fontDictFor(page, acc, name).foreach { fontDict =>
              resolveToUnicode(fontDict, acc, sources.headOption.map(_.index.number).getOrElse(-1L), fromBaseFont, toBaseFont, role = "source") match
                case Left(error)  => issue = Some(error)
                case Right(value) => out.update(name, value)
            }
        }
    }
    issue match
      case Some(error) => Left(error)
      case None if out.isEmpty => Left(visualFailure(fromBaseFont, toBaseFont, s"no page resources reference /BaseFont /$fromBaseFont"))
      case None => Right(out.toMap)
  }

  private def resolveToUnicode(
    fontDict: Prim.Dict,
    acc: TextExtract.Acc,
    objectNumber: Long,
    fromBaseFont: String,
    toBaseFont: String,
    role: String
  ): Either[PdfTransform.Error.VisualRecodingFailed, TextExtract.ToUnicode] =
    fontDict.data.get("ToUnicode") match
      case Some(Prim.Ref(number, _)) =>
        acc.streams.get(number).flatMap(payload => payload.stream.exec.toOption.flatMap(bits => TextExtract.ToUnicode.parse(bits))) match
          case Some(cmap) => Right(cmap)
          case None         => Left(visualFailure(fromBaseFont, toBaseFont, s"$role font object $objectNumber has an unreadable /ToUnicode CMap"))
      case Some(_) =>
        Left(visualFailure(fromBaseFont, toBaseFont, s"$role font object $objectNumber has an invalid /ToUnicode reference"))
      case None =>
        baseName(fontDict, "Subtype") match
          case Some("Type0") =>
            Left(visualFailure(fromBaseFont, toBaseFont, s"$role composite font object $objectNumber lacks /ToUnicode"))
          case Some("Type1") | Some("TrueType") =>
            baseName(fontDict, "Encoding") match
              case Some("WinAnsiEncoding") => Right(TextExtract.ToUnicode.winAnsiSingleByte)
              case Some(encoding)            => Left(visualFailure(fromBaseFont, toBaseFont, s"$role font object $objectNumber uses unsupported /Encoding /$encoding without /ToUnicode"))
              case None                      => Left(visualFailure(fromBaseFont, toBaseFont, s"$role font object $objectNumber lacks /ToUnicode and /Encoding"))
          case subtype =>
            Left(visualFailure(fromBaseFont, toBaseFont, s"$role font object $objectNumber lacks /ToUnicode${subtype.fold("")(value => s" (/Subtype /$value)")}"))

  private final case class RewriteStats(streams: Long, glyphs: Long)

  private def scanPageStreams(
    document: PdfTransform.Document,
    acc: TextExtract.Acc,
    fromBaseFont: String,
    toBaseFont: String,
    sourceResourceNames: Set[String],
    sourceMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder,
    write: Boolean
  ): Either[PdfTransform.Error.VisualRecodingFailed, (RewriteStats, PdfTransform.Document)] = {
    var streamsRewritten = 0L
    var glyphsRecoded    = 0L
    var updated          = document
    var blocking: Option[PdfTransform.Error.VisualRecodingFailed] = None

    selectedPages(acc).foreach { page =>
      if blocking.isEmpty then
        val pageResourceNames = resourceBaseFonts(page, acc).collect {
          case (name, baseFont) if sourceResourceNames.contains(name) => name
        }.toSet
        if pageResourceNames.nonEmpty then
          val fontMaps = sourceMaps.view.filterKeys(pageResourceNames.contains).toMap
          contentRefs(page, acc.objects).foreach { streamNumber =>
            if blocking.isEmpty then
              acc.streams.get(streamNumber).foreach { payload =>
                payload.stream.exec.toEither match
                  case Left(error) => blocking = Some(visualFailure(fromBaseFont, toBaseFont, error.messageWithContext))
                  case Right(bits) =>
                    rewriteStream(fromBaseFont, toBaseFont, bits.toByteArray, fontMaps.keySet, fontMaps, targetEncoder) match
                      case Left(error) => blocking = Some(error)
                      case Right((rewrittenBytes, recoded)) =>
                        if write && (recoded > 0 || rewrittenBytes.length != bits.size.toInt) then
                          updateStream(updated, streamNumber, payload, rewrittenBytes) match
                            case Left(error)  => blocking = Some(visualFailure(fromBaseFont, toBaseFont, error.getMessage))
                            case Right(nextDoc) =>
                              updated = nextDoc
                              streamsRewritten += 1L
                              glyphsRecoded += recoded
                        else if recoded > 0L then
                          glyphsRecoded += recoded
              }
          }
    }

    blocking match
      case Some(error) => Left(error)
      case None        => Right((RewriteStats(streamsRewritten, glyphsRecoded), updated))
  }

  private def visualFailure(
    sourceBaseFont: String,
    targetBaseFont: String,
    reason: String,
    missingCharacters: Chunk[String] = Chunk.empty,
    undecodableOffset: Option[Long] = None
  ): PdfTransform.Error.VisualRecodingFailed =
    PdfTransform.Error.VisualRecodingFailed(sourceBaseFont, targetBaseFont, reason, missingCharacters, undecodableOffset)

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
    fromBaseFont: String,
    toBaseFont: String,
    bytes: Array[Byte],
    sourceResourceNames: Set[String],
    fontMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): Either[PdfTransform.Error.VisualRecodingFailed, (Array[Byte], Long)] = {
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
          rewriteTextToken(fromBaseFont, toBaseFont, tokens(index - 1), activeResource, sourceResourceNames, fontMaps, targetEncoder) match
            case Left(error) => return Left(error)
            case Right(rewritten) =>
              recoded += rewritten.recoded
              out += rewritten.token
              out += tokens(index)
              index += 1
        case ContentToken.Op("TJ") if index >= 1 =>
          tokens(index - 1) match
            case array: ContentToken.Array =>
              rewriteArrayToken(fromBaseFont, toBaseFont, array, activeResource, sourceResourceNames, fontMaps, targetEncoder) match
                case Left(error) => return Left(error)
                case Right(rewritten) =>
                  recoded += rewritten.recoded
                  out += rewritten.token
            case other => out += other
          out += tokens(index)
          index += 1
        case ContentToken.Op("'") if index >= 1 =>
          rewriteTextToken(fromBaseFont, toBaseFont, tokens(index - 1), activeResource, sourceResourceNames, fontMaps, targetEncoder) match
            case Left(error) => return Left(error)
            case Right(rewritten) =>
              recoded += rewritten.recoded
              out += rewritten.token
              out += tokens(index)
              index += 1
        case ContentToken.Op("\"") if index >= 1 =>
          if index >= 4 then
            out += tokens(index - 4)
            out += tokens(index - 3)
            out += tokens(index - 2)
          rewriteTextToken(fromBaseFont, toBaseFont, tokens(index - 1), activeResource, sourceResourceNames, fontMaps, targetEncoder) match
            case Left(error) => return Left(error)
            case Right(rewritten) =>
              recoded += rewritten.recoded
              out += rewritten.token
              out += tokens(index)
              index += 1
        case token =>
          out += token
          index += 1

    Right((ContentOps.render(out.toList), recoded))
  }

  private final case class RewrittenToken(token: ContentToken, recoded: Long)

  private def rewriteTextToken(
    fromBaseFont: String,
    toBaseFont: String,
    token: ContentToken,
    activeResource: Option[String],
    sourceResourceNames: Set[String],
    fontMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): Either[PdfTransform.Error.VisualRecodingFailed, RewrittenToken] =
    activeResource.filter(sourceResourceNames.contains).flatMap(fontMaps.get) match
      case Some(sourceMap) =>
        tokenBytes(token) match
          case Some(sourceBytes) =>
            sourceMap.decodeStrict(ByteVector.view(sourceBytes)).left.map(offset =>
              visualFailure(fromBaseFont, toBaseFont, "source glyph codes could not be decoded", undecodableOffset = Some(offset))
            ).flatMap { text =>
              targetEncoder.encodeStrict(text).left.map(missing =>
                visualFailure(fromBaseFont, toBaseFont, "target font lacks glyphs for extracted text", missing)
              ).map { encoded =>
                if encoded.sameElements(sourceBytes) then RewrittenToken(token, 0L)
                else RewrittenToken(toToken(encoded), text.length.toLong)
              }
            }
          case None => Right(RewrittenToken(token, 0L))
      case None => Right(RewrittenToken(token, 0L))

  private def rewriteArrayToken(
    fromBaseFont: String,
    toBaseFont: String,
    array: ContentToken.Array,
    activeResource: Option[String],
    sourceResourceNames: Set[String],
    fontMaps: Map[String, TextExtract.ToUnicode],
    targetEncoder: TextExtract.ToUnicode.CmapEncoder
  ): Either[PdfTransform.Error.VisualRecodingFailed, RewrittenToken] =
    activeResource.filter(sourceResourceNames.contains).flatMap(fontMaps.get) match
      case Some(sourceMap) =>
        var recoded = 0L
        var error: Option[PdfTransform.Error.VisualRecodingFailed] = None
        val elems = array.elems.map {
          case token @ (ContentToken.Literal(_) | ContentToken.Hex(_)) if error.isEmpty =>
            tokenBytes(token) match
              case Some(sourceBytes) =>
                sourceMap.decodeStrict(ByteVector.view(sourceBytes)) match
                  case Left(offset) =>
                    error = Some(visualFailure(fromBaseFont, toBaseFont, "source glyph codes could not be decoded", undecodableOffset = Some(offset)))
                    token
                  case Right(text) =>
                    targetEncoder.encodeStrict(text) match
                      case Left(missing) =>
                        error = Some(visualFailure(fromBaseFont, toBaseFont, "target font lacks glyphs for extracted text", missing))
                        token
                      case Right(encoded) =>
                        if !encoded.sameElements(sourceBytes) then recoded += text.length.toLong
                        toToken(encoded)
              case None => token
          case other => other
        }
        error match
          case Some(value) => Left(value)
          case None        => Right(RewrittenToken(ContentToken.Array(elems), recoded))
      case None => Right(RewrittenToken(array, 0L))

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

  private def fontDictFor(page: Page, acc: TextExtract.Acc, resourceName: String): Option[Prim.Dict] =
    resourcesFor(page.data, acc.objects)
      .flatMap(_.data.get("Font"))
      .flatMap(dictFor(_, acc.objects))
      .flatMap(_.data.get(resourceName))
      .flatMap(dictFor(_, acc.objects))

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
