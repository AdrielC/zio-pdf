/*
 * Native text extraction from page content streams.
 *
 * PDF text show operators carry glyph codes, not necessarily Unicode bytes.
 * We resolve each selected page font's `/ToUnicode` CMap before falling back
 * to ISO-8859-1, which keeps a legitimate encoded text layer distinct from
 * an image-only page.
 */

package zio.pdf

import java.nio.charset.StandardCharsets

import scala.collection.mutable.{ArrayBuffer, LongMap}

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.pdf.content.{ContentOps, ContentToken}

final case class PageText(pageNumber: Long, text: String)

object TextExtract {

  /**
   * Fold state — object metadata plus lazy stream payloads. This intentionally
   * avoids retaining the full Element timeline while still allowing the final
   * page tree to select the document's live pages after incremental updates.
   */
  final case class Acc(
    objects: LongMap[Prim] = LongMap.empty,
    streams: LongMap[ContentPayload] = LongMap.empty,
    pages: ArrayBuffer[Page] = ArrayBuffer.empty,
    pagesByIndex: LongMap[Page] = LongMap.empty,
    trailer: Option[Trailer] = None
  )

  final case class ContentPayload(data: Prim, raw: BitVector, stream: Uncompressed)

  def fold(acc: Acc, el: Element): Acc =
    el match {
      case Element.Content(obj, raw, stream, _) =>
        // Deferring expansion means we inflate only page content and the
        // handful of ToUnicode CMaps selected by those pages.
        acc.streams.update(
          obj.index.number,
          ContentPayload(obj.data, raw, stream)
        )
        acc
      case Element.Data(obj, Element.DataKind.Page(page)) =>
        acc.objects.update(obj.index.number, obj.data)
        acc.pages += page
        acc.pagesByIndex.update(obj.index.number, page)
        acc
      case Element.Data(obj, _) =>
        acc.objects.update(obj.index.number, obj.data)
        acc
      case Element.Meta(trailer, _) =>
        acc.copy(trailer = trailer.orElse(acc.trailer))
    }

  def finish(acc: Acc): Chunk[PageText] =
    val pages = Chunk.newBuilder[PageText]
    foreachPage(acc) { (pageObjectNumber, _, text) =>
      pages += PageText(pageObjectNumber, text)
    }
    pages.result()

  /**
   * Visit logical pages in document order without retaining their extracted
   * text. This is the bounded bridge used by [[PdfEvidence]]: a browser
   * summary can count and preview text without first building one giant
   * `Chunk[PageText]` for a large document.
   */
  private[pdf] def foreachPage(acc: Acc)(consume: (Long, Chunk[Long], String) => Unit): Unit =
    selectedPages(acc).foreach { page =>
      val refs = contentRefs(page, acc.objects)
      consume(
        page.index.number,
        Chunk.fromIterable(refs),
        extractFromBytes(contentBytes(refs, acc.streams), fontMaps(page, acc))
      )
    }

  /** Page object numbers in document order (handles nested `/Pages` trees). */
  private[pdf] def orderedPageObjectNumbers(decoded: Chunk[Decoded]): List[Long] = {
    val acc = Elements.foldSync(decoded).foldLeft(Acc())(fold)
    selectedPages(acc).map(_.index.number).toList
  }

  def fromElements(elements: Chunk[Element]): Chunk[PageText] = {
    var acc = Acc()
    val it  = elements.iterator
    while it.hasNext do acc = fold(acc, it.next())

    finish(acc)
  }

  def extractFromBytes(bytes: Array[Byte], fonts: Map[String, ToUnicode] = Map.empty): String = {
    // The scanner below needs indexed look-behind for PDF operators. Keep an
    // array here: `List.length` in the loop turns a long page stream into an
    // accidental quadratic traversal.
    val tokens = ContentOps.tokenize(bytes).toArray
    val out    = new StringBuilder
    var i      = 0
    var activeFont: Option[ToUnicode] = None
    def emit(s: String): Unit =
      if s.nonEmpty then out.append(s)

    def newline(): Unit =
      if out.nonEmpty && out.last != '\n' then out.append('\n')

    while i < tokens.length do
      tokens(i) match {
        case ContentToken.Op("Tf") if i >= 2 =>
          tokens(i - 2) match {
            case ContentToken.Name(name) => activeFont = fonts.get(name)
            case _                       => ()
          }
          i += 1
        case ContentToken.Op("Tj") if i >= 1 =>
          tokenText(tokens(i - 1), activeFont).foreach(emit)
          i += 1
        case ContentToken.Op("TJ") if i >= 1 =>
          tokens(i - 1) match {
            case a: ContentToken.Array => emit(arrayText(a, activeFont))
            case _                     => ()
          }
          i += 1
        case ContentToken.Op("'") if i >= 1 =>
          newline()
          tokenText(tokens(i - 1), activeFont).foreach(emit)
          i += 1
        case ContentToken.Op("\"") if i >= 1 =>
          newline()
          tokenText(tokens(i - 1), activeFont).foreach(emit)
          i += 1
        case ContentToken.Op("T*") =>
          newline()
          i += 1
        case ContentToken.Op("Td") | ContentToken.Op("TD") =>
          if i >= 2 then
            (tokens(i - 2), tokens(i - 1)) match {
              case (ContentToken.Number(_), ContentToken.Number(ty)) if ty != 0 =>
                newline()
              case _ => ()
            }
          i += 1
        case ContentToken.Op("Tm") =>
          newline()
          i += 1
        case _ =>
          i += 1
      }
    out.toString.trim
  }

  def extractFromBits(bits: BitVector): String =
    extractFromBytes(bits.toByteArray)

  private def contentBytes(refs: List[Long], streams: LongMap[ContentPayload]): Array[Byte] = {
    val buf = ArrayBuffer.empty[Byte]
    refs.foreach { n =>
      streams.get(n).foreach { payload =>
        val bits = payload.stream.exec match {
          case Attempt.Successful(value) => value
          case Attempt.Failure(_)        => payload.raw
        }
        buf ++= bits.toByteArray
        buf += '\n'.toByte
      }
    }
    buf.toArray
  }

  /**
   * `/Contents` may directly name a stream, directly contain an array of
   * stream references, or refer to an indirect array. The last form is common
   * in producer output and must be resolved before looking in `streams`.
   */
  private def contentRefs(page: Page, objects: LongMap[Prim]): List[Long] = {
    def resolve(value: Prim, visited: Set[Long]): List[Long] =
      value match {
        case Prim.Ref(number, _) if !visited(number) =>
          objects.get(number) match {
            case Some(indirect) => resolve(indirect, visited + number)
            case None           => List(number)
          }
        case Prim.Array(values) => values.iterator.flatMap(resolve(_, visited)).toList
        case _                  => Nil
      }

    page.data.data.get("Contents").toList.flatMap(resolve(_, Set.empty))
  }

  /**
   * The physical decode stream may contain stale page revisions. Follow the
   * trailer's catalog and `/Pages` tree so a document yields its logical page
   * order exactly once. A syntactically incomplete tree falls back to the
   * conservative event order rather than discarding recoverable text.
   */
  private def selectedPages(acc: Acc): Seq[Page] = {
    val tree = for {
      trailer <- acc.trailer
      root <- trailer.root
    } yield pageTree(root.number, acc, Set.empty)

    tree.filter(_.nonEmpty).getOrElse(acc.pages.toSeq)
  }

  private def pageTree(number: Long, acc: Acc, visited: Set[Long]): List[Page] =
    if visited(number) then Nil
    else
      acc.objects.get(number) match {
        case Some(dict: Prim.Dict) if dict.data.get("Type").contains(Prim.Name("Catalog")) =>
          dict.data
            .get("Pages")
            .toList
            .flatMap {
              case Prim.Ref(pages, _) => pageTree(pages, acc, visited + number)
              case _                  => Nil
            }
        case Some(dict: Prim.Dict) if dict.data.get("Type").contains(Prim.Name("Page")) =>
          acc.pagesByIndex.get(number).toList
        case Some(dict: Prim.Dict) if dict.data.get("Type").contains(Prim.Name("Pages")) =>
          dict.data
            .get("Kids")
            .toList
            .flatMap(indirectArray(_, acc.objects, visited))
            .flatMap {
              case Prim.Ref(child, _) => pageTree(child, acc, visited + number)
              case _                  => Nil
            }
        case _ => Nil
      }

  private def indirectArray(
    value: Prim,
    objects: LongMap[Prim],
    visited: Set[Long]
  ): List[Prim] =
    value match {
      case Prim.Array(values) => values.toList
      case Prim.Ref(number, _) if !visited(number) =>
        objects.get(number).toList.flatMap(indirectArray(_, objects, visited + number))
      case _ => Nil
    }

  private def tokenText(token: ContentToken, font: Option[ToUnicode]): Option[String] =
    token match {
      case ContentToken.Literal(bytes) => Some(decode(bytes, font))
      case ContentToken.Hex(bytes)     => Some(decode(bytes, font))
      case _                           => None
    }

  private def arrayText(array: ContentToken.Array, font: Option[ToUnicode]): String =
    val out = new StringBuilder
    var wordGap = false
    array.elems.foreach {
      case ContentToken.Number(adjustment) if adjustment < -120 =>
        // In a TJ array, a sufficiently negative adjustment moves the next
        // glyph farther right and conventionally represents a word break.
        wordGap = true
      case token =>
        tokenText(token, font).foreach { text =>
          if wordGap && out.nonEmpty && text.nonEmpty && !out.last.isWhitespace && !text.head.isWhitespace then
            out.append(' ')
          out.append(text)
          wordGap = false
        }
    }
    out.toString

  private def decode(bytes: _root_.scodec.bits.ByteVector, font: Option[ToUnicode]): String =
    font.fold(new String(bytes.toArray, StandardCharsets.ISO_8859_1))(_.decode(bytes))

  private def fontMaps(page: Page, acc: Acc): Map[String, ToUnicode] =
    resourcesFor(page.data, acc.objects)
      .flatMap(_.data.get("Font"))
      .flatMap(dictFor(_, acc.objects))
      .fold(Map.empty[String, ToUnicode]) { fonts =>
        fonts.data.iterator.flatMap { case (name, font) =>
          for
            fontDict <- dictFor(font, acc.objects)
            cmapRef <- fontDict.data.get("ToUnicode").collect { case ref: Prim.Ref => ref }
            payload <- acc.streams.get(cmapRef.number)
            bytes <- payload.stream.exec.toOption
            cmap <- ToUnicode.parse(bytes)
          yield name -> cmap
        }.toMap
      }

  private def resourcesFor(page: Prim.Dict, objects: LongMap[Prim]): Option[Prim.Dict] =
    def loop(data: Prim.Dict, visited: Set[Long]): Option[Prim.Dict] =
      data.data.get("Resources").flatMap(dictFor(_, objects)).orElse {
        data.data.get("Parent") match {
          case Some(Prim.Ref(number, _)) if !visited(number) =>
            objects.get(number).collect { case parent: Prim.Dict => parent }.flatMap(loop(_, visited + number))
          case _ => None
        }
      }
    loop(page, Set.empty)

  private def dictFor(value: Prim, objects: LongMap[Prim]): Option[Prim.Dict] =
    value match {
      case dict: Prim.Dict => Some(dict)
      case Prim.Ref(number, _) => objects.get(number).collect { case dict: Prim.Dict => dict }
      case _ => None
    }

  /** Parsed `/ToUnicode` CMap, indexed by source code width for a hot decode loop. */
  final case class ToUnicode private (byWidth: Map[Int, Map[Long, String]]) {
    private val widths = byWidth.keys.toArray.sorted(using Ordering.Int.reverse)

    def decode(bytes: _root_.scodec.bits.ByteVector): String = {
      val out = new StringBuilder
      var offset = 0L
      while offset < bytes.size do
        var matched = false
        var widthIndex = 0
        while widthIndex < widths.length && !matched do
          val width = widths(widthIndex)
          if offset + width <= bytes.size then {
            val code = sourceCode(bytes, offset, width)
            byWidth(width).get(code).foreach { value =>
              out.append(value)
              offset += width
              matched = true
            }
          }
          widthIndex += 1
        if !matched then {
          out.append((bytes(offset) & 0xff).toChar)
          offset += 1L
        }
      out.toString
    }
  }

  object ToUnicode {
    private val bfCharBlocks = "(?is)(?:\\d+\\s+)?beginbfchar\\s*(.*?)\\s*endbfchar".r
    private val bfRangeBlocks = "(?is)(?:\\d+\\s+)?beginbfrange\\s*(.*?)\\s*endbfrange".r
    private val pair = "(?is)<([0-9a-f]+)>\\s*<([0-9a-f]+)>".r
    private val range = "(?is)<([0-9a-f]+)>\\s+<([0-9a-f]+)>\\s+(?:<([0-9a-f]+)>|\\[([^]]*)\\])".r
    private val hex = "(?is)<([0-9a-f]+)>".r

    def parse(bits: BitVector): Option[ToUnicode] = {
      val source = new String(bits.toByteArray, StandardCharsets.ISO_8859_1)
      val entries = scala.collection.mutable.Map.empty[Int, scala.collection.mutable.Map[Long, String]]

      def add(raw: String, value: String): Unit =
        bytes(raw).foreach { sourceBytes =>
          if sourceBytes.nonEmpty && sourceBytes.length <= 8 then
            entries.getOrElseUpdate(sourceBytes.length, scala.collection.mutable.Map.empty).update(
              sourceCode(sourceBytes),
              value
            )
        }

      bfCharBlocks.findAllMatchIn(source).foreach { block =>
        pair.findAllMatchIn(block.group(1)).foreach { entry =>
          unicode(entry.group(2)).foreach(add(entry.group(1), _))
        }
      }

      bfRangeBlocks.findAllMatchIn(source).foreach { block =>
        range.findAllMatchIn(block.group(1)).foreach { entry =>
          (bytes(entry.group(1)), bytes(entry.group(2))) match {
            case (Some(first), Some(last)) if first.length == last.length && first.length <= 8 =>
              val start = sourceCode(first)
              val end = sourceCode(last)
              Option(entry.group(3)) match {
                case Some(destination) =>
                  unicode(destination).flatMap(firstCodePoint).foreach { initial =>
                    var code = start
                    while code <= end do
                      add(sourceHex(code, first.length), new String(Character.toChars(initial + (code - start).toInt)))
                      code += 1L
                  }
                case None =>
                  val destinations = hex.findAllMatchIn(Option(entry.group(4)).getOrElse(""))
                  var code = start
                  destinations.foreach { destination =>
                    if code <= end then unicode(destination.group(1)).foreach(add(sourceHex(code, first.length), _))
                    code += 1L
                  }
              }
            case _ => ()
          }
        }
      }

      val frozen = entries.iterator.map { case (width, values) => width -> values.toMap }.toMap
      Option.when(frozen.nonEmpty)(ToUnicode(frozen))
    }

    private def bytes(value: String): Option[Array[Byte]] =
      if value.length % 2 != 0 then None
      else
        try Some(value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray)
        catch { case _: NumberFormatException => None }

    private def unicode(value: String): Option[String] =
      bytes(value).filter(_.length % 2 == 0).map(new String(_, StandardCharsets.UTF_16BE))

    private def firstCodePoint(value: String): Option[Int] =
      Option.when(value.nonEmpty)(value.codePointAt(0))
  }

  private def sourceCode(bytes: Array[Byte]): Long =
    sourceCode(_root_.scodec.bits.ByteVector(bytes), 0L, bytes.length)

  private def sourceCode(bytes: _root_.scodec.bits.ByteVector, offset: Long, width: Int): Long = {
    var code = 0L
    var index = 0
    while index < width do
      code = (code << 8) | (bytes(offset + index) & 0xff).toLong
      index += 1
    code
  }

  private def sourceHex(code: Long, width: Int): String =
    (0 until width).reverse.map { index => f"${(code >>> (index * 8)) & 0xff}%02X" }.mkString
}
