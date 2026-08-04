/*
 * Simple literal text extraction from page content streams.
 * No ToUnicode / CMap — ISO-8859-1 from Tj / TJ / ' / ".
 */

package zio.pdf

import scala.collection.mutable.ArrayBuffer

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.pdf.content.{ContentOps, ContentToken}

final case class PageText(pageNumber: Long, text: String)

object TextExtract {

  /** Fold state — content payloads + pages only (not a full [[Element]] timeline). */
  final case class Acc(
    streams: scala.collection.mutable.LongMap[BitVector] = scala.collection.mutable.LongMap.empty,
    pages: ArrayBuffer[Page] = ArrayBuffer.empty
  )

  def fold(acc: Acc, el: Element): Acc =
    el match {
      case Element.Content(obj, raw, stream, _) =>
        val bits = stream.exec match {
          case Attempt.Successful(b) => b
          case Attempt.Failure(_)    => raw
        }
        acc.streams.update(obj.index.number, bits)
        acc
      case Element.Data(_, Element.DataKind.Page(p)) =>
        acc.pages += p
        acc
      case _ => acc
    }

  def finish(acc: Acc): Chunk[PageText] =
    Chunk.fromArray {
      acc.pages.iterator.map { page =>
        PageText(page.index.number, extractFromBytes(contentBytes(page, acc.streams)))
      }.toArray
    }

  def fromElements(elements: Chunk[Element]): Chunk[PageText] = {
    var acc = Acc()
    val it  = elements.iterator
    while it.hasNext do acc = fold(acc, it.next())
    finish(acc)
  }

  def extractFromBytes(bytes: Array[Byte]): String = {
    val tokens = ContentOps.tokenize(bytes)
    val out    = new StringBuilder
    var i      = 0
    def emit(s: String): Unit =
      if s.nonEmpty then
        if out.nonEmpty && !out.last.isWhitespace && !s.head.isWhitespace then out.append(' ')
        out.append(s)

    def newline(): Unit =
      if out.nonEmpty && out.last != '\n' then out.append('\n')

    while i < tokens.length do
      tokens(i) match {
        case ContentToken.Op("Tj") if i >= 1 =>
          ContentOps.tokenText(tokens(i - 1)).foreach(emit)
          i += 1
        case ContentToken.Op("TJ") if i >= 1 =>
          tokens(i - 1) match {
            case a: ContentToken.Array => emit(ContentOps.tjArrayText(a))
            case _                     => ()
          }
          i += 1
        case ContentToken.Op("'") if i >= 1 =>
          newline()
          ContentOps.tokenText(tokens(i - 1)).foreach(emit)
          i += 1
        case ContentToken.Op("\"") if i >= 1 =>
          newline()
          ContentOps.tokenText(tokens(i - 1)).foreach(emit)
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

  private def contentBytes(page: Page, streams: scala.collection.mutable.LongMap[BitVector]): Array[Byte] = {
    val refs: List[Long] = page.data.data.get("Contents") match {
      case Some(Prim.Ref(n, _)) => List(n)
      case Some(Prim.Array(es)) => es.iterator.collect { case Prim.Ref(n, _) => n }.toList
      case _                    => Nil
    }
    val buf = ArrayBuffer.empty[Byte]
    refs.foreach { n =>
      streams.get(n).foreach { bits =>
        buf ++= bits.toByteArray
        buf += '\n'.toByte
      }
    }
    buf.toArray
  }
}
