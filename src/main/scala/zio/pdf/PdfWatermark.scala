/*
 * Text watermark stamps painted into page content streams.
 *
 * Uses a standard-14 Helvetica show-string and an extra content stream per
 * selected page. Encrypted filings are rejected; the library does not render
 * images or embed custom fonts.
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

  final case class Text(
    text: String,
    diagonal: Boolean = true,
    fromPage: Int = 1,
    toPage: Option[Int] = None
  )

  def fromBytes(
    bytes: Chunk[Byte],
    stamp: Text,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    if bytes.size.toLong > opts.maxMaterializedDocumentBytes.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, bytes.size.toLong))
    else
      PdfEngine.decode(bytes, opts).flatMap { decoded =>
        ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded)) *>
          ZIO.fromEither(parts(decoded, stamp)).flatMap(writeParts)
      }

  def parts(decoded: Chunk[Decoded], stamp: Text): Either[Error, Chunk[Part[Trailer]]] = {
    val label = stamp.text.trim
    if label.isEmpty then Left(EmptyText)
    else
      val objects = objectMap(decoded)
      val pages   = TextExtract.orderedPageObjectNumbers(decoded)
      if pages.isEmpty then Left(NoPages)
      else
        val last = stamp.toPage.getOrElse(pages.size)
        if stamp.fromPage < 1 || last < stamp.fromPage || last > pages.size then
          Left(InvalidRange(stamp.fromPage, last, pages.size))
        else
          val targets = pages.slice(stamp.fromPage - 1, last).toSet
          val start   = (objects.keys.iterator ++ streamMap(decoded).keys.iterator).maxOption.getOrElse(0L) + 1L
          val fresh   = Fresh(start)
          val extras  = Chunk.newBuilder[Part[Trailer]]
          val updates = scala.collection.mutable.Map.empty[Long, Prim.Dict]
          targets.foreach { pageNumber =>
            objects.get(pageNumber).flatMap(obj => dictAt(obj.data)).foreach { page =>
              val box    = mediaBoxOf(pageNumber, objects).getOrElse((0.0, 0.0, 612.0, 792.0))
              val stream = fresh.next()
              extras += Part.Obj(IndirectObj.stream(stream, Prim.dict(), BitVector(content(label, box, stamp.diagonal))))
              val stamped = addHelvetica(appendContents(page, Prim.Ref(stream, 0)), objects)
              updates.update(pageNumber, stamped)
            }
          }
          Right(rewrite(decoded, updates.toMap, extras.result(), fresh.peek))
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

  private def content(text: String, box: (Double, Double, Double, Double), diagonal: Boolean): Array[Byte] = {
    val (x1, y1, x2, y2) = normalize(box)
    val width            = math.max(x2 - x1, 1.0)
    val height           = math.max(y2 - y1, 1.0)
    val cx               = x1 + width / 2.0
    val cy               = y1 + height / 2.0
    val size             = math.max(18.0, math.min(width, height) * 0.12)
    val estimated        = math.max(size, text.length.toDouble * size * 0.5)
    val angle            = if diagonal then math.atan2(height, width) else 0.0
    val cos              = math.cos(angle)
    val sin              = math.sin(angle)
    val tx               = cx - estimated / 2.0 * cos
    val ty               = cy - estimated / 2.0 * sin
    val body =
      s"""q
         |BT
         |/Helv ${pdfNum(size)} Tf
         |0.72 g
         |${pdfNum(cos)} ${pdfNum(sin)} ${pdfNum(-sin)} ${pdfNum(cos)} ${pdfNum(tx)} ${pdfNum(ty)} Tm
         |${pdfLiteral(text)} Tj
         |ET
         |Q
         |""".stripMargin
    body.getBytes(StandardCharsets.ISO_8859_1)
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

  private def addHelvetica(page: Prim.Dict, objects: Map[Long, Obj]): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val fonts     = resolvedDict(resources, "Font", objects).getOrElse(Prim.dict())
    if fonts.data.contains("Helv") then
      val updatedR = Prim.Dict(resources.data.updated("Font", fonts))
      Prim.Dict(page.data.updated("Resources", updatedR))
    else
      val helv = Prim.dict(
        "Type"     -> Prim.Name("Font"),
        "Subtype"  -> Prim.Name("Type1"),
        "BaseFont" -> Prim.Name("Helvetica")
      )
      val updatedFonts = Prim.Dict(fonts.data.updated("Helv", helv))
      val updatedR     = Prim.Dict(resources.data.updated("Font", updatedFonts))
      Prim.Dict(page.data.updated("Resources", updatedR))
  }

  private def resolvedDict(owner: Prim.Dict, key: String, objects: Map[Long, Obj]): Option[Prim.Dict] =
    owner.data.get(key) match {
      case Some(dict: Prim.Dict)     => Some(dict)
      case Some(Prim.Ref(number, _)) => objects.get(number).flatMap(obj => dictAt(obj.data))
      case _                         => None
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
