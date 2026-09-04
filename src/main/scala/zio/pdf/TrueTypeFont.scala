/*
 * Minimal TrueType table reader for WinAnsi Type1-compatible embedding.
 *
 * Reads `head`, `hhea`, `hmtx`, and `OS/2` so a FontDescriptor and a
 * 32–126 width table can be written. The font program is stored whole;
 * this is not a subsetter or shaper.
 */

package zio.pdf

import java.nio.{ByteBuffer, ByteOrder}

private[pdf] object TrueTypeFont {

  final case class Metrics(
    unitsPerEm: Int,
    xMin: Double,
    yMin: Double,
    xMax: Double,
    yMax: Double,
    ascent: Double,
    descent: Double,
    capHeight: Double,
    widths: Vector[Double]
  )

  def parse(bytes: Array[Byte]): Either[PdfPrep.Error, Metrics] =
    if bytes.length < 12 then Left(PdfPrep.InvalidFont("TrueType font is too small"))
    else
      try
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
        val tables = readTables(buffer)
        for {
          head <- table(tables, bytes, "head")
          hhea <- table(tables, bytes, "hhea")
          hmtx <- table(tables, bytes, "hmtx")
          os2  <- table(tables, bytes, "OS/2").orElse(Right(Array.emptyByteArray))
        } yield {
          val headBuf = ByteBuffer.wrap(head).order(ByteOrder.BIG_ENDIAN)
          val units   = unsignedShort(headBuf, 18).max(1)
          val xMin    = toGlyph(signedShort(headBuf, 36), units)
          val yMin    = toGlyph(signedShort(headBuf, 38), units)
          val xMax    = toGlyph(signedShort(headBuf, 40), units)
          val yMax    = toGlyph(signedShort(headBuf, 42), units)
          val hheaBuf = ByteBuffer.wrap(hhea).order(ByteOrder.BIG_ENDIAN)
          val ascent  = toGlyph(signedShort(hheaBuf, 4), units)
          val descent = toGlyph(signedShort(hheaBuf, 6), units)
          val numberOfHMetrics = unsignedShort(hheaBuf, 34).max(1)
          val advance = readAdvances(hmtx, numberOfHMetrics)
          val fallback = advance.headOption.getOrElse(500.0)
          val widths = (32 to 126).map { code =>
            val glyph = winAnsiGlyph(code)
            if glyph < advance.length then advance(glyph) else fallback
          }.toVector
          val cap =
            if os2.length >= 90 then toGlyph(signedShort(ByteBuffer.wrap(os2).order(ByteOrder.BIG_ENDIAN), 88), units)
            else ascent * 0.7
          Metrics(units, xMin, yMin, xMax, yMax, ascent, descent, cap, widths)
        }
      catch
        case error: Exception =>
          Left(PdfPrep.InvalidFont(s"TrueType parse failed: ${error.getMessage}"))

  private final case class Table(offset: Int, length: Int)

  private def readTables(buffer: ByteBuffer): Map[String, Table] = {
    buffer.position(4)
    val count = buffer.getShort() & 0xffff
    buffer.position(12)
    val tables = scala.collection.mutable.Map.empty[String, Table]
    var index = 0
    while index < count && buffer.remaining() >= 16 do
      val tag    = new String(Array(buffer.get(), buffer.get(), buffer.get(), buffer.get()).map(_.toChar))
      buffer.getInt()
      val offset = buffer.getInt()
      val length = buffer.getInt()
      tables.update(tag, Table(offset, length))
      index += 1
    tables.toMap
  }

  private def table(tables: Map[String, Table], bytes: Array[Byte], name: String): Either[PdfPrep.Error, Array[Byte]] =
    tables.get(name) match {
      case None => Left(PdfPrep.InvalidFont(s"TrueType missing $name table"))
      case Some(Table(offset, length)) =>
        if offset < 0 || length < 0 || offset.toLong + length.toLong > bytes.length then
          Left(PdfPrep.InvalidFont(s"TrueType $name table is out of range"))
        else Right(bytes.slice(offset, offset + length))
    }

  private def readAdvances(hmtx: Array[Byte], numberOfHMetrics: Int): Vector[Double] = {
    val buffer = ByteBuffer.wrap(hmtx).order(ByteOrder.BIG_ENDIAN)
    val count  = math.min(numberOfHMetrics, hmtx.length / 4)
    val out    = Vector.newBuilder[Double]
    var index  = 0
    while index < count do
      out += (buffer.getShort() & 0xffff).toDouble
      buffer.getShort()
      index += 1
    out.result()
  }

  private def unsignedShort(buffer: ByteBuffer, offset: Int): Int =
    buffer.getShort(offset) & 0xffff

  private def signedShort(buffer: ByteBuffer, offset: Int): Int =
    buffer.getShort(offset).toInt

  private def toGlyph(value: Int, unitsPerEm: Int): Double =
    value.toDouble * 1000.0 / unitsPerEm.toDouble

  /** Approximate WinAnsi glyph indices for a simple Latin TrueType. */
  private def winAnsiGlyph(code: Int): Int =
    if code <= 32 then 0 else code - 29
}
