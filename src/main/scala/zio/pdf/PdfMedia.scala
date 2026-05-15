/*
 * Media-type helpers for PDF inputs, first-page text output, image
 * payloads, and embedded-file stream metadata.
 */

package zio.pdf

import zio.blocks.mediatype.*

object PdfMedia {

  val pdf: MediaType       = PdfMime.mimeType
  val textPlain: MediaType = PdfMime.textPlain
  val png: MediaType       = PdfMime.imagePng

  def fromExtension(ext: String): Option[MediaType] =
    MediaType.forFileExtension(ext)

  def fromFileName(name: String): Option[MediaType] =
    name.lastIndexOf('.') match {
      case i if i >= 0 && i + 1 < name.length => fromExtension(name.substring(i + 1))
      case _                                  => None
    }

  def parseContentType(value: String): Either[String, MediaType] =
    MediaType.parse(value)

  def fromEmbeddedFileSubtype(dict: Prim.Dict): Option[MediaType] =
    dict("Subtype").collect { case Prim.Name(raw) => raw }
      .flatMap(pdfNameHexDecode)
      .flatMap(parseContentType(_).toOption)

  private[pdf] def pdfNameHexDecode(value: String): Option[String] = {
    val out = new StringBuilder(value.length)
    var i   = 0
    var ok  = true
    while (ok && i < value.length) {
      val c = value.charAt(i)
      if (c == '#') {
        if (i + 2 >= value.length) ok = false
        else {
          val hi = hexValue(value.charAt(i + 1))
          val lo = hexValue(value.charAt(i + 2))
          (hi, lo) match {
            case (Some(h), Some(l)) =>
              out.append(((h << 4) | l).toChar)
              i += 3
            case _ =>
              ok = false
          }
        }
      } else {
        out.append(c)
        i += 1
      }
    }
    Option.when(ok)(out.result())
  }

  private def hexValue(c: Char): Option[Int] =
    if (c >= '0' && c <= '9') Some(c - '0')
    else if (c >= 'a' && c <= 'f') Some(c - 'a' + 10)
    else if (c >= 'A' && c <= 'F') Some(c - 'A' + 10)
    else None
}
