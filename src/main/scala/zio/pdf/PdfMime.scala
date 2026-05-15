package zio.pdf

import zio.blocks.mediatype.*

object PdfMime {

  /** IANA media type for Portable Document Format. */
  val mimeType: MediaType =
    MediaTypes.application.pdf

  /** Plain-text media type used by text extraction pipelines. */
  val textPlain: MediaType =
    MediaTypes.text.plain

  /** PNG media type used by first-page render/slice test oracles. */
  val imagePng: MediaType =
    MediaTypes.image.png

  /** Typical `Content-Type` header value for raw PDF bytes (no charset). */
  val contentTypeHeader: String =
    mimeType.toString
}
