package zio.pdf

import zio.blocks.mediatype.*

object PdfMime {

  /** IANA media type for Portable Document Format. */
  val mimeType: MediaType =
    MediaTypes.application.pdf

  /** Typical `Content-Type` header value for raw PDF bytes (no charset). */
  val contentTypeHeader: String =
    mimeType.toString
}
