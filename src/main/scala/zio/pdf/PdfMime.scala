/*
 * Canonical PDF MIME / Content-Type strings (IANA `application/pdf`).
 *
 * A richer integration with zio-blocks `MediaType` and zio-http `ContentType`
 * can be added later behind explicit library dependencies once those modules
 * are aligned on this project’s classpath.
 */

package zio.pdf

object PdfMime {

  /** IANA media type for Portable Document Format. */
  val mimeType: String = "application/pdf"

  /** Typical `Content-Type` header value for raw PDF bytes (no charset). */
  val contentTypeHeader: String = "application/pdf"
}
