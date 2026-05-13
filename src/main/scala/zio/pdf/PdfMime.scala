/*
 * Canonical PDF media type via zio-blocks [[MediaType]] and zio-http-model
 * [[ContentType]] (HTTP Content-Type header shape).
 *
 * `ContentType` lives in an `@experimental` module; this object is marked
 * accordingly so callers can opt in by referencing [[PdfMime]] from
 * experimental code or by enabling `-experimental` in their build.
 */

package zio.pdf

import scala.annotation.experimental

import zio.blocks.mediatype.MediaTypes
import zio.http.ContentType

@experimental
object PdfMime {

  /** IANA type for Portable Document Format. */
  val mediaType: zio.blocks.mediatype.MediaType =
    MediaTypes.application.`pdf`

  /** For headers / metadata that expect `ContentType` (media type + optional charset/boundary). */
  val contentType: ContentType =
    ContentType(mediaType)
}
