/*
 * Port of fs2.pdf.Version to Scala 3 + scodec 2.3.
 *
 * The legacy implementation used a shapeless `HList` to splice the
 * three components together. scodec 2.x on Scala 3 uses native Scala
 * tuples instead, and `Codec.as[T]` derives an `Iso` from the case
 * class's `Mirror`.
 */

package zio.pdf

import java.nio.charset.StandardCharsets

import zio.pdf.codec.{Codecs, Text, Whitespace}
import _root_.scodec.Codec
import _root_.scodec.bits.ByteVector

final case class Version(major: Int, minor: Int, binaryMarker: Option[ByteVector])

object Version {

  import Text.{ascii, char, line, str}

  /**
   * Trailing whitespace after the version line. The legacy
   * `Whitespace.whitespaceAndCommentAsNewline` recursed into
   * `Comment.many` which made it impossible to use this codec from
   * inside `Comment` itself; here we just consume any whitespace.
   */
  private val nlWs: Codec[Unit] = Whitespace.skipWs

  /** `%PDF-x.y` followed by trailing whitespace. */
  val versionLine: Codec[(Int, Int)] =
    (str("%PDF-") ~> ascii.int) :: (char('.') ~> ascii.int <~ nlWs)

  /** Optional binary marker: a `%` followed by a line of binary bytes. */
  val binaryMarker: Codec[Option[ByteVector]] =
    Codecs.opt(using char('%') ~> line("binary marker"))

  /** A full version header: `%PDF-x.y` and an optional binary marker. */
  given codec: Codec[Version] =
    (versionLine :: binaryMarker)
      .xmap(
        { case ((major, minor), bm) => Version(major, minor, bm) },
        v => ((v.major, v.minor), v.binaryMarker)
      )

  val default: Version =
    Version(1, 7, Some(ByteVector("âãÏÓ".getBytes(StandardCharsets.ISO_8859_1))))
}
