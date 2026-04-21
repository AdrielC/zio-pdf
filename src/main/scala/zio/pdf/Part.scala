/*
 * Helper data type for encoding streams of indirect objects, with
 * an interleaved Meta variant carrying trailer information and an
 * optional Version override.
 */

package zio.pdf

sealed trait Part[+A]

object Part {
  final case class Obj(obj: IndirectObj)        extends Part[Nothing]
  final case class Meta[A](meta: A)             extends Part[A]
  final case class Version(version: zio.pdf.Version) extends Part[Nothing]
}
