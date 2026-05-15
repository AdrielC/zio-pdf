/*
 * Port of fs2.pdf.Tiff — minimal TIFF header builder + strip decoder
 * for CCITT test fixtures (used with javax.imageio).
 */

package zio.pdf

import java.awt.image.BufferedImage
import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

import _root_.scodec.bits.{BitVector, ByteOrdering, ByteVector}
import zio.*

final case class Tiff(width: Long, height: Long, group: Long, blackIsZero: Long)

object Tiff {
  private val le = ByteOrdering.LittleEndian

  def short(a: Short): ByteVector =
    ByteVector.fromShort(a, size = 2, ordering = le)

  def long(a: Long): ByteVector =
    ByteVector.fromLong(a, size = 4, ordering = le)

  def ssll(a1: Short, a2: Short, a3: Long, a4: Long): ByteVector =
    short(a1) ++ short(a2) ++ long(a3) ++ long(a4)

  val headerSize: Int = 108

  def header(params: Tiff, size: Long): ByteVector =
    ByteVector.fromByte('I') ++
      ByteVector.fromByte('I') ++
      short(42) ++
      long(8) ++
      short(8) ++
      ssll(256, 4, 1, params.width) ++
      ssll(257, 4, 1, params.height) ++
      ssll(258, 3, 1, 1) ++
      ssll(259, 3, 1, params.group) ++
      ssll(262, 3, 1, params.blackIsZero) ++
      ssll(273, 4, 1, headerSize) ++
      ssll(278, 4, 1, params.height) ++
      ssll(279, 4, 1, size) ++
      short(0)

  def fromData(params: Tiff)(data: ByteVector): ByteVector =
    header(params, data.size) ++ data

  def image(params: Tiff)(data: ByteVector): Task[BufferedImage] =
    ZIO.attempt(ImageIO.read(new ByteArrayInputStream(fromData(params)(data).toArray)))

  /** Strip the fixed-size baseline TIFF header produced by [[header]], leaving CCITT payload bits. */
  def raw(data: BitVector): BitVector =
    data.drop(headerSize * 8L)
}
