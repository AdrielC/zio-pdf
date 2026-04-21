/*
 * Port of fs2.pdf.codec.Many to Scala 3 + scodec 2.3.
 *
 * scodec 2.x dropped the `Codec.listMultiplexed` builder used by the
 * legacy `till` family. The same semantics are re-implemented below
 * using a tail-recursive decoder driven by `optional(recover(end), main)`.
 */

package zio.pdf.codec

import _root_.scodec.{Attempt, Codec, DecodeResult, Decoder}
import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.*

private[pdf] object Many {

  /**
   * Decode as many `A` values as possible until either input is
   * exhausted or `end` succeeds at the head of the buffer.
   */
  def till[A](end: BitVector => Boolean)(main: Codec[A]): Codec[List[A]] = {
    @annotation.tailrec
    def spin(acc: List[A], bits: BitVector): Attempt[DecodeResult[List[A]]] =
      if (bits.isEmpty || end(bits)) Attempt.successful(DecodeResult(acc.reverse, bits))
      else
        main.decode(bits) match {
          case Attempt.Successful(DecodeResult(a, rem)) => spin(a :: acc, rem)
          case Attempt.Failure(cause)                   => Attempt.failure(cause)
        }
    Codec(list(main), Decoder(spin(Nil, _))).withContext("manyTill")
  }

  def tillDecodes[A](end: Codec[Unit]): Codec[A] => Codec[List[A]] =
    till(end.decode(_).isSuccessful)

  def bracket[A](start: Codec[Unit], end: Codec[Unit])(main: Codec[A]): Codec[List[A]] =
    Codecs.bracket(start, end)(tillDecodes(end)(main))

  /**
   * Decode zero-or-more `A`s by trying `indicator` first; if the
   * indicator does not match, decoding stops cleanly with what we
   * have so far.
   */
  def apply[A](indicator: Codec[Unit], target: Codec[A]): Codec[List[A]] = {
    def one: Decoder[Option[A]] =
      optional(recover(indicator), target)

    @annotation.tailrec
    def spin(acc: List[A])(bits: BitVector): Attempt[DecodeResult[List[A]]] =
      one.decode(bits) match {
        case Attempt.Successful(DecodeResult(None, remainder)) =>
          Attempt.successful(DecodeResult(acc, remainder))
        case Attempt.Successful(DecodeResult(Some(a), remainder)) =>
          spin(a :: acc)(remainder)
        case Attempt.Failure(cause) =>
          Attempt.failure(cause)
      }

    Codec(list(target), Decoder(spin(Nil)).map(_.reverse))
  }
}
