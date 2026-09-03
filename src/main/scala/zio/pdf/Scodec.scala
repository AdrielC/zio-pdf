/*
 * Port of fs2.pdf.Scodec — Attempt helpers shared across the PDF codec layer.
 */

package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.prelude.Validation

object Scodec {

  def attemptEither[A, B](eab: Either[A, B]): Attempt[B] =
    eab match
      case Right(b) => Attempt.successful(b)
      case Left(a)  => Attempt.failure(Err(a.toString))

  def fail[A](message: String): Attempt[A] =
    Attempt.failure(Err(message))

  def attemptNel[A](desc: String)(as: Chunk[A]): Attempt[NonEmptyChunk[A]] =
    NonEmptyChunk.fromIterableOption(as) match
      case Some(nec) => Attempt.successful(nec)
      case None      => Attempt.failure(Err(s"$desc: empty list"))

  def attemptNel[A](desc: String)(as: Iterable[A]): Attempt[NonEmptyChunk[A]] =
    attemptNel(desc)(Chunk.fromIterable(as))

  def validateAttempt[A]: Attempt[A] => Validation[String, A] = {
    case Attempt.Successful(a)  => Validation.succeed(a)
    case Attempt.Failure(cause) => Validation.fail(cause.messageWithContext)
  }

  def stringBytes(s: String): ByteVector =
    ByteVector.view(s.getBytes)

  def stringBits(s: String): BitVector =
    stringBytes(s).bits
}
