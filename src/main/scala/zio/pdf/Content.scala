/*
 * Port of fs2.pdf.Content to Scala 3 + scodec 2.3.
 *
 * Represents a (possibly compressed) content stream. The legacy
 * version used `cats.Eval` for memoised lazy evaluation; we use
 * Scala 3 `lazy val` directly which serves the same purpose.
 */

package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.{BitVector, ByteVector}

/** A potentially still-compressed stream. The first call to `exec`
  * decompresses (FlateDecode + optional Predictor) and memoises the
  * result. */
final class Uncompressed(thunk: () => Attempt[BitVector]) {
  lazy val exec: Attempt[BitVector] = thunk()
}

object Uncompressed {
  def now(value: BitVector): Uncompressed = new Uncompressed(() => Attempt.successful(value))
  def lazily(thunk: => Attempt[BitVector]): Uncompressed = new Uncompressed(() => thunk)
}

private[pdf] object Content {

  def extractObjectStream(stream: Uncompressed): Prim => Option[Attempt[ObjectStream]] = {
    case Prim.tpe("ObjStm", _) =>
      Some(stream.exec.flatMap(ObjectStream.codec.complete.decode).map(_.value))
    case _ =>
      None
  }

  def uncompress(stream: BitVector): Prim => Uncompressed = {
    case Prim.filter("FlateDecode", data) =>
      Uncompressed.lazily(FlateDecode(stream, data))
    case _ =>
      Uncompressed.now(stream)
  }

  def streamLength(dict: Prim): Attempt[Long] =
    Prim.Dict.number("Length")(dict).map(_.toLong)

  val endstream: ByteVector = ByteVector("endstream".getBytes)

  def endstreamIndex(bytes: ByteVector): Attempt[Long] =
    bytes.indexOfSlice(endstream) match {
      case i if i >= 0 => Attempt.successful(i)
      case _           => Attempt.failure(Err.InsufficientBits(0, bytes.bits.size, List("no stream end position found")))
    }
}
