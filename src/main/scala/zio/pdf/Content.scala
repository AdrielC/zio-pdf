/*
 * Content streams — filter chains + length helpers + Flate recompress.
 */

package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.{BitVector, ByteVector}

/** A potentially still-compressed stream. The first call to `exec`
  * decompresses (filter chain) and memoises the result. */
final class Uncompressed(thunk: () => Attempt[BitVector]) {
  lazy val exec: Attempt[BitVector] = thunk()
}

object Uncompressed {
  def now(value: BitVector): Uncompressed                = new Uncompressed(() => Attempt.successful(value))
  def lazily(thunk: => Attempt[BitVector]): Uncompressed = new Uncompressed(() => thunk)
}

private[pdf] object Content {

  def extractObjectStream(stream: Uncompressed): Prim => Option[Attempt[ObjectStream]] = {
    case Prim.tpe("ObjStm", _) =>
      Some(stream.exec.flatMap(ObjectStream.codec.complete.decode).map(_.value))
    case _ =>
      None
  }

  /**
   * Expand `/Filter` (Name or Array). Image filters stay raw so
   * [[Elements]] can classify DCT/CCITT/JBIG2/JPX payloads.
   */
  def uncompress(stream: BitVector): Prim => Uncompressed = data =>
    FilterDecode.filterNames(data) match {
      case Nil =>
        Uncompressed.now(stream)
      case names if names.forall(FilterDecode.passthrough.contains) =>
        Uncompressed.now(stream)
      case _ =>
        Uncompressed.lazily(FilterDecode.applyChain(stream, data))
    }

  /**
   * True when the stream may be safely expanded, mapped, and rewritten
   * with `/Filter /FlateDecode`. Image / Crypt filters are left alone.
   */
  def mayRewriteFilters(data: Prim): Boolean =
    FilterDecode.filterNames(data).forall(n => !FilterDecode.passthrough.contains(n))

  /**
   * Compress `uncompressed` with Flate and patch `data` to
   * `/Filter /FlateDecode` + numeric `/Length` (drops stale DecodeParms).
   */
  def compressFlate(data: Prim, uncompressed: BitVector): Attempt[(Prim, BitVector)] =
    FlateEncode(uncompressed).map { compressed =>
      val len = Prim.Number(BigDecimal(compressed.bytes.size))
      val dict = data match {
        case Prim.Dict(m) =>
          Prim.Dict(
            m.updated("Filter", Prim.Name("FlateDecode"))
              .updated("Length", len)
              .removed("DecodeParms")
              .removed("DP")
          )
        case _ =>
          Prim.dict("Filter" -> Prim.Name("FlateDecode"), "Length" -> len)
      }
      (dict, compressed)
    }

  /** Numeric `/Length` only — `None` when missing or a [[Prim.Ref]]. */
  def streamLengthOpt(dict: Prim): Option[Long] =
    Prim.tryDict("Length")(dict).collect { case Prim.Number(n) => n.toLong }

  def streamLengthRef(dict: Prim): Option[Prim.Ref] =
    Prim.tryDict("Length")(dict).collect { case r @ Prim.Ref(_, _) => r }

  def streamLength(dict: Prim): Attempt[Long] =
    streamLengthOpt(dict) match {
      case Some(n) => Attempt.successful(n)
      case None =>
        streamLengthRef(dict) match {
          case Some(r) =>
            Attempt.failure(Err(s"stream /Length is indirect ref $r (resolve or scan endstream)"))
          case None =>
            Attempt.failure(Err(s"key `Length` not present in $dict"))
        }
    }

  val endstream: ByteVector = ByteVector("endstream".getBytes)

  def endstreamIndex(bytes: ByteVector): Attempt[Long] =
    bytes.indexOfSlice(endstream) match {
      case i if i >= 0 => Attempt.successful(i)
      case _           => Attempt.failure(Err.InsufficientBits(0, bytes.bits.size, List("no stream end position found")))
    }
}
