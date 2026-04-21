/*
 * Port of fs2.pdf.FlateDecode + PredictorTransform to Scala 3 +
 * scodec 2.3. The original used cats.effect IO + java.io streams
 * to call into the Java predictor; here we just call the Java
 * helper directly with `ByteArray{In,Out}putStream`. There's no
 * effect to manage - it's a pure byte-array transform.
 */

package zio.pdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import _root_.scodec.{Attempt, DecodeResult, Err}
import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.{bits, zlib}

private[pdf] object PredictorTransform {

  def param(key: String, default: Int)(params: Prim): Int =
    Prim.Dict.number(key)(params).map(_.toInt).getOrElse(default)

  def apply(stream: BitVector, predictor: BigDecimal, params: Prim): Attempt[BitVector] = {
    val is  = new ByteArrayInputStream(stream.toByteArray)
    val os  = new ByteArrayOutputStream()
    try {
      zio.pdf.image.Predictor.decodePredictor(
        predictor.toInt,
        param("Colors", 1)(params),
        param("BitsPerComponent", 8)(params),
        param("Columns", 1)(params),
        is,
        os
      )
      Attempt.successful(BitVector(os.toByteArray))
    } catch {
      case t: Throwable =>
        Attempt.failure(Err(s"PredictorTransform failed: ${t.getMessage}"))
    }
  }
}

private[pdf] object FlateDecode {

  def handlePredictor(stream: BitVector, params: Prim.Dict): Option[Attempt[BitVector]] =
    Prim.path("Predictor")(params) {
      case Prim.Number(predictor) if predictor > 1 =>
        PredictorTransform(stream, predictor, params)
    }

  def handleParams(stream: BitVector, data: Prim): Attempt[BitVector] =
    Prim
      .path("DecodeParms")(data) { case params @ Prim.Dict(_) =>
        handlePredictor(stream, params)
      }
      .flatten
      .getOrElse(Attempt.successful(stream))

  def apply(stream: BitVector, data: Prim): Attempt[BitVector] =
    zlib(bits).decode(stream).flatMap { case DecodeResult(inflated, _) =>
      handleParams(inflated, data)
    }
}
