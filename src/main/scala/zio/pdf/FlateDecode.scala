/*
 * Port of fs2.pdf.FlateDecode + PredictorTransform to Scala 3 +
 * scodec 2.3. The original used cats.effect IO + java.io streams
 * to call into the Java predictor; here we just call the Java
 * helper directly with `ByteArray{In,Out}putStream`. There's no
 * effect to manage - it's a pure byte-array transform.
 */

package zio.pdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.InflaterInputStream

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

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

  def apply(
    stream: BitVector,
    data: Prim,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[BitVector] =
    val input  = new InflaterInputStream(new ByteArrayInputStream(stream.toByteArray))
    val output = new ByteArrayOutputStream(math.min(stream.bytes.size.toInt.max(32), maxOutputBytes.bytes))
    val buffer = new Array[Byte](8192)
    var total  = 0L
    try
      var read = input.read(buffer)
      while read >= 0 do
        if read > 0 then
          total += read.toLong
          if total > maxOutputBytes.toLong then
            return Attempt.failure(
              Err(
                FilterDecode.OutputLimitExceeded("FlateDecode", maxOutputBytes.bytes, total).getMessage
              )
            )
          output.write(buffer, 0, read)
        read = input.read(buffer)
      handleParams(BitVector(output.toByteArray), data)
    catch
      case error: Throwable => Attempt.failure(Err(s"FlateDecode: ${error.getMessage}"))
    finally input.close()
}
