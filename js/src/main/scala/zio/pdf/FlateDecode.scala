package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector
import scala.scalajs.js

private[pdf] object PredictorTransform:

  def param(key: String, default: Int)(params: Prim): Int =
    Prim.Dict.number(key)(params).map(_.toInt).getOrElse(default)

  def apply(stream: BitVector, predictor: BigDecimal, params: Prim): Attempt[BitVector] =
    try
      Attempt.successful(
        BitVector(
          decode(
            stream.toByteArray,
            predictor.toInt,
            param("Colors", 1)(params),
            param("BitsPerComponent", 8)(params),
            param("Columns", 1)(params)
          )
        )
      )
    catch
      case throwable: Throwable => Attempt.failure(Err(s"PredictorTransform failed: ${throwable.getMessage}"))

  private def decode(input: Array[Byte], predictor: Int, colors: Int, bitsPerComponent: Int, columns: Int): Array[Byte] =
    if predictor == 1 then input
    else
      val rowLength = (columns * colors * bitsPerComponent + 7) / 8
      val output    = new Array[Byte](input.length)
      val previous  = new Array[Byte](rowLength)
      var read      = 0
      var written   = 0

      while read < input.length do
        val linePredictor =
          if predictor >= 10 then
            if read >= input.length then throw IllegalArgumentException("truncated PNG predictor row")
            val value = (input(read) & 0xff) + 10
            read += 1
            value
          else predictor

        if input.length - read < rowLength then throw IllegalArgumentException("truncated predictor row")
        val active = new Array[Byte](rowLength)
        copy(input, read, active, 0, rowLength)
        read += rowLength
        decodeRow(linePredictor, colors, bitsPerComponent, columns, active, previous)
        copy(active, 0, previous, 0, rowLength)
        copy(active, 0, output, written, rowLength)
        written += rowLength

      if written == output.length then output
      else java.util.Arrays.copyOf(output, written)

  private def decodeRow(
    predictor: Int,
    colors: Int,
    bitsPerComponent: Int,
    columns: Int,
    active: Array[Byte],
    previous: Array[Byte]
  ): Unit =
    val bytesPerPixel = (colors * bitsPerComponent + 7) / 8
    predictor match
      case 1 | 10 => ()
      case 2 if bitsPerComponent == 8 =>
        var index = bytesPerPixel
        while index < active.length do
          active(index) = (unsigned(active(index)) + unsigned(active(index - bytesPerPixel))).toByte
          index += 1
      case 2 if bitsPerComponent == 16 =>
        var index = bytesPerPixel
        while index < active.length do
          val sub  = (unsigned(active(index)) << 8) + unsigned(active(index + 1))
          val left = (unsigned(active(index - bytesPerPixel)) << 8) + unsigned(active(index - bytesPerPixel + 1))
          active(index) = ((sub + left) >>> 8).toByte
          active(index + 1) = (sub + left).toByte
          index += 2
      case 2 if bitsPerComponent == 1 && colors == 1 =>
        var byteIndex = 0
        while byteIndex < active.length do
          var bit = 7
          while bit >= 0 do
            if byteIndex != 0 || bit != 7 then
              val sub = (unsigned(active(byteIndex)) >>> bit) & 1
              val left =
                if bit == 7 then unsigned(active(byteIndex - 1)) & 1
                else (unsigned(active(byteIndex)) >>> (bit + 1)) & 1
              active(byteIndex) = setBit(active(byteIndex), bit, (sub + left) & 1)
            bit -= 1
          byteIndex += 1
      case 2 =>
        val elements = columns * colors
        var index    = colors
        while index < elements do
          val subByte = index * bitsPerComponent / 8
          val subBit  = 8 - index * bitsPerComponent % 8 - bitsPerComponent
          val leftByte = (index - colors) * bitsPerComponent / 8
          val leftBit  = 8 - (index - colors) * bitsPerComponent % 8 - bitsPerComponent
          active(subByte) = setBits(
            active(subByte),
            subBit,
            bitsPerComponent,
            getBits(active(subByte), subBit, bitsPerComponent) + getBits(active(leftByte), leftBit, bitsPerComponent)
          )
          index += 1
      case 11 =>
        var index = bytesPerPixel
        while index < active.length do
          active(index) = (unsigned(active(index)) + unsigned(active(index - bytesPerPixel))).toByte
          index += 1
      case 12 =>
        var index = 0
        while index < active.length do
          active(index) = (unsigned(active(index)) + unsigned(previous(index))).toByte
          index += 1
      case 13 =>
        var index = 0
        while index < active.length do
          val left = if index >= bytesPerPixel then unsigned(active(index - bytesPerPixel)) else 0
          active(index) = (unsigned(active(index)) + (left + unsigned(previous(index))) / 2).toByte
          index += 1
      case 14 =>
        var index = 0
        while index < active.length do
          val left = if index >= bytesPerPixel then unsigned(active(index - bytesPerPixel)) else 0
          val up   = unsigned(previous(index))
          val upLeft = if index >= bytesPerPixel then unsigned(previous(index - bytesPerPixel)) else 0
          val candidate = left + up - upLeft
          val a = math.abs(candidate - left)
          val b = math.abs(candidate - up)
          val c = math.abs(candidate - upLeft)
          val selected = if a <= b && a <= c then left else if b <= c then up else upLeft
          active(index) = (unsigned(active(index)) + selected).toByte
          index += 1
      case _ => ()

  private def unsigned(value: Byte): Int = value & 0xff

  private def getBits(value: Byte, startBit: Int, size: Int): Int =
    (unsigned(value) >>> startBit) & ((1 << size) - 1)

  private def setBits(value: Byte, startBit: Int, size: Int, next: Int): Byte =
    val mask = (1 << size) - 1
    ((unsigned(value) & ~(mask << startBit)) | ((next & mask) << startBit)).toByte

  private def setBit(value: Byte, bit: Int, next: Int): Byte =
    if next == 0 then (unsigned(value) & ~(1 << bit)).toByte
    else (unsigned(value) | (1 << bit)).toByte

  private def copy(source: Array[Byte], sourceOffset: Int, target: Array[Byte], targetOffset: Int, length: Int): Unit =
    var index = 0
    while index < length do
      target(targetOffset + index) = source(sourceOffset + index)
      index += 1

private[pdf] object FlateDecode:

  def handlePredictor(stream: BitVector, params: Prim.Dict): Option[Attempt[BitVector]] =
    Prim.path("Predictor")(params) {
      case Prim.Number(predictor) if predictor > 1 => PredictorTransform(stream, predictor, params)
    }

  def handleParams(stream: BitVector, data: Prim): Attempt[BitVector] =
    Prim
      .path("DecodeParms")(data) { case params @ Prim.Dict(_) => handlePredictor(stream, params) }
      .flatten
      .getOrElse(Attempt.successful(stream))

  def apply(
    stream: BitVector,
    data: Prim,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[BitVector] =
    try
      val chunks   = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
      var total    = 0L
      var overflow: FilterDecode.OutputLimitExceeded | Null = null
      val inflater = new PakoInflate(js.Dictionary("chunkSize" -> 16384))
      inflater.onData = (chunk: scala.scalajs.js.typedarray.Uint8Array) =>
        val observed = total + chunk.length.toLong
        if observed > maxOutputBytes.toLong then
          overflow = FilterDecode.OutputLimitExceeded("FlateDecode", maxOutputBytes.bytes, observed)
        else if overflow == null then
          chunks += JsBinary.bytes(chunk)
          total = observed
      val accepted = inflater.push(JsBinary.uint8(stream.toByteArray), true)
      if overflow != null then throw overflow.nn
      if !accepted || inflater.err != 0 then throw IllegalArgumentException(inflater.msg)
      val output = new Array[Byte](total.toInt)
      var offset = 0
      chunks.foreach { chunk =>
        var index = 0
        while index < chunk.length do
          output(offset + index) = chunk(index)
          index += 1
        offset += chunk.length
      }
      val inflated = BitVector(output)
      handleParams(inflated, data)
    catch
      case throwable: Throwable => Attempt.failure(Err(s"FlateDecode: ${throwable.getMessage}"))
