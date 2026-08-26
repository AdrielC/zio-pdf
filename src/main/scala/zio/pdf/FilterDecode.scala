/*
 * PDF stream filter chain — ASCIIHex, ASCII85, RunLength, LZW, Flate.
 * Filters apply outermost-first. Image filters stay raw for [[Elements]].
 */

package zio.pdf

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.BitVector

private[pdf] object FilterDecode {

  final case class OutputLimitExceeded(filter: String, maxBytes: Int, observedBytes: Long)
      extends RuntimeException(s"$filter output exceeded the configured $maxBytes-byte limit at $observedBytes bytes")

  val passthrough: Set[String] =
    Set("DCTDecode", "CCITTFaxDecode", "JBIG2Decode", "JPXDecode", "Crypt")

  def filterNames(data: Prim): List[String] =
    Prim.tryDict("Filter")(data) match {
      case Some(Prim.Name(n)) => List(n)
      case Some(Prim.Array(elems)) =>
        elems.iterator.collect { case Prim.Name(n) => n }.toList
      case _ => Nil
    }

  def decodeParmsAt(data: Prim, index: Int): Prim =
    Prim.tryDict("DecodeParms")(data) match {
      case Some(d @ Prim.Dict(_)) => d
      case Some(Prim.Array(elems)) if index < elems.length =>
        elems(index) match {
          case d @ Prim.Dict(_) => d
          case Prim.Null        => Prim.Dict.empty
          case other            => other
        }
      case _ => Prim.Dict.empty
    }

  def applyOne(
    name: String,
    stream: BitVector,
    parms: Prim,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[BitVector] =
    name match {
      case "FlateDecode" =>
        // Wrap DecodeParms so FlateDecode.handleParams finds them.
        FlateDecode(stream, Prim.dict("DecodeParms" -> parms), maxOutputBytes)
      case "ASCIIHexDecode"  => AsciiHexDecode(stream)
      case "ASCII85Decode"   => Ascii85Decode(stream)
      case "RunLengthDecode" => RunLengthDecode(stream, maxOutputBytes)
      case "LZWDecode"       => LzwDecode(stream, parms, maxOutputBytes)
      case other if passthrough.contains(other) =>
        Attempt.successful(stream)
      case other =>
        Attempt.failure(Err(s"unsupported stream filter: $other"))
    }

  def applyChain(
    stream: BitVector,
    data: Prim,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[BitVector] = {
    val names = filterNames(data)
    if names.isEmpty then Attempt.successful(stream)
    else
      names.zipWithIndex.foldLeft[Attempt[BitVector]](Attempt.successful(stream)) {
        case (Attempt.Successful(cur), (name, idx)) =>
          if passthrough.contains(name) then Attempt.successful(cur)
          else
            applyOne(name, cur, decodeParmsAt(data, idx), maxOutputBytes).flatMap { output =>
              val observed = output.bytes.size
              if observed > maxOutputBytes.toLong then
                Attempt.failure(Err(OutputLimitExceeded(name, maxOutputBytes.bytes, observed).getMessage))
              else Attempt.successful(output)
            }
        case (err, _) => err
      }
  }
}

private[pdf] object AsciiHexDecode {
  def apply(stream: BitVector): Attempt[BitVector] = {
    val in     = stream.toByteArray
    val out    = new Array[Byte]((in.length + 1) / 2)
    var nibble = -1
    var o      = 0
    var i      = 0
    while i < in.length do
      val b = in(i)
      i += 1
      if b == '>'.toByte then
        if nibble >= 0 then
          out(o) = (nibble << 4).toByte
          o += 1
        return Attempt.successful(BitVector(java.util.Arrays.copyOf(out, o)))
      else if b == ' '.toByte || b == '\n'.toByte || b == '\r'.toByte || b == '\t'.toByte || b == '\f'.toByte || b == 0.toByte then
        ()
      else {
        val v =
          if b >= '0'.toByte && b <= '9'.toByte then b - '0'.toByte
          else if b >= 'A'.toByte && b <= 'F'.toByte then b - 'A'.toByte + 10
          else if b >= 'a'.toByte && b <= 'f'.toByte then b - 'a'.toByte + 10
          else return Attempt.failure(Err(s"ASCIIHexDecode: bad nibble ${b.toInt & 0xff}"))
        if nibble < 0 then nibble = v
        else {
          out(o) = ((nibble << 4) | v).toByte
          o += 1
          nibble = -1
        }
      }
    if nibble >= 0 then
      out(o) = (nibble << 4).toByte
      o += 1
    Attempt.successful(BitVector(java.util.Arrays.copyOf(out, o)))
  }
}

private[pdf] object Ascii85Decode {
  def apply(stream: BitVector): Attempt[BitVector] = {
    val in  = stream.toByteArray
    val out = new scala.collection.mutable.ArrayBuffer[Byte](in.length)
    val tup = new Array[Byte](5)
    var n   = 0
    var i   = 0
    def flush(count: Int): Unit = {
      var value = 0L
      var k     = 0
      while k < count do
        value = value * 85L + ((tup(k) & 0xff) - 33)
        k += 1
      while k < 5 do
        value = value * 85L + 84
        k += 1
      val bytes = count - 1
      var shift = 24
      var b     = 0
      while b < bytes do
        out += ((value >> shift) & 0xff).toByte
        shift -= 8
        b += 1
    }
    while i < in.length do
      val b = in(i)
      i += 1
      if b == '~'.toByte then
        if i < in.length && in(i) == '>'.toByte then i += 1
        if n > 0 then flush(n)
        return Attempt.successful(BitVector(out.toArray))
      else if b == 'z'.toByte && n == 0 then
        out += 0; out += 0; out += 0; out += 0
      else if b == ' '.toByte || b == '\n'.toByte || b == '\r'.toByte || b == '\t'.toByte || b == '\f'.toByte || b == 0.toByte then
        ()
      else if b >= '!'.toByte && b <= 'u'.toByte then
        tup(n) = b
        n += 1
        if n == 5 then
          flush(5)
          n = 0
      else
        return Attempt.failure(Err(s"ASCII85Decode: bad byte ${b.toInt & 0xff}"))
    if n > 0 then flush(n)
    Attempt.successful(BitVector(out.toArray))
  }
}

private[pdf] object RunLengthDecode {
  def apply(
    stream: BitVector,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[BitVector] = {
    val in  = stream.toByteArray
    val out = new scala.collection.mutable.ArrayBuffer[Byte](in.length * 2)
    var i   = 0
    while i < in.length do
      val len = in(i) & 0xff
      i += 1
      if len == 128 then return Attempt.successful(BitVector(out.toArray))
      else if len < 128 then
        val copy = len + 1
        if i + copy > in.length then return Attempt.failure(Err("RunLengthDecode: truncated copy"))
        if out.length.toLong + copy.toLong > maxOutputBytes.toLong then
          return Attempt.failure(
            Err(FilterDecode.OutputLimitExceeded("RunLengthDecode", maxOutputBytes.bytes, out.length.toLong + copy).getMessage)
          )
        var k = 0
        while k < copy do
          out += in(i + k)
          k += 1
        i += copy
      else
        val run = 257 - len
        if i >= in.length then return Attempt.failure(Err("RunLengthDecode: truncated run"))
        if out.length.toLong + run.toLong > maxOutputBytes.toLong then
          return Attempt.failure(
            Err(FilterDecode.OutputLimitExceeded("RunLengthDecode", maxOutputBytes.bytes, out.length.toLong + run).getMessage)
          )
        val b = in(i)
        i += 1
        var k = 0
        while k < run do
          out += b
          k += 1
    Attempt.successful(BitVector(out.toArray))
  }
}

private[pdf] object LzwDecode {
  def apply(
    stream: BitVector,
    parms: Prim,
    maxOutputBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[BitVector] = {
    val earlyChange =
      Prim.Dict.number("EarlyChange")(parms).toOption.map(_.toInt).getOrElse(1) != 0
    try
      val raw = decodeLzw(stream.toByteArray, earlyChange, maxOutputBytes)
      // Predictor via FlateDecode's DecodeParms wrapper
      FlateDecode.handleParams(BitVector(raw), Prim.dict("DecodeParms" -> parms))
    catch {
      case t: Throwable => Attempt.failure(Err(s"LZWDecode: ${t.getMessage}"))
    }
  }

  private def decodeLzw(input: Array[Byte], earlyChange: Boolean, maxOutputBytes: ByteLimit): Array[Byte] = {
    val out      = new scala.collection.mutable.ArrayBuffer[Byte](input.length * 2)
    val table    = Array.ofDim[Array[Byte]](4096)
    var nextCode = 258
    var bits     = 9
    var bitPos   = 0
    val bitLen   = input.length * 8

    def readCode(width: Int): Int = {
      if bitPos + width > bitLen then return -1
      var v = 0
      var i = 0
      while i < width do
        val byteIndex = (bitPos + i) >> 3
        val bitIndex  = 7 - ((bitPos + i) & 7)
        v = (v << 1) | ((input(byteIndex) >> bitIndex) & 1)
        i += 1
      bitPos += width
      v
    }

    def clear(): Unit =
      var c = 0
      while c < 256 do
        table(c) = Array(c.toByte)
        c += 1
      nextCode = 258
      bits = 9

    clear()
    var prev: Array[Byte] | Null = null
    while true do
      val threshold =
        if earlyChange then (1 << bits) - 1
        else 1 << bits
      if nextCode == threshold && bits < 12 then bits += 1
      val code = readCode(bits)
      if code < 0 then return out.toArray
      if code == 256 then
        clear()
        prev = null
      else if code == 257 then return out.toArray
      else {
        val entry: Array[Byte] =
          if code < nextCode && table(code) != null then table(code)
          else if prev != null && code == nextCode then
            val p = prev.nn
            p :+ p(0)
          else
            throw new IllegalArgumentException(s"bad LZW code $code")
        if out.length.toLong + entry.length.toLong > maxOutputBytes.toLong then
          throw FilterDecode.OutputLimitExceeded(
            "LZWDecode",
            maxOutputBytes.bytes,
            out.length.toLong + entry.length.toLong
          )
        out ++= entry
        if prev != null && nextCode < 4096 then
          table(nextCode) = prev.nn :+ entry(0)
          nextCode += 1
        prev = entry
      }
    out.toArray
  }
}
