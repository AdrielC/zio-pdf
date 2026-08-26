package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.{Attempt, Err}
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.pdf.codec.Whitespace

/** Objects embedded in a PDF `/Type /ObjStm` stream. */
final case class ObjectStream(objs: List[Obj])

object ObjectStream {

  /** Encoded stream bytes plus the dictionary values required by ISO 32000. */
  final case class Encoded(bytes: BitVector, count: Int, first: Int)

  /** Encode real object-number/offset pairs and a newline-delimited body. */
  def encode(
    stream: ObjectStream,
    maxBytes: ByteLimit = ByteLimit.DefaultStreamMaterialization
  ): Attempt[Encoded] = {
    val encoded  = List.newBuilder[(Long, BitVector)]
    val iterator = stream.objs.iterator
    while iterator.hasNext do {
      val obj = iterator.next()
      Prim.Codec_Prim.encode(obj.data) match {
        case Attempt.Successful(bits) => encoded += ((obj.index.number, bits))
        case Attempt.Failure(error)   => return Attempt.failure(error)
      }
    }

    val values = encoded.result()
    val header = new StringBuilder
    var offset = 0L
    values.foreach { case (number, bits) =>
      header.append(number).append(' ').append(offset).append(' ')
      offset = Math.addExact(offset, bits.bytes.size + 1L)
    }
    header.append('\n')

    val headerBytes = ByteVector.view(header.result().getBytes(StandardCharsets.US_ASCII))
    val totalBytes  = Math.addExact(headerBytes.size, offset)
    if totalBytes > maxBytes.toLong then
      return Attempt.failure(
        Err(s"encoded object stream is $totalBytes bytes, above the configured ${maxBytes.bytes}-byte limit")
      )
    val body = values.foldLeft(BitVector.empty) { case (all, (_, bits)) =>
      all ++ bits ++ BitVector.fromByte('\n'.toByte)
    }
    Attempt.successful(Encoded(headerBytes.bits ++ body, values.size, headerBytes.size.toInt))
  }

  /** Decode using the enclosing stream dictionary's mandatory `/N` and `/First`. */
  def decode(bits: BitVector, dictionary: Prim): Attempt[ObjectStream] =
    for {
      count <- integerField(dictionary, "N")
      first <- integerField(dictionary, "First")
      value <- decode(bits, count, first)
    } yield value

  def decode(bits: BitVector, count: Int, first: Int): Attempt[ObjectStream] = {
    val bytes = bits.bytes
    if count < 0 then return Attempt.failure(Err(s"object stream /N must be non-negative: $count"))
    if first < 0 || first.toLong > bytes.size then
      return Attempt.failure(Err(s"object stream /First $first is outside ${bytes.size}-byte stream"))

    val header = bytes.take(first.toLong)
    val pairs  = new Array[(Long, Long)](count)
    var at     = 0L
    var index  = 0

    def whitespace(byte: Byte): Boolean =
      byte == 0 || byte == 9 || byte == 10 || byte == 12 || byte == 13 || byte == 32

    def skipWhitespace(): Unit =
      while at < header.size && whitespace(header(at)) do at += 1L

    def readUnsigned(field: String): Either[Err, Long] = {
      skipWhitespace()
      if at >= header.size || header(at) < '0'.toByte || header(at) > '9'.toByte then
        Left(Err(s"object stream header is missing $field for entry $index"))
      else {
        var value = 0L
        while at < header.size && header(at) >= '0'.toByte && header(at) <= '9'.toByte do {
          val digit = (header(at) - '0'.toByte).toInt
          if value > (Long.MaxValue - digit.toLong) / 10L then
            return Left(Err(s"object stream header $field overflows Long for entry $index"))
          value = value * 10L + digit.toLong
          at += 1L
        }
        Right(value)
      }
    }

    while index < count do {
      val number = readUnsigned("object number") match {
        case Right(value) => value
        case Left(error)  => return Attempt.failure(error)
      }
      val offset = readUnsigned("object offset") match {
        case Right(value) => value
        case Left(error)  => return Attempt.failure(error)
      }
      pairs(index) = (number, offset)
      index += 1
    }
    skipWhitespace()
    if at != header.size then
      return Attempt.failure(Err(s"object stream header has unexpected bytes after $count entries"))

    val payload = bytes.drop(first.toLong)
    index = 0
    var previousOffset = -1L
    val objects = List.newBuilder[Obj]
    val decoder = (Prim.Codec_Prim <~ Whitespace.skipTrivia).complete
    while index < count do {
      val (number, offset) = pairs(index)
      val nextOffset = if index + 1 < count then pairs(index + 1)._2 else payload.size
      if offset < 0L || offset < previousOffset || nextOffset < offset || nextOffset > payload.size then
        return Attempt.failure(
          Err(s"object stream offset range [$offset, $nextOffset) is invalid for ${payload.size}-byte payload")
        )
      decoder.decode(payload.slice(offset, nextOffset).bits) match {
        case Attempt.Successful(result) => objects += Obj(Obj.Index(number, 0), result.value)
        case Attempt.Failure(error)     => return Attempt.failure(error.pushContext(s"object stream entry $number"))
      }
      previousOffset = offset
      index += 1
    }
    Attempt.successful(ObjectStream(objects.result()))
  }

  private def integerField(dictionary: Prim, name: String): Attempt[Int] =
    Prim.tryDict(name)(dictionary) match {
      case Some(Prim.Number(value)) if value.isWhole && value >= 0 && value <= Int.MaxValue =>
        Attempt.successful(value.toInt)
      case Some(value) => Attempt.failure(Err(s"object stream /$name must be a non-negative Int: $value"))
      case None        => Attempt.failure(Err(s"object stream dictionary is missing /$name"))
    }
}
