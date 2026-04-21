/*
 * Port of fs2.pdf.ObjectStream to Scala 3 + scodec 2.3.
 */

package zio.pdf

import zio.pdf.codec.{Many, Text, Whitespace}
import _root_.scodec.Codec

/** An indirect object whose `/Type` is `/ObjStm`. The decompressed
  * stream contains a header of `(num offset)` pairs followed by the
  * concatenated `Prim` payloads. */
final case class ObjectStream(objs: List[Obj])

object ObjectStream {

  import _root_.scodec.codecs.{listOfN, provide}

  private def encode(os: ObjectStream): (List[Long], List[Prim]) =
    os.objs.map(o => (o.index.number, o.data)).unzip

  private val decode: ((List[Long], List[Prim])) => ObjectStream = { case (numbers, objects) =>
    ObjectStream(numbers.zip(objects).map { case (num, data) => Obj(Obj.Index(num, 0), data) })
  }

  // The legacy code TODO-noted that the offset is set to 0 on
  // encode; preserved here.
  private def number: Codec[Long] =
    Text.ascii.long <~ Whitespace.ws <~ Text.ascii.long.unit(0L) <~ Whitespace.ws

  given codec: Codec[ObjectStream] =
    Many
      .till[Long](a => !a.bytes.headOption.exists(Text.isDigit))(number)
      .flatZip(numbers => listOfN(provide(numbers.size), Prim.Codec_Prim))
      .xmap(decode, encode)
}
