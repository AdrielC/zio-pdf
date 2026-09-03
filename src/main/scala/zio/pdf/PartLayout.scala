/*
 * Measure encoded byte sizes and cumulative offsets for a [[Part]] stream.
 */

package zio.pdf

import zio.Chunk

object PartLayout {

  final case class Entry(index: Obj.Index, offset: Long, size: Long)

  final case class Measured(
    headerSize: Long,
    entries: List[Entry],
    trailer: Option[Trailer]
  ):
    def objectNumbers: List[Long] = entries.map(_.index.number)

    def entry(number: Long): Option[Entry] =
      entries.find(_.index.number == number)

    def totalBodySize: Long =
      if entries.isEmpty then 0L else entries.last.offset + entries.last.size - headerSize

  /** Encode each part to bytes and accumulate xref offsets (no xref trailer). */
  def measure(parts: Chunk[Part[Trailer]]): Either[String, Measured] =
    WritePdf.encodeVersion(parts.headOption.collect { case Part.Version(v) => Part.Version(v) }) match {
      case Left(message) =>
        Left(message)
      case Right((bytes, leftover)) =>
        val headerSize = bytes.size.toLong
        val tail = leftover.fold(parts.drop(1))(part => part +: parts.drop(1)).filterNot(_.isInstanceOf[Part.Version])
        tail.foldLeft[Either[String, (Long, List[Entry], Option[Trailer])]](Right((headerSize, Nil, None))) {
          case (Left(error), _) =>
            Left(error)
          case (Right((offset, entries, trailer)), part) =>
            part match {
              case Part.Obj(obj) =>
                EncodedObj.indirect(obj) match {
                  case _root_.scodec.Attempt.Successful(EncodedObj(_, encoded)) =>
                    Right((offset + encoded.size, Entry(obj.obj.index, offset, encoded.size) :: entries, trailer))
                  case _root_.scodec.Attempt.Failure(c) =>
                    Left(s"encoding object ${obj.obj.index.number}: ${c.messageWithContext}")
                }
              case Part.Preencoded(index, encoded) =>
                Right((offset + encoded.size, Entry(index, offset, encoded.size) :: entries, trailer))
              case Part.StreamObj(index, data, length, _) =>
                WritePdf.encodeStreamHeaderForMeasure(index, data, length) match {
                  case Left(error) =>
                    Left(error.getMessage)
                  case Right(header) =>
                    val total = header.size + length + WritePdf.streamTrailerSize
                    Right((offset + total, Entry(index, offset, total) :: entries, trailer))
                }
              case Part.Meta(value) =>
                Right((offset, entries, Some(value)))
              case Part.Version(_) =>
                Left("Part.Version must appear at the head of the stream")
            }
        }.map { case (_, entries, trailer) =>
          Measured(headerSize, entries.reverse, trailer)
        }
    }
}
