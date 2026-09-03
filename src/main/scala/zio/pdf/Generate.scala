/*
 * Port of fs2.pdf.Generate — incremental xref generation while encoding
 * a stream of [[IndirectObj]] values. Simpler than [[WritePdf]] when
 * callers only have raw objects and a trailer dict.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.*
import zio.pdf.codec.Codecs
import zio.stream.*

private[pdf] object Generate {

  val header: String =
    "%PDF-1.7\n%âãÏÓ\n"

  val headerBytes: ByteVector =
    Scodec.stringBytes(header)

  def generateXrefEntry(start: Long)(index: Obj.Index): (Long, Xref.Entry) =
    (index.number, EncodeMeta.xrefEntry(start, index.generation))

  def generateXref(trailer: Prim.Dict, startxref: Long, entries: List[(Long, Xref.Entry)]): Xref = {
    val sorted = entries.sortBy(_._1)
    val free   = Xref.entry(0, 65535, Xref.EntryType.Free)
    val tables = NonEmptyChunk(Xref.Table(0, NonEmptyChunk(free, sorted.map(_._2)*)))
    Xref(tables, Trailer(sorted.size + 1, trailer, Some(Prim.Ref(1, 0))), StartXref(startxref))
  }

  /** Encode objects to [[ByteVector]] chunks (header + bodies + xref). */
  def byteVectors(trailer: Prim.Dict): ZPipeline[Any, Throwable, IndirectObj, ByteVector] =
    ZPipeline.fromFunction[Any, Throwable, IndirectObj, ByteVector] { in =>
      ZStream.unwrap {
        in.mapZIO { obj =>
          ZIO.fromEither(
            EncodedObj.indirect(obj).toEither.left.map(e => new RuntimeException(e.messageWithContext)).map(enc => (obj, enc))
          )
        }.runFold((headerBytes.size.toLong, List.empty[(Long, Xref.Entry)], Chunk.single(headerBytes))) {
          case ((start, entries, out), (obj, enc)) =>
            val entry     = generateXrefEntry(start)(obj.obj.index)
            val nextStart = start + enc.bytes.size
            (nextStart, entry :: entries, out :+ enc.bytes)
        }.flatMap { case (startxref, entries, body) =>
          val xref = generateXref(trailer, startxref, entries)
          Codecs.encodeBytes(xref)(using summon[_root_.scodec.Codec[Xref]]) match
            case _root_.scodec.Attempt.Successful(xrefBytes) =>
              ZIO.succeed(ZStream.fromChunk(body :+ xrefBytes))
            case _root_.scodec.Attempt.Failure(cause) =>
              ZIO.fail(new RuntimeException(s"failed to encode xref: ${cause.messageWithContext}"))
        }
      }
    }

  /** Legacy-compatible pipe: [[IndirectObj]] → flattened [[Byte]]. */
  def apply(trailer: Prim.Dict): ZPipeline[Any, Throwable, IndirectObj, Byte] =
    byteVectors(trailer) >>> StreamUtil.bytesPipe
}
