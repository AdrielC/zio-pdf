/*
 * Byte-identical object grafting via [[Part.Preencoded]].
 */

package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.{BitVector, ByteVector}
import zio.Chunk
import zio.stream.ZStream

object PdfGraft {

  final case class RawParts(version: Option[Version], objects: Chunk[Part.Preencoded])

  /** Wrap raw `obj … endobj` bytes for inclusion in a [[WritePdf.parts]] stream. */
  def preencoded(objectNumber: Long, rawObjectBytes: ByteVector, generation: Int = 0): Part.Preencoded =
    Part.Preencoded(Obj.Index(objectNumber, generation), rawObjectBytes)

  /**
   * Slice verbatim top-level indirect-object bytes from a PDF without
   * expanding object streams or re-encoding payloads.
   */
  def rawObjectParts(bytes: Array[Byte]): Either[String, RawParts] = {
    var rest       = BitVector.view(bytes)
    var version    = Option.empty[Version]
    val objects    = Chunk.newBuilder[Part.Preencoded]
    var stop       = false

    while rest.nonEmpty && !stop do
      TopLevel.streamDecoder.decode(rest) match {
        case Attempt.Successful(result) =>
          val consumed = rest.size - result.remainder.size
          val raw      = rest.take(consumed).toByteVector
          result.value match {
            case TopLevel.VersionT(v) =>
              version = Some(v)
            case TopLevel.IndirectObjT(obj) if !isLinearizedDict(obj) =>
              objects += Part.Preencoded(obj.obj.index, raw)
            case TopLevel.XrefT(_) | TopLevel.StartXrefT(_) =>
              stop = true
            case _ =>
              ()
          }
          if !stop then rest = result.remainder
        case Attempt.Failure(cause) =>
          stop = true
      }

    Right(RawParts(version, objects.result()))
  }

  /** Build a [[Part]] stream that preserves donor bytes for every top-level object. */
  def partsFromPdf(bytes: Chunk[Byte]): Either[String, Chunk[Part[Trailer]]] =
    rawObjectParts(bytes.toArray).map { raw =>
      val versionPart = raw.version.fold(Chunk.empty[Part[Trailer]])(v => Chunk.single(Part.Version(v)))
      versionPart ++ raw.objects
    }

  /** Extract top-level object byte spans from an existing PDF. */
  def objectsFromPdf(bytes: Chunk[Byte]): zio.ZIO[Any, Throwable, Chunk[Part.Preencoded]] =
    ZStream
      .fromChunk(bytes)
      .via(PdfStream.topLevel)
      .runCollect
      .map { tops =>
        tops.flatMap {
          case TopLevel.IndirectObjT(obj) if !isLinearizedDict(obj) =>
            EncodedObj.indirect(obj) match {
              case Attempt.Successful(EncodedObj(_, encoded)) =>
                Chunk.single(preencoded(obj.obj.index.number, encoded, obj.obj.index.generation))
              case _ =>
                Chunk.empty
            }
          case _ =>
            Chunk.empty
        }
      }

  /** Graft selected object numbers from `donor` into preencoded parts. */
  def graft(
    donor: Chunk[Byte],
    objectNumbers: Set[Long]
  ): zio.ZIO[Any, Throwable, Chunk[Part.Preencoded]] =
    rawObjectParts(donor.toArray) match {
      case Left(message) =>
        zio.ZIO.fail(new RuntimeException(message))
      case Right(raw) =>
        zio.ZIO.succeed(raw.objects.filter(part => objectNumbers(part.index.number)))
    }

  private def isLinearizedDict(obj: IndirectObj): Boolean =
    obj.obj.data match {
      case dict: Prim.Dict => dict.data.contains("Linearized")
      case _               => false
    }
}
