/*
 * Port of fs2.pdf.WriteLinearized — linearization dictionary + first-page
 * xref encoding (Attempt-based). The legacy FS2 `pipe` that interleaved
 * this prefix with [[WritePdf.parts]] is not re-exposed yet: build bytes
 * with [[encodeLinearizedPrefix]] and concatenate with the tail encoded
 * via [[WritePdf.parts]] when you need the full linearized file layout.
 */

package zio.pdf

import _root_.scodec.{Attempt, Codec}
import _root_.scodec.bits.ByteVector
import zio.*
import zio.pdf.codec.Codecs

object WriteLinearized {

  def objectNumber[A]: Part[A] => Attempt[Long] = {
    case Part.Obj(IndirectObj(Obj(Obj.Index(n, _), _), _)) => Attempt.successful(n)
    case _                                                 => Codecs.fail("first part is not an object")
  }

  def encode[A]: Part[A] => Attempt[EncodedObj] = {
    case Part.Obj(obj)     => EncodedObj.indirect(obj)
    case Part.Meta(_)      => Codecs.fail("trailer in first page data")
    case Part.Version(_)   => Codecs.fail("Part.Version not at the head of stream")
    case _: Part.StreamObj => Codecs.fail("Part.StreamObj in first page chunk")
  }

  val xrefStatic: String =
    """xref
      |
      |trailer
      |
      |startxref
      |0
      |%%EOF
      |""".stripMargin

  def calculateXrefLength(entries: Int, totalCount: Int, trailer: Prim.Dict): Attempt[Long] =
    Prim.Codec_Prim.encode(trailer).map { encTrailer =>
      encTrailer.bytes.size +
        entries * 20 +
        entries.toString.length +
        (totalCount - entries + 1).toString.length +
        1 +
        xrefStatic.length
    }

  def encodeFirstPageParts[A](
    firstPage: NonEmptyChunk[Part[A]],
    count: Int,
    trailer: Prim.Dict
  ): Attempt[(NonEmptyChunk[XrefObjMeta], NonEmptyChunk[ByteVector], Trailer)] =
    firstPage.toList.foldLeft[Attempt[List[EncodedObj]]](Attempt.successful(Nil)) {
      case (Attempt.Successful(xs), p) => encode(p).map(_ :: xs)
      case (f @ Attempt.Failure(_), _)   => f
    }.flatMap { reversed =>
      val encoded = reversed.reverse
      NonEmptyChunk.fromIterableOption(encoded) match {
        case None => Codecs.fail("encodeFirstPageParts: empty")
        case Some(encNec) =>
          val xrefs = encNec.map(_.xref)
          val bytes = encNec.map(_.bytes)
          val t = Trailer(BigDecimal(count), trailer ++ Prim.dict("Size" -> Prim.num(count)), None)
          Attempt.successful((xrefs, bytes, t))
      }
    }

  final case class FirstPage(
    xref:        ByteVector,
    xrefLength: Long,
    data:       NonEmptyChunk[ByteVector],
    firstObjNumber: Long
  )

  val linearizationSize: Long = 100

  def encodeFirstPage[A](
    trailerData: Prim.Dict,
    totalCount: Int
  )(headerSize: Long)(firstPageChunk: Chunk[Part[A]]): Attempt[FirstPage] =
    NonEmptyChunk.fromIterableOption(firstPageChunk) match {
      case None => Codecs.fail("first page objects")
      case Some(firstPage) =>
        for {
          firstNumber <- objectNumber(firstPage.head)
          xrefOffset = headerSize + linearizationSize
          triple       <- encodeFirstPageParts(firstPage, totalCount, trailerData)
          (entries, data, trailer) = triple
          xrefLength <- calculateXrefLength(entries.size + 1, totalCount, trailer.data)
          encXref    <- encodeXrefBytes(entries, trailer, xrefOffset + xrefLength)
        } yield FirstPage(
          encXref,
          xrefOffset + data.toList.map(_.size).sum + xrefLength,
          data,
          firstNumber,
        )
    }

  private def encodeXrefBytes(
    entries: NonEmptyChunk[XrefObjMeta],
    trailer: Trailer,
    initialOffset: Long
  ): Attempt[ByteVector] =
    Codecs.encodeBytes(GenerateXref(entries, trailer, initialOffset))(using summon[Codec[Xref]])

  final case class LinearizationParams(
    fileSize:           Long,
    firstPageObjNumber: Long,
    hintStreamOffset:   Long,
    hintStreamLength:   Long,
    firstPageEndOffset: Long,
    pageCount:          Long,
    mainXrefOffset:      Long
  )

  def linearizationDict: LinearizationParams => Prim.Dict = {
    case LinearizationParams(
          fileSize,
          firstPageObjNumber,
          hintStreamOffset,
          hintStreamLength,
          firstPageEndOffset,
          pageCount,
          mainXrefOffset,
        ) =>
      Prim.dict(
        "Linearized" -> Prim.Number(1),
        "L"          -> Prim.Number(fileSize),
        "H"          -> Prim.Array.nums(hintStreamOffset, hintStreamLength),
        "O"          -> Prim.Number(firstPageObjNumber),
        "E"          -> Prim.Number(firstPageEndOffset),
        "N"          -> Prim.Number(pageCount),
        "T"          -> Prim.Number(mainXrefOffset),
      )
  }

  def linearizationObj(number: Long, data: Prim): IndirectObj =
    IndirectObj(Obj(Obj.Index(number, 0), data), None)

  def createLinearizationBytes(number: Long, params: LinearizationParams): Attempt[ByteVector] =
    Codecs.encodeBytes(linearizationObj(number, linearizationDict(params)))

  def linParams(totalCount: Int, fileSize: Long): LinearizationParams =
    LinearizationParams(
      fileSize,
      0,
      0,
      0,
      0,
      totalCount,
      0,
    )

  /** Linearization object bytes, first-page xref, then each first-page object chunk. */
  def encodeLinearizedPrefix(
    trailerData: Prim.Dict,
    totalCount: Int,
    headerSize: Long,
    fileSize: Long,
    firstPage: Chunk[Part[Trailer]]
  ): Attempt[Chunk[ByteVector]] =
    encodeFirstPage(trailerData, totalCount)(headerSize)(firstPage).flatMap { fp =>
      createLinearizationBytes(fp.firstObjNumber - 1, linParams(totalCount, fileSize)).map { lin =>
        Chunk(lin, fp.xref) ++ Chunk.fromIterable(fp.data.toList)
      }
    }
}
