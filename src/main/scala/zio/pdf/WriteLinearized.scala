/*
 * Port of fs2.pdf.WriteLinearized — linearization dictionary + first-page
 * xref encoding. [[pipe]] is the streaming compatibility layout from fs2-pdf:
 * it buffers only the declared first-page object prefix, then delegates the
 * remaining parts to WritePdf's streaming encoder.
 */

package zio.pdf

import _root_.scodec.{Attempt, Codec}
import _root_.scodec.bits.ByteVector
import zio.*
import zio.pdf.codec.Codecs
import zio.stream.{ZChannel, ZPipeline}

object WriteLinearized {

  final case class InvalidLayout(
    firstPageCount: Int,
    totalCount: Int,
    fileSize: Long
  ) extends RuntimeException(
        s"invalid linearized layout: firstPageCount=$firstPageCount, totalCount=$totalCount, fileSize=$fileSize"
      )

  final case class MissingFirstPageObjects(expected: Int, actual: Int)
      extends RuntimeException(s"linearized layout expected $expected first-page parts but received $actual")

  def objectNumber[A]: Part[A] => Attempt[Long] = {
    case Part.Obj(IndirectObj(Obj(Obj.Index(n, _), _), _))       => Attempt.successful(n)
    case Part.Preencoded(Obj.Index(n, _), _)                    => Attempt.successful(n)
    case _                                                     => Codecs.fail("first part is not an object")
  }

  def encode[A]: Part[A] => Attempt[EncodedObj] = {
    case Part.Obj(obj)       => EncodedObj.indirect(obj)
    case Part.Preencoded(index, bytes) =>
      Attempt.successful(EncodedObj(XrefObjMeta(index, bytes.size), bytes))
    case Part.Meta(_)        => Codecs.fail("trailer in first page data")
    case Part.Version(_)     => Codecs.fail("Part.Version not at the head of stream")
    case _: Part.StreamObj   => Codecs.fail("Part.StreamObj in first page chunk")
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
          val maxNumber = xrefs.map(_.index.number).max
          val t = Trailer(
            BigDecimal(count),
            trailer ++ Prim.dict("Size" -> Prim.num(maxNumber + 1L)),
            None
          )
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
          triple      <- encodeFirstPageParts(firstPage, totalCount, trailerData)
          (entries, data, trailer) = triple
          xrefLength  <- calculateXrefLength(entries.size + 1, totalCount, trailer.data)
          encXref     <- encodeXrefBytes(entries, trailer, headerSize + xrefLength)
        } yield FirstPage(
          encXref,
          headerSize + data.toList.map(_.size).sum + xrefLength,
          data,
          firstNumber
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

  /** Linearization object bytes, optional hint stream, first-page xref, then first-page objects. */
  final case class PrefixResult(
    bytes: Chunk[ByteVector],
    absoluteObjects: List[(Long, Long, Long)]
  )

  def encodeLinearizedPrefix(
    trailerData: Prim.Dict,
    totalCount: Int,
    headerSize: Long,
    params: LinearizationParams,
    firstPage: Chunk[Part[Trailer]],
    hintStream: Option[IndirectObj] = None
  ): Attempt[PrefixResult] =
    NonEmptyChunk.fromIterableOption(firstPage) match {
      case None => Codecs.fail("first page objects")
      case Some(firstPageNec) =>
        for {
          firstNumber <- objectNumber(firstPageNec.head)
          triple      <- encodeFirstPageParts(firstPageNec, totalCount, trailerData)
          (entries, data, trailer) = triple
          linNumber = if firstNumber > 1L then firstNumber - 1L else entries.map(_.index.number).max + 1L
          lin         <- createLinearizationBytes(linNumber, params)
          hintBytes   <- hintStream match {
                           case None =>
                             Attempt.successful(Option.empty[ByteVector])
                           case Some(obj) =>
                             EncodedObj.indirect(obj).map(encoded => Some(encoded.bytes))
                         }
          xrefLength  <- calculateXrefLength(entries.size + 1, totalCount, trailer.data)
          dataStart    = headerSize + lin.size + hintBytes.fold(0L)(_.size) + xrefLength
          encXref     <- encodeXrefBytes(entries, trailer, dataStart)
        } yield {
          var offset = headerSize
          val linEntry = (linNumber, offset, lin.size.toLong)
          offset += lin.size
          val hintEntry = hintBytes.map { hint =>
            val current = (linNumber + 1L, offset, hint.size.toLong)
            offset += hint.size
            current
          }
          offset += encXref.size
          val pageObjects = data.toList.zip(entries.toList).map { case (bytes, meta) =>
            val current = (meta.index.number, offset, bytes.size.toLong)
            offset += bytes.size
            current
          }
          val absoluteObjects = linEntry :: hintEntry.toList ++ pageObjects
          val prefix = hintBytes match {
            case Some(hint) => Chunk(lin, hint, encXref) ++ Chunk.fromIterable(data.toList)
            case None       => Chunk(lin, encXref) ++ Chunk.fromIterable(data.toList)
          }
          PrefixResult(prefix, absoluteObjects)
        }
    }

  /** @deprecated prefer [[encodeLinearizedPrefix]] with explicit [[LinearizationParams]] */
  def encodeLinearizedPrefix(
    trailerData: Prim.Dict,
    totalCount: Int,
    headerSize: Long,
    fileSize: Long,
    firstPage: Chunk[Part[Trailer]]
  ): Attempt[Chunk[ByteVector]] =
    encodeLinearizedPrefix(
      trailerData,
      totalCount,
      headerSize,
      linParams(totalCount, fileSize),
      firstPage,
      None
    ).map(_.bytes)

  /**
   * fs2-pdf-compatible streaming layout writer.
   *
   * `firstPageCount`, `totalCount`, and `fileSize` are layout facts supplied
   * by the caller. This method does not discover first-page dependencies or
   * generate hint streams; callers that need complete ISO linearization need
   * a planner that owns those semantics. It does guarantee that the version is
   * emitted once, only the bounded first-page prefix is retained, and the tail
   * is encoded incrementally with a generated final xref.
   */
  def pipe(
    trailerData: Prim.Dict,
    firstPageCount: Int,
    totalCount: Int,
    fileSize: Long
  ): ZPipeline[Any, Throwable, Part[Trailer], ByteVector] =
    pipeWithHints(trailerData, firstPageCount, totalCount, linParams(totalCount, fileSize), None)

  /** Linearized layout writer with explicit params and an optional ISO hint stream. */
  def pipeWithHints(
    trailerData: Prim.Dict,
    firstPageCount: Int,
    totalCount: Int,
    params: LinearizationParams,
    hintStream: Option[IndirectObj]
  ): ZPipeline[Any, Throwable, Part[Trailer], ByteVector] =
    ZPipeline.fromChannel(encodeLayout(trailerData, firstPageCount, totalCount, params, hintStream))

  private def encodeLayout(
    trailerData: Prim.Dict,
    firstPageCount: Int,
    totalCount: Int,
    params: LinearizationParams,
    hintStream: Option[IndirectObj]
  ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] = {
    if firstPageCount <= 0 || totalCount < firstPageCount || params.fileSize < 0L then
      ZChannel.fail(InvalidLayout(firstPageCount, totalCount, params.fileSize))
    else {
      def startTail(
        headerSize: Long,
        firstPage: Chunk[Part[Trailer]],
        pending: Chunk[Part[Trailer]]
      ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
        encodeLinearizedPrefix(trailerData, totalCount, headerSize, params, firstPage, hintStream) match {
          case _root_.scodec.Attempt.Failure(error) =>
            ZChannel.fail(new RuntimeException(s"encoding linearized first page: ${error.messageWithContext}"))
          case _root_.scodec.Attempt.Successful(prefixResult) =>
            val prefixSize = prefixResult.bytes.foldLeft(0L)(_ + _.size)
            val finalTrailer = Trailer(
              BigDecimal(totalCount),
              trailerData,
              trailerData.data.get("Root").collect { case root: Prim.Ref => root }
            )
            ZChannel.write(prefixResult.bytes) *>
              WritePdf.tailEncoder(
                headerSize + prefixSize,
                pending,
                Some(finalTrailer),
                absolutePrefix = prefixResult.absoluteObjects
              )
        }

      def collectFirstPage(
        headerSize: Long,
        collected: Chunk[Part[Trailer]],
        pending: Chunk[Part[Trailer]]
      ): ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
        if collected.size >= firstPageCount then startTail(headerSize, collected, pending)
        else if pending.nonEmpty then {
          val needed = firstPageCount - collected.size
          collectFirstPage(headerSize, collected ++ pending.take(needed), pending.drop(needed))
        } else {
          ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
            chunk => collectFirstPage(headerSize, collected, chunk),
            cause => ZChannel.refailCause(cause),
            _ => ZChannel.fail(MissingFirstPageObjects(firstPageCount, collected.size))
          )
        }

      def initial: ZChannel[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit] =
        ZChannel.readWithCause[Any, Throwable, Chunk[Part[Trailer]], Any, Throwable, Chunk[ByteVector], Unit](
          chunk =>
            if chunk.isEmpty then initial
            else {
              WritePdf.encodeVersion(Some(chunk.head)) match {
                case Left(message) => ZChannel.fail(new RuntimeException(message))
                case Right((header, leftover)) =>
                  val pending = leftover.fold[Chunk[Part[Trailer]]](chunk.drop(1))(part => part +: chunk.drop(1))
                  ZChannel.write(Chunk.single(header)) *>
                    collectFirstPage(header.size.toLong, Chunk.empty, pending)
              }
            },
          cause => ZChannel.refailCause(cause),
          _ => ZChannel.fail(MissingFirstPageObjects(firstPageCount, 0))
        )

      initial
    }
  }
}
