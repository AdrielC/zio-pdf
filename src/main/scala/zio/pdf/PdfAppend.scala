/*
 * Incremental PDF update writer for sign-and-append workflows.
 *
 * Preserves the original byte prefix, appends new/changed objects, and closes
 * with an xref subsection whose trailer carries `/Prev` back to the prior startxref.
 */

package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import zio.*
import zio.stream.ZStream

object PdfAppend {

  sealed abstract class Error(message: String) extends Exception(message)

  case object NoStartXref extends Error("no startxref marker found in PDF bytes")

  case object NoTrailer extends Error("decoded PDF has no trailer")

  /**
   * Parse the last conventional `trailer` dictionary from the trailing 256 KiB
   * without decoding the rest of the document. Xref-stream files fall back to
   * a full decode in [[append]].
   */
  def trailerFromTail(bytes: Chunk[Byte]): Either[Error, Trailer] = {
    val arr    = bytes.toArray
    val needle = "trailer".getBytes(StandardCharsets.US_ASCII)
    val window = math.min(arr.length, 256 * 1024)
    val start  = arr.length - window
    var last   = -1
    var index  = start
    while index <= arr.length - needle.length do
      if bytesEqualAt(arr, index, needle) then last = index
      index += 1
    if last < 0 then Left(NoTrailer)
    else
      Xref.Codec_Trailer.decode(BitVector(arr.drop(last))) match {
        case Attempt.Successful(decoded) => Right(decoded.value)
        case Attempt.Failure(_)          => Left(NoTrailer)
      }
  }

  /** Locate the last `startxref` offset in a byte buffer (searches the trailing 256 KiB). */
  def previousStartXref(bytes: Chunk[Byte]): Either[Error, Long] = {
    val arr    = bytes.toArray
    val needle = "startxref".getBytes(StandardCharsets.US_ASCII)
    val window = math.min(arr.length, 256 * 1024)
    val start  = arr.length - window
    var last   = -1
    var index  = start
    while index <= arr.length - needle.length do
      if bytesEqualAt(arr, index, needle) then last = index
      index += 1
    if last < 0 then Left(NoStartXref)
    else
      val tail = new String(arr, last + needle.length, arr.length - last - needle.length, StandardCharsets.US_ASCII)
      tail
        .trim
        .takeWhile(c => c.isDigit || c == '-' )
        .toLongOption
        .toRight(NoStartXref)
  }

  private def bytesEqualAt(source: Array[Byte], offset: Int, needle: Array[Byte]): Boolean = {
    var index = 0
    while index < needle.length do
      if source(offset + index) != needle(index) then return false
      index += 1
    true
  }

  /** Latest trailer from a decoded timeline (last xref wins for incremental files). */
  def latestTrailer(decoded: Chunk[Decoded]): Option[Trailer] =
    decoded.foldLeft(Option.empty[Trailer]) {
      case (_, Decoded.Meta(_, trailer, _)) => trailer
      case (acc, _)                         => acc
    }

  /** First unused object number for a new revision. */
  def nextObjectNumber(decoded: Chunk[Decoded], trailer: Trailer): Long = {
    val maxDecoded = decoded.foldLeft(0L) {
      case (max, Decoded.DataObj(obj))           => math.max(max, obj.index.number)
      case (max, Decoded.ContentObj(obj, _, _)) => math.max(max, obj.index.number)
      case (max, _)                             => max
    }
    math.max(maxDecoded + 1L, trailer.size.toLong)
  }

  /** Append a revision after an existing PDF byte prefix. */
  def append(
    base: Chunk[Byte],
    revision: Chunk[Part[Trailer]],
    preserveNumbers: Set[Long] = Set.empty
  ): ZIO[Any, Throwable, Chunk[Byte]] =
    for {
      prevXref <- ZIO.fromEither(previousStartXref(base))
      trailer  <- resolveTrailer(base)
      _        <- ZIO.fromEither(PdfCrypto.requireUnencrypted(trailer))
      startAt  = trailer.size.toLong
      prepared <- ZIO.attempt(prepareRevision(revision, startAt, preserveNumbers))
      appended <- ZStream
                    .fromChunk(prepared)
                    .via(WritePdf.appendParts(base.size.toLong, WritePdf.AppendContext(prevXref, trailer)))
                    .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
    } yield base ++ appended

  private def resolveTrailer(base: Chunk[Byte]): ZIO[Any, Throwable, Trailer] =
    ZIO.fromEither(trailerFromTail(base)).catchAll { _ =>
      ZStream.fromChunk(base).via(PdfStream.decode()).runCollect.flatMap { decoded =>
        ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded)) *>
          ZIO.fromOption(latestTrailer(decoded)).orElseFail(NoTrailer)
      }
    }

  private def prepareRevision(
    revision: Chunk[Part[Trailer]],
    startAt: Long,
    preserveNumbers: Set[Long]
  ): Chunk[Part[Trailer]] = {
    val withoutVersion = revision.filterNot(_.isInstanceOf[Part.Version])
    val meta           = withoutVersion.collect { case meta: Part.Meta[Trailer] => meta }
    val body           = withoutVersion.filterNot(_.isInstanceOf[Part.Meta[?]])
    val renumbered     = renumberParts(body, startAt, preserveNumbers)
    Chunk.fromIterable(renumbered ++ meta)
  }

  private def renumberParts(
    parts: Chunk[Part[Trailer]],
    startAt: Long,
    preserveNumbers: Set[Long]
  ): List[Part[Trailer]] = {
    val numbers = parts.collect {
      case Part.Obj(obj)                  => obj.obj.index.number
      case Part.StreamObj(index, _, _, _) => index.number
    }.toSet.toList.sorted
    if numbers.isEmpty then parts.toList
    else
      val movable = numbers.filter(n => !preserveNumbers(n) && n < startAt)
      if movable.isEmpty then parts.toList
      else
        val offset = startAt - movable.head
        parts.map {
          case objPart @ Part.Obj(obj) if preserveNumbers(obj.obj.index.number) =>
            objPart
          case Part.Obj(obj) =>
            val shifted = obj.obj.index.number + offset
            Part.Obj(
              IndirectObj(
                Obj(Obj.Index(shifted, obj.obj.index.generation), mapRefs(obj.obj.data, offset)),
                obj.stream
              )
            )
          case streamPart @ Part.StreamObj(index, data, length, payload) if preserveNumbers(index.number) =>
            streamPart
          case Part.StreamObj(index, data, length, payload) =>
            Part.StreamObj(
              Obj.Index(index.number + offset, index.generation),
              mapRefs(data, offset),
              length,
              payload
            )
          case other =>
            other
        }.toList
  }

  private def mapRefs(prim: Prim, offset: Long): Prim =
    prim match {
      case ref: Prim.Ref =>
        Prim.Ref(ref.number + offset, ref.generation)
      case Prim.Dict(data) =>
        Prim.Dict(data.map { case (key, value) => key -> mapRefs(value, offset) })
      case Prim.Array(data) =>
        Prim.Array(data.map(mapRefs(_, offset)))
      case other =>
        other
    }
}
