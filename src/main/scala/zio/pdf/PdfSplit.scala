/*
 * Page-range extract, per-page split, and /Rotate rewrite for filing prep.
 *
 * Extract and split rebuild a fresh catalog/pages tree from each selected
 * page's dependency closure (the same path as [[PdfMerge]]). Rotate keeps
 * object numbers and only patches page dictionaries.
 */

package zio.pdf

import zio.*
import zio.stream.ZStream

object PdfSplit {

  sealed abstract class Error(message: String) extends Exception(message)

  case object NoPages extends Error("document has no pages")

  final case class InvalidRange(fromPage: Int, toPage: Int, pageCount: Int)
      extends Error(s"page range $fromPage-$toPage is outside 1-$pageCount")

  final case class InvalidRotation(degrees: Int)
      extends Error(s"rotation must be a multiple of 90 degrees: $degrees")

  def pageCount(decoded: Chunk[Decoded]): Int =
    TextExtract.orderedPageObjectNumbers(decoded).size

  def extract(
    decoded: Chunk[Decoded],
    fromPage: Int,
    toPage: Int
  ): Either[Error, Chunk[Part[Trailer]]] =
    selectedPages(decoded, fromPage, toPage).flatMap { pages =>
      PdfMerge.fromPageNumbers(decoded, pages).left.map(_ => NoPages)
    }

  def extractBytes(
    bytes: Chunk[Byte],
    fromPage: Int,
    toPage: Int,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(extract(decoded, fromPage, toPage)).flatMap(writeParts)
    }

  def split(decoded: Chunk[Decoded]): Either[Error, NonEmptyChunk[Chunk[Part[Trailer]]]] = {
    val pages = TextExtract.orderedPageObjectNumbers(decoded)
    if pages.isEmpty then Left(NoPages)
    else
      val extracted = pages.indices.toList.map(index => extract(decoded, index + 1, index + 1))
      extracted.collectFirst { case Left(error) => error } match {
        case Some(error) => Left(error)
        case None =>
          val ok = extracted.collect { case Right(parts) => parts }
          Right(NonEmptyChunk(ok.head, ok.tail*))
      }
  }

  def splitBytes(
    bytes: Chunk[Byte],
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[PdfEngine, Throwable, NonEmptyChunk[Chunk[Byte]]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(split(decoded)).flatMap { parts =>
        ZIO.foreach(parts)(writeParts).map { written =>
          NonEmptyChunk(written.head, written.tail*)
        }
      }
    }

  def rotate(
    decoded: Chunk[Decoded],
    degrees: Int,
    fromPage: Int,
    toPage: Int
  ): Either[Error, Chunk[Part[Trailer]]] =
    if degrees % 90 != 0 then Left(InvalidRotation(degrees))
    else
      selectedPages(decoded, fromPage, toPage).map { pages =>
        val targets = pages.toSet
        toParts(decoded.map(rotateDecoded(_, targets, degrees)))
      }

  def rotateBytes(
    bytes: Chunk[Byte],
    degrees: Int,
    fromPage: Int,
    toPage: Int,
    opts: PdfEngine.Options = PdfEngine.Options.default
  ): ZIO[PdfEngine, Throwable, Chunk[Byte]] =
    decodeUnencrypted(bytes, opts).flatMap { decoded =>
      ZIO.fromEither(rotate(decoded, degrees, fromPage, toPage)).flatMap(writeParts)
    }

  private def decodeUnencrypted(
    bytes: Chunk[Byte],
    opts: PdfEngine.Options
  ): ZIO[PdfEngine, Throwable, Chunk[Decoded]] =
    if bytes.size.toLong > opts.maxMaterializedDocumentBytes.toLong then
      ZIO.fail(PdfEngine.MaterializedDocumentLimitExceeded(opts.maxMaterializedDocumentBytes, bytes.size.toLong))
    else
      PdfEngine.decode(bytes, opts).tap { decoded =>
        ZIO.fromEither(PdfCrypto.requireUnencrypted(decoded))
      }

  private def selectedPages(
    decoded: Chunk[Decoded],
    fromPage: Int,
    toPage: Int
  ): Either[Error, List[Long]] = {
    val pages = TextExtract.orderedPageObjectNumbers(decoded)
    if pages.isEmpty then Left(NoPages)
    else if fromPage < 1 || toPage < fromPage || toPage > pages.size then
      Left(InvalidRange(fromPage, toPage, pages.size))
    else Right(pages.slice(fromPage - 1, toPage))
  }

  private def writeParts(parts: Chunk[Part[Trailer]]): ZIO[Any, Throwable, Chunk[Byte]] =
    ZStream
      .fromChunk(parts)
      .via(WritePdf.parts)
      .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))

  private def toParts(decoded: Chunk[Decoded]): Chunk[Part[Trailer]] = {
    val objects = decoded.collect {
      case Decoded.DataObj(obj)          => Part.Obj(IndirectObj(obj, None))
      case Decoded.ContentObj(obj, raw, _) => Part.Obj(IndirectObj(obj, Some(raw)))
    }
    val trailer = decoded.collect { case Decoded.Meta(_, Some(value), _) => value }.lastOption
    objects ++ trailer.toList.map(Part.Meta(_))
  }

  private def rotateDecoded(decoded: Decoded, pages: Set[Long], degrees: Int): Decoded =
    decoded match {
      case Decoded.DataObj(obj) if pages(obj.index.number) =>
        Decoded.DataObj(rotateObj(obj, degrees))
      case Decoded.ContentObj(obj, raw, stream) if pages(obj.index.number) =>
        Decoded.ContentObj(rotateObj(obj, degrees), raw, stream)
      case other =>
        other
    }

  private def rotateObj(obj: Obj, degrees: Int): Obj =
    obj.data match {
      case dict: Prim.Dict if dict.data.get("Type").contains(Prim.Name("Page")) =>
        val current = dict.data.get("Rotate") match {
          case Some(Prim.Number(value)) => value.toInt
          case _                        => 0
        }
        val next = Math.floorMod(current + degrees, 360)
        Obj(obj.index, Prim.Dict(dict.data.updated("Rotate", Prim.Number(BigDecimal(next)))))
      case _ =>
        obj
    }
}
