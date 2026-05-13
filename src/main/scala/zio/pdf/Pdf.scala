/*
 * Port of fs2.pdf.Pdf to Scala 3 + ZIO. Holds the assembled PDF model
 * plus read-only stream helpers mirroring the legacy `fs2.pdf.Pdf`
 * object (`objectNumbers`, `pageNumber`, `dictOfPage`, …) expressed as
 * `ZStream` / `ZPipeline` composition.
 */

package zio.pdf

import _root_.scodec.Attempt
import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.utf8
import zio.*
import zio.prelude.Validation
import zio.stream.*

sealed trait PdfObj

object PdfObj {
  final case class Content(obj: IndirectObj) extends PdfObj
  final case class Data(obj: Obj)             extends PdfObj
}

/** A fully-assembled PDF in memory: the objects, the xref tables,
  * the trailer. */
final case class Pdf(objs: NonEmptyChunk[PdfObj], xrefs: NonEmptyChunk[Xref], trailer: Trailer)

object Pdf {

  /** Decode top-level indirect objects and select by object number (raw stream bits, if any). */
  def objectNumbersRaw(numbers: List[Long]): ZPipeline[Any, Throwable, Byte, (Obj, Option[BitVector])] =
    PdfStream.topLevel >>> ZPipeline.mapChunks { ch =>
      ch.flatMap {
        case TopLevel.IndirectObjT(IndirectObj(Obj(idx @ Obj.Index(n, _), data), s)) if numbers.contains(n) =>
          Chunk.single((Obj(idx, data), s))
        case _ =>
          Chunk.empty
      }
    }

  /** Fully decode PDF bytes and select objects by number (materialised stream payload when present). */
  def objectNumbers(log: Log, numbers: List[Long]): ZPipeline[Any, Throwable, Byte, (Obj, Option[BitVector])] =
    ZPipeline.fromFunction[Any, Throwable, Byte, (Obj, Option[BitVector])] { bytes =>
      bytes.via(PdfStream.decode(log)).flatMap {
        case Decoded.ContentObj(obj @ Obj(Obj.Index(n, _), _), _, stream) if numbers.contains(n) =>
          ZStream.fromZIO(
            ZIO
              .fromEither(stream.exec.toEither.left.map(e => new RuntimeException(e.messageWithContext)))
              .map(bits => (obj, Some(bits)))
          )
        case Decoded.DataObj(obj @ Obj(Obj.Index(n, _), _)) if numbers.contains(n) =>
          ZStream.succeed((obj, None))
        case _ =>
          ZStream.empty
      }
    }

  /** First `/Pages` tree that lists `page` in its `/Kids` → that page's object number. */
  def pageNumber(log: Log, page: Int): ZPipeline[Any, Throwable, Byte, Long] =
    PdfStream.elements(log) >>> ZPipeline.mapChunks { ch =>
      ch.flatMap {
        case Element.Data(_, Element.DataKind.Pages(Pages(_, _, kids, _))) =>
          kids.lift(page).fold[Chunk[Long]](Chunk.empty)(r => Chunk.single(r.number))
        case _ =>
          Chunk.empty
      }
    } >>> ZPipeline.take(1L)

  /** Resolve the page object (and optional stream) for zero-based `page`. */
  def pageObject(log: Log, page: Int)(bytes: ZStream[Any, Throwable, Byte]): ZStream[Any, Throwable, (Obj, Option[BitVector])] =
    ZStream.unwrap(
      bytes.via(pageNumber(log, page)).runHead.map {
        case None       => ZStream.empty
        case Some(num) => bytes.via(objectNumbers(log, List(num)))
      }
    )

  def rawStreamOfObjs(numbers: List[Long]): ZPipeline[Any, Throwable, Byte, BitVector] =
    ZPipeline.fromFunction[Any, Throwable, Byte, BitVector](_.via(objectNumbersRaw(numbers)).collect { case (_, Some(b)) => b })

  def rawStreamOfObj(number: Long): ZPipeline[Any, Throwable, Byte, BitVector] =
    rawStreamOfObjs(List(number)) >>> ZPipeline.take(1L)

  def streamOfObjs(log: Log, numbers: List[Long]): ZPipeline[Any, Throwable, Byte, BitVector] =
    ZPipeline.fromFunction[Any, Throwable, Byte, BitVector](_.via(objectNumbers(log, numbers)).collect { case (_, Some(b)) => b })

  def streamTextOfObjs(log: Log, numbers: List[Long]): ZPipeline[Any, Throwable, Byte, String] =
    streamOfObjs(log, numbers) >>> ZPipeline.mapZIO { (bits: BitVector) =>
      ZIO.fromEither(
        utf8
          .decode(bits)
          .toEither
          .left
          .map(e => new RuntimeException(s"utf8 decode for objects $numbers: ${e.messageWithContext}"))
          .map(_.value)
      )
    }

  def dictOfPage(log: Log, page: Int)(bytes: ZStream[Any, Throwable, Byte]): ZStream[Any, Throwable, Prim.Dict] =
    pageObject(log, page)(bytes).collect { case (Obj(_, d: Prim.Dict), _) => d }

  def streamsOfPage(log: Log, page: Int)(bytes: ZStream[Any, Throwable, Byte]): ZStream[Any, Throwable, String] =
    dictOfPage(log, page)(bytes).flatMap { pageDict =>
      Prim.Dict.collectRefs("Contents")(pageDict) match {
        case Attempt.Successful(nums) if nums.nonEmpty =>
          ZStream.fromIterable(nums).flatMap(n => bytes.via(streamTextOfObjs(log, List(n))))
        case _ =>
          ZStream.empty
      }
    }

  def dictOfObj(log: Log, number: Long)(bytes: ZStream[Any, Throwable, Byte]): ZStream[Any, Throwable, Map[String, Prim]] =
    bytes.via(objectNumbers(log, List(number))).collect {
      case (Obj(_, d: Prim.Dict), _) =>
        Map.from(d.data.toList)
    }
}

/** A successfully assembled PDF, plus any non-fatal assembly errors. */
final case class ValidatedPdf(pdf: Pdf, errors: Validation[AssemblyError, Unit])

sealed trait AssemblyError
object AssemblyError {
  case object NoObjs                                                extends AssemblyError
  case object NoXrefs                                                extends AssemblyError
  case object NoOutput                                               extends AssemblyError
  final case class BrokenStream(obj: Obj, error: String)             extends AssemblyError

  def format: AssemblyError => String = {
    case NoObjs                       => "no objects in pdf"
    case NoXrefs                      => "no xrefs in pdf"
    case NoOutput                     => "no output from Assemble"
    case BrokenStream(obj, error)     => s"broken stream for ${obj.index}: $error"
  }
}

object AssemblePdf {

  private final case class State(objs: List[PdfObj], xrefs: List[Xref], errors: List[AssemblyError]) {
    def obj(o: PdfObj): State                = copy(objs = o :: objs)
    def error(e: AssemblyError): State        = copy(errors = e :: errors)
  }

  private val emptyState: State = State(Nil, Nil, Nil)

  private def step(state: State, decoded: Decoded): State = decoded match {
    case Decoded.ContentObj(obj, _, stream) =>
      stream.exec match {
        case Attempt.Successful(s) => state.obj(PdfObj.Content(IndirectObj(obj, Some(s))))
        case Attempt.Failure(c)    => state.error(AssemblyError.BrokenStream(obj, c.messageWithContext))
      }
    case Decoded.Meta(xrefs, _, _) =>
      state.copy(xrefs = xrefs)
    case Decoded.DataObj(obj) =>
      state.obj(PdfObj.Data(obj))
  }

  private def consPdf(state: State): Validation[AssemblyError, ValidatedPdf] = {
    val objs  = NonEmptyChunk.fromIterableOption(state.objs.reverse)
    val xrefs = NonEmptyChunk.fromIterableOption(state.xrefs.reverse)
    (objs, xrefs) match {
      case (Some(o), Some(x)) =>
        val pdf = Pdf(o, x, Trailer.sanitize(x.map(_.trailer)))
        val errs: Validation[AssemblyError, Unit] =
          NonEmptyChunk.fromIterableOption(state.errors) match {
            case None         => Validation.succeed(())
            case Some(errors) =>
              // Concatenate all errors into a single failed Validation.
              errors.tail.foldLeft[Validation[AssemblyError, Unit]](
                Validation.fail(errors.head)
              )((acc, e) => acc.zipParLeft(Validation.fail(e)))
          }
        Validation.succeed(ValidatedPdf(pdf, errs))
      case (None, _) => Validation.fail(AssemblyError.NoObjs)
      case (_, None) => Validation.fail(AssemblyError.NoXrefs)
    }
  }

  /** Run `decoded` to completion and assemble the result. */
  def apply(decoded: ZStream[Any, Throwable, Decoded]): ZIO[Any, Throwable, Validation[AssemblyError, ValidatedPdf]] =
    decoded
      .runFold(emptyState)(step)
      .map(consPdf)
}
