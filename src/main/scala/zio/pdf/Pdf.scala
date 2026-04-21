/*
 * Port of fs2.pdf.Pdf to Scala 3 + ZIO. The legacy file mixed the
 * `Pdf` data type with the `AssemblePdf` builder *and* a bunch of
 * convenience pipes (`pageNumber`, `objectNumbers`, `streamsOfPage`,
 * etc.). The convenience pipes are nice-to-have but the core is
 * the assembled PDF + its associated error model, which is what
 * Validate/Compare both consume. Everything else can be added back
 * incrementally as needed.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.{NonEmptyChunk, ZIO}
import zio.prelude.Validation
import zio.stream.ZStream

sealed trait PdfObj

object PdfObj {
  final case class Content(obj: IndirectObj) extends PdfObj
  final case class Data(obj: Obj)             extends PdfObj
}

/** A fully-assembled PDF in memory: the objects, the xref tables,
  * the trailer. */
final case class Pdf(objs: NonEmptyChunk[PdfObj], xrefs: NonEmptyChunk[Xref], trailer: Trailer)

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
