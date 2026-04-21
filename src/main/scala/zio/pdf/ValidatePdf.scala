/*
 * Port of fs2.pdf.ValidatePdf + ComparePdfs to Scala 3 + ZIO.
 *
 * Replaces cats Validated with zio.prelude.Validation. The
 * accumulating ApplicativeError shape that legacy code used via
 * cats's Validated[NonEmptyList[E], A] is exactly Validation[E, A]
 * in zio-prelude.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.{NonEmptyChunk, ZIO}
import zio.prelude.Validation
import zio.stream.ZStream

final case class ContentRef(owner: Long, target: Long)

final case class Refs(contents: List[ContentRef]) {
  def content(add: List[ContentRef]): Refs = copy(contents = contents ++ add)
}

sealed trait PdfError

object PdfError {
  final case class Assembly(error: AssemblyError)        extends PdfError
  final case class ContentMissing(ref: ContentRef)        extends PdfError
  final case class ContentStreamMissing(ref: ContentRef)  extends PdfError
  final case class PageMissing(num: Long)                 extends PdfError
  final case class InvalidPageTreeObject(obj: IndirectObj) extends PdfError
  case object NoPages                                      extends PdfError
  case object NoRoot                                       extends PdfError
  case object NoCatalog                                    extends PdfError
  case object NoPagesInCatalog                             extends PdfError
  case object NoPageRoot                                   extends PdfError

  def format: PdfError => String = {
    case Assembly(e)                       => AssemblyError.format(e)
    case ContentMissing(ContentRef(o, t))  => s"content object $t missing (owner $o)"
    case ContentStreamMissing(ContentRef(o, t)) => s"content object $t has no stream (owner $o)"
    case PageMissing(n)                    => s"page $n doesn't exist"
    case InvalidPageTreeObject(o)          => s"object $o is neither Pages nor Page"
    case NoPages                           => "no Pages objects in the catalog"
    case NoRoot                            => "no Root in trailer"
    case NoCatalog                         => "couldn't find catalog"
    case NoPagesInCatalog                  => "no Pages in catalog"
    case NoPageRoot                        => "couldn't find root Pages"
  }
}

object ValidatePdf {

  def collectRefFromDict(owner: Long, dict: Prim.Dict): List[ContentRef] =
    Prim.Dict
      .collectRefs("Contents")(dict)
      .toOption
      .map(_.map(ContentRef(owner, _)))
      .getOrElse(Nil)

  def collectRefs(z: Refs, obj: PdfObj): Refs =
    obj match {
      case PdfObj.Content(IndirectObj.dict(number, dict)) =>
        z.content(collectRefFromDict(number, dict))
      case PdfObj.Data(Obj.dict(number, dict)) =>
        z.content(collectRefFromDict(number, dict))
      case _ =>
        z
    }

  def indirect(pdf: Pdf): List[IndirectObj] =
    pdf.objs.toList.collect {
      case PdfObj.Content(obj) => obj
      case PdfObj.Data(obj)    => IndirectObj(Obj(obj.index, obj.data), None)
    }

  def validateContentStream(byNumber: Map[Long, IndirectObj]): ContentRef => Validation[PdfError, Unit] = {
    case ref @ ContentRef(owner, target) =>
      byNumber.get(target) match {
        case None =>
          Validation.fail(PdfError.ContentMissing(ref))
        case Some(IndirectObj(Obj(_, Prim.refs(refs)), None)) =>
          combineAll(refs.map(r => validateContentStream(byNumber)(ContentRef(owner, r.number))))
        case Some(IndirectObj(_, None)) =>
          Validation.fail(PdfError.ContentStreamMissing(ref))
        case _ =>
          Validation.succeed(())
      }
  }

  def collectPages(byNumber: Map[Long, IndirectObj])(
    root: IndirectObj
  ): Validation[PdfError, (List[Page], NonEmptyChunk[Pages])] = {
    def spin(obj: IndirectObj): Validation[PdfError, (List[Page], List[Pages])] =
      obj match {
        case Pages.obj(p @ Pages(_, _, kids, _)) =>
          val children = kids.flatMap(k => byNumber.get(k.number).fold[List[IndirectObj]](Nil)(List(_)))
          val missingErrors: List[Validation[PdfError, Unit]] =
            kids
              .filterNot(k => byNumber.contains(k.number))
              .map(k => Validation.fail[PdfError](PdfError.PageMissing(k.number)))
          val collected: Validation[PdfError, (List[Page], List[Pages])] =
            children.foldLeft[Validation[PdfError, (List[Page], List[Pages])]](
              Validation.succeed((Nil, Nil))
            ) { (acc, child) =>
              // zipPar of (A, B) with (C, D) is flattened by Zippable
              // into (A, B, C, D); explicitly recompose.
              acc.zipWithPar(spin(child)) { case ((pa, pb), (ca, cb)) =>
                (pa ++ ca, pb ++ cb)
              }
            }
          combineAll(missingErrors).zipParRight(collected.map { case (pa, pb) => (pa, p :: pb) })
        case Page.obj(page) =>
          Validation.succeed((List(page), Nil))
        case other =>
          Validation.fail(PdfError.InvalidPageTreeObject(other))
      }
    spin(root).flatMap {
      case (_, Nil)     => Validation.fail(PdfError.NoPages)
      case (ps, h :: t) => Validation.succeed((ps, NonEmptyChunk(h, t*)))
    }
  }

  private def opt[A](error: PdfError)(oa: Option[A]): Validation[PdfError, A] =
    oa.fold[Validation[PdfError, A]](Validation.fail(error))(Validation.succeed(_))

  private def att[A](error: PdfError)(aa: Attempt[A]): Validation[PdfError, A] =
    opt(error)(aa.toOption)

  private def combineAll[E](xs: List[Validation[E, Unit]]): Validation[E, Unit] =
    xs.foldLeft[Validation[E, Unit]](Validation.succeed(()))((acc, v) => acc.zipParRight(v))

  def validatePages(byNumber: Map[Long, IndirectObj], pdf: Pdf): Validation[PdfError, Unit] =
    opt(PdfError.NoRoot)(pdf.trailer.root).flatMap { catRef =>
      opt(PdfError.NoCatalog)(byNumber.get(catRef.number))
    }.flatMap { cat =>
      att(PdfError.NoPagesInCatalog)(
        Prim.Dict.path("Pages")(cat.obj.data) { case r @ Prim.Ref(_, _) => r }
      )
    }.flatMap { rootRef =>
      opt(PdfError.NoPageRoot)(byNumber.get(rootRef.number))
    }.flatMap(collectPages(byNumber)).map(_ => ())

  def validateContentStreams(byNumber: Map[Long, IndirectObj], refs: Refs): Validation[PdfError, Unit] =
    combineAll(refs.contents.map(validateContentStream(byNumber)))

  def objsByNumber(pdf: Pdf): Map[Long, IndirectObj] =
    indirect(pdf).map(o => (o.obj.index.number, o)).toMap

  def apply(pdf: Pdf): Validation[PdfError, Unit] = {
    val byNumber = objsByNumber(pdf)
    val refs     = pdf.objs.foldLeft(Refs(Nil))(collectRefs)
    validateContentStreams(byNumber, refs).zipParRight(validatePages(byNumber, pdf))
  }

  def fromDecoded(decoded: ZStream[Any, Throwable, Decoded]): ZIO[Any, Throwable, Validation[PdfError, Unit]] =
    AssemblePdf(decoded).map { v =>
      v.mapError(e => PdfError.Assembly(e): PdfError).flatMap { case ValidatedPdf(pdf, errors) =>
        errors.mapError(e => PdfError.Assembly(e): PdfError).zipParRight(apply(pdf))
      }
    }
}

sealed trait CompareError

object CompareError {
  final case class DeletedStream(num: Long, data: Prim)     extends CompareError
  final case class ObjectMissing(num: Long, obj: IndirectObj) extends CompareError
  final case class ObjectAdded(num: Long, obj: IndirectObj)   extends CompareError
  final case class Assembly(error: AssemblyError)             extends CompareError
  final case class Validation(error: PdfError)                extends CompareError

  def format: CompareError => String = {
    case DeletedStream(n, d)  => s"stream deleted from object $n:\n$d"
    case ObjectMissing(n, o)  => s"object $n missing from updated pdf:\n$o"
    case ObjectAdded(n, o)    => s"object $n added to updated pdf:\n$o"
    case Assembly(e)          => AssemblyError.format(e)
    case Validation(e)        => PdfError.format(e)
  }
}

object ComparePdfs {

  def compareObjs(num: Long, pair: (Option[IndirectObj], Option[IndirectObj])): Validation[CompareError, Unit] =
    pair match {
      case (Some(IndirectObj(Obj(_, data), Some(_))), Some(IndirectObj(_, None))) =>
        Validation.fail(CompareError.DeletedStream(num, data))
      case (Some(obj), None) =>
        Validation.fail(CompareError.ObjectMissing(num, obj))
      case (None, Some(obj)) =>
        Validation.fail(CompareError.ObjectAdded(num, obj))
      case (Some(_), Some(_)) | (None, None) =>
        Validation.succeed(())
    }

  def fromDecoded(
    oldDecoded: ZStream[Any, Throwable, Decoded],
    updatedDecoded: ZStream[Any, Throwable, Decoded]
  ): ZIO[Any, Throwable, Validation[CompareError, Unit]] =
    AssemblePdf(oldDecoded).zipPar(AssemblePdf(updatedDecoded)).map { case (oldV, newV) =>
      oldV.zipPar(newV).mapError(e => CompareError.Assembly(e): CompareError).flatMap {
        case (ValidatedPdf(oldPdf, _), ValidatedPdf(newPdf, newErrors)) =>
          val oldByNumber = ValidatePdf.objsByNumber(oldPdf)
          val newByNumber = ValidatePdf.objsByNumber(newPdf)
          val keys        = oldByNumber.keySet ++ newByNumber.keySet
          val compared    = keys.toList.foldLeft[Validation[CompareError, Unit]](Validation.succeed(())) {
            (acc, num) =>
              acc.zipParRight(compareObjs(num, (oldByNumber.get(num), newByNumber.get(num))))
          }
          val newErrorsAsCompare = newErrors.mapError(e => CompareError.Assembly(e): CompareError)
          val validated = ValidatePdf(newPdf).mapError(e => CompareError.Validation(e): CompareError)
          newErrorsAsCompare.zipParRight(validated).zipParRight(compared)
      }
    }

  def fromBytes(log: Log)(
    oldBytes: ZStream[Any, Throwable, Byte],
    updatedBytes: ZStream[Any, Throwable, Byte]
  ): ZIO[Any, Throwable, Validation[CompareError, Unit]] =
    fromDecoded(oldBytes.via(PdfStream.decode(log)), updatedBytes.via(PdfStream.decode(log)))
}
