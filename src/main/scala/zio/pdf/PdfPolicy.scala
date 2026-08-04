/*
 * Structural compliance rules over an assembled PDF — fonts, JS/actions,
 * optional embeds. Accumulating [[zio.prelude.Validation]].
 */

package zio.pdf

import zio.prelude.Validation

sealed trait PolicyViolation

object PolicyViolation {
  final case class DeniedFont(obj: Long, baseFont: String)      extends PolicyViolation
  final case class JavaScript(obj: Long, where: String)         extends PolicyViolation
  final case class DangerousAction(obj: Long, action: String)   extends PolicyViolation
  final case class EmbeddedFile(obj: Long)                      extends PolicyViolation
  final case class FileAttachment(obj: Long)                    extends PolicyViolation
  final case class ExternalUri(obj: Long, uri: String)          extends PolicyViolation

  def format: PolicyViolation => String = {
    case DeniedFont(n, f)      => s"object $n uses denied font `$f`"
    case JavaScript(n, w)      => s"object $n contains JavaScript ($w)"
    case DangerousAction(n, a) => s"object $n has dangerous action `/S /$a`"
    case EmbeddedFile(n)       => s"object $n is an EmbeddedFile"
    case FileAttachment(n)     => s"object $n is a FileAttachment annotation"
    case ExternalUri(n, u)     => s"object $n has external URI `$u`"
  }
}

final case class PdfPolicy(
  denyFonts: Set[String] = Set.empty,
  banJavaScript: Boolean = true,
  banDangerousActions: Boolean = true,
  banEmbeddedFiles: Boolean = false,
  banFileAttachments: Boolean = false,
  banExternalUri: Boolean = false
)

object PdfPolicy {

  val strict: PdfPolicy = PdfPolicy()

  val permissive: PdfPolicy =
    PdfPolicy(banJavaScript = false, banDangerousActions = false)

  private val dangerousS: Set[String] =
    Set("Launch", "SubmitForm", "ImportData", "RichMedia", "GoToE", "GoToR")

  private def combineAll(xs: List[Validation[PolicyViolation, Unit]]): Validation[PolicyViolation, Unit] =
    xs.foldLeft[Validation[PolicyViolation, Unit]](Validation.succeed(()))((a, b) => a.zipParRight(b))

  private def nameOf(p: Prim): Option[String] =
    p match {
      case Prim.Name(n) => Some(n)
      case _            => None
    }

  private def walkPrim(objNum: Long, rules: PdfPolicy)(p: Prim): List[Validation[PolicyViolation, Unit]] =
    p match {
      case d @ Prim.Dict(_)  => walkDict(objNum, rules)(d)
      case Prim.Array(elems) => elems.toList.flatMap(walkPrim(objNum, rules))
      case _                 => Nil
    }

  private def actionViolations(objNum: Long, rules: PdfPolicy, where: String)(
    action: Prim
  ): List[Validation[PolicyViolation, Unit]] =
    action match {
      case Prim.Dict(data) =>
        val sName = data.get("S").flatMap(nameOf)
        val js =
          if rules.banJavaScript && (sName.contains("JavaScript") || data.contains("JS")) then
            List(Validation.fail(PolicyViolation.JavaScript(objNum, where)): Validation[PolicyViolation, Unit])
          else Nil
        val danger =
          if rules.banDangerousActions then
            sName.filter(dangerousS.contains).toList.map { a =>
              Validation.fail(PolicyViolation.DangerousAction(objNum, a)): Validation[PolicyViolation, Unit]
            }
          else Nil
        val uri =
          if rules.banExternalUri && sName.contains("URI") then
            data.get("URI") match {
              case Some(Prim.Str(b)) =>
                List(
                  Validation.fail(
                    PolicyViolation.ExternalUri(objNum, new String(b.toArray, "ISO-8859-1"))
                  ): Validation[PolicyViolation, Unit]
                )
              case Some(Prim.HexStr(b)) =>
                List(
                  Validation.fail(
                    PolicyViolation.ExternalUri(objNum, new String(b.toArray, "ISO-8859-1"))
                  ): Validation[PolicyViolation, Unit]
                )
              case Some(_) =>
                List(Validation.fail(PolicyViolation.ExternalUri(objNum, "<non-string>")): Validation[PolicyViolation, Unit])
              case None => Nil
            }
          else Nil
        js ++ danger ++ uri ++ data.toList.map(_._2).flatMap(walkPrim(objNum, rules))
      case Prim.Array(elems) =>
        elems.toList.flatMap(actionViolations(objNum, rules, where))
      case other =>
        walkPrim(objNum, rules)(other)
    }

  private def walkDict(objNum: Long, rules: PdfPolicy)(dict: Prim.Dict): List[Validation[PolicyViolation, Unit]] = {
    val data = dict.data
    val fonts =
      data.get("BaseFont").flatMap(nameOf).toList.flatMap { bf =>
        if rules.denyFonts.contains(bf) then
          List(Validation.fail(PolicyViolation.DeniedFont(objNum, bf)): Validation[PolicyViolation, Unit])
        else Nil
      }

    val typeName = data.get("Type").flatMap(nameOf)
    val subtype  = data.get("Subtype").flatMap(nameOf)

    val embed =
      if rules.banEmbeddedFiles && typeName.contains("EmbeddedFile") then
        List(Validation.fail(PolicyViolation.EmbeddedFile(objNum)): Validation[PolicyViolation, Unit])
      else Nil

    val fileAnnot =
      if rules.banFileAttachments && subtype.contains("FileAttachment") then
        List(Validation.fail(PolicyViolation.FileAttachment(objNum)): Validation[PolicyViolation, Unit])
      else Nil

    val openAction =
      data.get("OpenAction").toList.flatMap(actionViolations(objNum, rules, "OpenAction"))
    val aa =
      data.get("AA").toList.flatMap(actionViolations(objNum, rules, "AA"))
    val annotA =
      data.get("A").toList.flatMap(actionViolations(objNum, rules, "A"))

    val jsBare =
      if rules.banJavaScript && data.contains("JS") && data.get("S").isEmpty then
        List(Validation.fail(PolicyViolation.JavaScript(objNum, "JS")): Validation[PolicyViolation, Unit])
      else Nil

    val namesJs =
      data.get("Names") match {
        case Some(Prim.Dict(n)) if rules.banJavaScript && n.contains("JavaScript") =>
          List(Validation.fail(PolicyViolation.JavaScript(objNum, "Names.JavaScript")): Validation[PolicyViolation, Unit]) ++
            n.get("JavaScript").toList.flatMap(walkPrim(objNum, rules))
        case Some(other) => walkPrim(objNum, rules)(other)
        case None        => Nil
      }

    val skip = Set("OpenAction", "AA", "A", "Names", "BaseFont", "JS")
    val rest = data.toList.collect {
      case (k, v) if !skip.contains(k) => v
    }.flatMap(walkPrim(objNum, rules))

    fonts ++ embed ++ fileAnnot ++ openAction ++ aa ++ annotA ++ jsBare ++ namesJs ++ rest
  }

  def apply(rules: PdfPolicy)(pdf: Pdf): Validation[PolicyViolation, Unit] = {
    val byNumber = ValidatePdf.objsByNumber(pdf)
    val perObj = byNumber.iterator.flatMap { case (num, IndirectObj(Obj(_, data), _)) =>
      val direct = walkPrim(num, rules)(data)
      val fromDescendants = data match {
        case Prim.Dict(d) =>
          d.get("DescendantFonts") match {
            case Some(Prim.Array(elems)) =>
              elems.toList.flatMap {
                case Prim.Ref(n, _) =>
                  byNumber.get(n).toList.flatMap {
                    case IndirectObj(Obj(_, child), _) => walkPrim(n, rules)(child)
                  }
                case other => walkPrim(num, rules)(other)
              }
            case _ => Nil
          }
        case _ => Nil
      }
      direct ++ fromDescendants
    }.toList

    combineAll(perObj ++ walkPrim(0L, rules)(pdf.trailer.data))
  }

  def fromDecoded(
    rules: PdfPolicy
  )(decoded: zio.stream.ZStream[Any, Throwable, Decoded]): zio.ZIO[Any, Throwable, Validation[PolicyViolation, Unit]] =
    AssemblePdf(decoded).map {
      _.fold(_ => Validation.succeed(()), { case ValidatedPdf(pdf, _) => apply(rules)(pdf) })
    }

  def fromChunk(rules: PdfPolicy)(decoded: zio.Chunk[Decoded]): Validation[PolicyViolation, Unit] =
    AssemblePdf.fromChunk(decoded).fold(
      _ => Validation.succeed(()),
      { case ValidatedPdf(pdf, _) => apply(rules)(pdf) }
    )
}
