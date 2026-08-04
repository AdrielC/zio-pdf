/*
 * Composable PDF compliance policy DSL.
 *
 * {{{
 *   import zio.pdf.PdfPolicy.dsl.*
 *
 *   val policy =
 *     banJavaScript &
 *     denyFonts("Courier", "ComicSansMS") &
 *     when(hasEncrypt)(banExternalUri) &
 *     when(hasEmbeddedFile)(reject("no attachments"))
 *
 *   PdfPolicy.eval(policy)(pdf)
 * }}}
 */

package zio.pdf

import zio.prelude.Validation

sealed trait PolicyViolation

object PolicyViolation {
  final case class DeniedFont(obj: Long, baseFont: String)    extends PolicyViolation
  final case class JavaScript(obj: Long, where: String)       extends PolicyViolation
  final case class DangerousAction(obj: Long, action: String) extends PolicyViolation
  final case class EmbeddedFile(obj: Long)                    extends PolicyViolation
  final case class FileAttachment(obj: Long)                  extends PolicyViolation
  final case class ExternalUri(obj: Long, uri: String)        extends PolicyViolation
  final case class Custom(message: String)                    extends PolicyViolation

  def format: PolicyViolation => String = {
    case DeniedFont(n, f)      => s"object $n uses denied font `$f`"
    case JavaScript(n, w)      => s"object $n contains JavaScript ($w)"
    case DangerousAction(n, a) => s"object $n has dangerous action `/S /$a`"
    case EmbeddedFile(n)       => s"object $n is an EmbeddedFile"
    case FileAttachment(n)     => s"object $n is a FileAttachment annotation"
    case ExternalUri(n, u)     => s"object $n has external URI `$u`"
    case Custom(m)             => m
  }
}

/** Precomputed document facts for cheap [[PolicyPred]] tests. */
final case class PolicyFacts(
  fonts: Set[String],
  hasJavaScript: Boolean,
  dangerousActions: Set[String],
  hasEmbeddedFile: Boolean,
  hasFileAttachment: Boolean,
  hasExternalUri: Boolean,
  hasEncrypt: Boolean,
  objectCount: Long
)

final case class PolicyCtx(
  pdf: Pdf,
  byNumber: Map[Long, IndirectObj],
  facts: PolicyFacts
)

/** Boolean predicate over [[PolicyCtx]] — combinable with `&&` / `||` / `!`. */
sealed trait PolicyPred {
  def test(ctx: PolicyCtx): Boolean

  final def &&(other: PolicyPred): PolicyPred = PolicyPred.And(this, other)
  final def ||(other: PolicyPred): PolicyPred = PolicyPred.Or(this, other)
  final def unary_! : PolicyPred              = PolicyPred.Not(this)
}

object PolicyPred {
  final case class Pure(run: PolicyCtx => Boolean) extends PolicyPred {
    def test(ctx: PolicyCtx): Boolean = run(ctx)
  }
  final case class And(a: PolicyPred, b: PolicyPred) extends PolicyPred {
    def test(ctx: PolicyCtx): Boolean = a.test(ctx) && b.test(ctx)
  }
  final case class Or(a: PolicyPred, b: PolicyPred) extends PolicyPred {
    def test(ctx: PolicyCtx): Boolean = a.test(ctx) || b.test(ctx)
  }
  final case class Not(a: PolicyPred) extends PolicyPred {
    def test(ctx: PolicyCtx): Boolean = !a.test(ctx)
  }
}

/**
 * Executable policy AST.
 *
 * - `&` accumulates violations from both sides (conjunction)
 * - `|` passes if either side has zero violations (disjunction)
 * - `when` / `unless` gate a branch on a [[PolicyPred]]
 */
sealed trait Policy {
  final def &(other: Policy): Policy = Policy.All(this, other)
  final def |(other: Policy): Policy = Policy.Any(this, other)

  final def when(cond: PolicyPred): Policy   = Policy.When(cond, this, Policy.Pass)
  final def unless(cond: PolicyPred): Policy = Policy.When(PolicyPred.Not(cond), this, Policy.Pass)
}

object Policy {
  case object Pass extends Policy

  final case class All(a: Policy, b: Policy) extends Policy
  final case class Any(a: Policy, b: Policy) extends Policy
  final case class When(cond: PolicyPred, ifTrue: Policy, ifFalse: Policy) extends Policy
  final case class Check(rule: Builtin) extends Policy
  final case class Pure(run: PolicyCtx => List[PolicyViolation]) extends Policy

  sealed trait Builtin
  object Builtin {
    case object BanJavaScript                                              extends Builtin
    case object BanDangerousActions                                        extends Builtin
    case object BanEmbeddedFiles                                           extends Builtin
    case object BanFileAttachments                                         extends Builtin
    case object BanExternalUri                                             extends Builtin
    final case class DenyFonts(names: Set[String])                         extends Builtin
    final case class BanActions(names: Set[String])                        extends Builtin
  }
}

object PdfPolicy {

  /** Prefer [[dsl]] imports for policy authoring. */
  val dsl: Dsl.type = Dsl

  object Dsl {
    // ---- predicates -------------------------------------------------------

    val hasJavaScript: PolicyPred =
      PolicyPred.Pure(_.facts.hasJavaScript)

    val hasEncrypt: PolicyPred =
      PolicyPred.Pure(_.facts.hasEncrypt)

    val hasEmbeddedFile: PolicyPred =
      PolicyPred.Pure(_.facts.hasEmbeddedFile)

    val hasFileAttachment: PolicyPred =
      PolicyPred.Pure(_.facts.hasFileAttachment)

    val hasExternalUri: PolicyPred =
      PolicyPred.Pure(_.facts.hasExternalUri)

    val hasDangerousAction: PolicyPred =
      PolicyPred.Pure(_.facts.dangerousActions.nonEmpty)

    def usesFont(name: String): PolicyPred =
      PolicyPred.Pure(_.facts.fonts.contains(name))

    def usesAnyFont(names: String*): PolicyPred = {
      val set = names.toSet
      PolicyPred.Pure(ctx => ctx.facts.fonts.exists(set.contains))
    }

    def objectCountAtLeast(n: Long): PolicyPred =
      PolicyPred.Pure(_.facts.objectCount >= n)

    def objectCountLessThan(n: Long): PolicyPred =
      PolicyPred.Pure(_.facts.objectCount < n)

    def pred(f: PolicyCtx => Boolean): PolicyPred =
      PolicyPred.Pure(f)

    // ---- rules ------------------------------------------------------------

    val pass: Policy = Policy.Pass

    val banJavaScript: Policy =
      Policy.Check(Policy.Builtin.BanJavaScript)

    val banDangerousActions: Policy =
      Policy.Check(Policy.Builtin.BanDangerousActions)

    val banEmbeddedFiles: Policy =
      Policy.Check(Policy.Builtin.BanEmbeddedFiles)

    val banFileAttachments: Policy =
      Policy.Check(Policy.Builtin.BanFileAttachments)

    val banExternalUri: Policy =
      Policy.Check(Policy.Builtin.BanExternalUri)

    def denyFonts(names: String*): Policy =
      Policy.Check(Policy.Builtin.DenyFonts(names.toSet))

    def banActions(names: String*): Policy =
      Policy.Check(Policy.Builtin.BanActions(names.toSet))

    def reject(message: String): Policy =
      Policy.Pure(_ => List(PolicyViolation.Custom(message)))

    def check(f: PolicyCtx => List[PolicyViolation]): Policy =
      Policy.Pure(f)

    def when(cond: PolicyPred)(body: Policy): Policy =
      Policy.When(cond, body, Policy.Pass)

    def unless(cond: PolicyPred)(body: Policy): Policy =
      Policy.When(PolicyPred.Not(cond), body, Policy.Pass)

    def ifElse(cond: PolicyPred)(ifTrue: Policy, ifFalse: Policy): Policy =
      Policy.When(cond, ifTrue, ifFalse)

    def allOf(policies: Policy*): Policy =
      policies.foldLeft[Policy](Policy.Pass)(_ & _)

    def anyOf(policies: Policy*): Policy =
      if policies.isEmpty then Policy.Pass
      else policies.reduce(_ | _)

    /**
     * Build a policy from the legacy boolean flags (compat).
     * Prefer composing DSL atoms with `&` / `when`.
     */
    def config(
      denyFonts: Set[String] = Set.empty,
      banJavaScript: Boolean = true,
      banDangerousActions: Boolean = true,
      banEmbeddedFiles: Boolean = false,
      banFileAttachments: Boolean = false,
      banExternalUri: Boolean = false
    ): Policy = {
      val parts = List(
        Option.when(banJavaScript)(Dsl.banJavaScript),
        Option.when(banDangerousActions)(Dsl.banDangerousActions),
        Option.when(banEmbeddedFiles)(Dsl.banEmbeddedFiles),
        Option.when(banFileAttachments)(Dsl.banFileAttachments),
        Option.when(banExternalUri)(Dsl.banExternalUri),
        Option.when(denyFonts.nonEmpty)(Dsl.denyFonts(denyFonts.toSeq*))
      ).flatten
      allOf(parts*)
    }
  }

  /** Default corporate-ish: no JS, no Launch/SubmitForm/…. */
  val strict: Policy =
    Dsl.banJavaScript & Dsl.banDangerousActions

  /** Empty policy — always succeeds. */
  val permissive: Policy = Policy.Pass

  private val defaultDangerous: Set[String] =
    Set("Launch", "SubmitForm", "ImportData", "RichMedia", "GoToE", "GoToR")

  private def nameOf(p: Prim): Option[String] =
    p match {
      case Prim.Name(n) => Some(n)
      case _            => None
    }

  private def stringOf(p: Prim): Option[String] =
    p match {
      case Prim.Str(b)    => Some(new String(b.toArray, "ISO-8859-1"))
      case Prim.HexStr(b) => Some(new String(b.toArray, "ISO-8859-1"))
      case _              => None
    }

  private def combineAll(xs: List[Validation[PolicyViolation, Unit]]): Validation[PolicyViolation, Unit] =
    xs.foldLeft[Validation[PolicyViolation, Unit]](Validation.succeed(()))((a, b) => a.zipParRight(b))

  private def toValidation(vs: List[PolicyViolation]): Validation[PolicyViolation, Unit] =
    if vs.isEmpty then Validation.succeed(())
    else combineAll(vs.map(v => Validation.fail(v): Validation[PolicyViolation, Unit]))

  // ---- fact collection ----------------------------------------------------

  private def collectFacts(pdf: Pdf, byNumber: Map[Long, IndirectObj]): PolicyFacts = {
    var fonts            = Set.empty[String]
    var hasJs            = false
    var dangerous        = Set.empty[String]
    var embed            = false
    var fileAtt          = false
    var uri              = false
    val hasEncrypt       = pdf.trailer.data.data.contains("Encrypt")
    val objectCount      = byNumber.size.toLong

    def scanAction(action: Prim): Unit =
      action match {
        case Prim.Dict(data) =>
          val s = data.get("S").flatMap(nameOf)
          if s.contains("JavaScript") || data.contains("JS") then hasJs = true
          s.foreach { name =>
            if defaultDangerous.contains(name) then dangerous += name
            if name == "URI" then uri = true
          }
          data.toList.foreach { case (_, v) => scanPrim(v) }
        case Prim.Array(es) => es.foreach(scanAction)
        case other          => scanPrim(other)
      }

    def scanPrim(p: Prim): Unit =
      p match {
        case Prim.Dict(data) =>
          data.get("BaseFont").flatMap(nameOf).foreach(fonts += _)
          data.get("Type").flatMap(nameOf).foreach { t =>
            if t == "EmbeddedFile" then embed = true
          }
          data.get("Subtype").flatMap(nameOf).foreach { s =>
            if s == "FileAttachment" then fileAtt = true
          }
          if data.contains("JS") then hasJs = true
          data.get("OpenAction").foreach(scanAction)
          data.get("AA").foreach(scanAction)
          data.get("A").foreach(scanAction)
          data.get("Names") match {
            case Some(Prim.Dict(n)) if n.contains("JavaScript") =>
              hasJs = true
              n.get("JavaScript").foreach(scanPrim)
            case Some(other) => scanPrim(other)
            case None        => ()
          }
          data.get("DescendantFonts") match {
            case Some(Prim.Array(elems)) =>
              elems.foreach {
                case Prim.Ref(n, _) =>
                  byNumber.get(n).foreach {
                    case IndirectObj(Obj(_, child), _) => scanPrim(child)
                  }
                case other => scanPrim(other)
              }
            case Some(other) => scanPrim(other)
            case None        => ()
          }
          data.toList.foreach { case (k, v) =>
            if k != "OpenAction" && k != "AA" && k != "A" && k != "Names" && k != "DescendantFonts" then
              scanPrim(v)
          }
        case Prim.Array(es) => es.foreach(scanPrim)
        case _              => ()
      }

    byNumber.values.foreach {
      case IndirectObj(Obj(_, data), _) => scanPrim(data)
    }
    scanPrim(pdf.trailer.data)

    PolicyFacts(
      fonts = fonts,
      hasJavaScript = hasJs,
      dangerousActions = dangerous,
      hasEmbeddedFile = embed,
      hasFileAttachment = fileAtt,
      hasExternalUri = uri,
      hasEncrypt = hasEncrypt,
      objectCount = objectCount
    )
  }

  def context(pdf: Pdf): PolicyCtx = {
    val byNumber = ValidatePdf.objsByNumber(pdf)
    PolicyCtx(pdf, byNumber, collectFacts(pdf, byNumber))
  }

  // ---- violation collectors (detailed) ------------------------------------

  private def collectJavaScript(ctx: PolicyCtx): List[PolicyViolation] = {
    val out = scala.collection.mutable.ListBuffer.empty[PolicyViolation]
    def action(objNum: Long, where: String)(a: Prim): Unit =
      a match {
        case Prim.Dict(data) =>
          if data.get("S").flatMap(nameOf).contains("JavaScript") || data.contains("JS") then
            out += PolicyViolation.JavaScript(objNum, where)
          data.toList.foreach { case (_, v) => scan(objNum)(v) }
        case Prim.Array(es) => es.foreach(action(objNum, where))
        case other          => scan(objNum)(other)
      }
    def scan(objNum: Long)(p: Prim): Unit =
      p match {
        case Prim.Dict(data) =>
          data.get("OpenAction").foreach(action(objNum, "OpenAction"))
          data.get("AA").foreach(action(objNum, "AA"))
          data.get("A").foreach(action(objNum, "A"))
          if data.contains("JS") && data.get("S").isEmpty then
            out += PolicyViolation.JavaScript(objNum, "JS")
          data.get("Names") match {
            case Some(Prim.Dict(n)) if n.contains("JavaScript") =>
              out += PolicyViolation.JavaScript(objNum, "Names.JavaScript")
              n.get("JavaScript").foreach(scan(objNum))
            case Some(other) => scan(objNum)(other)
            case None        => ()
          }
          data.toList.foreach { case (k, v) =>
            if k != "OpenAction" && k != "AA" && k != "A" && k != "Names" && k != "JS" then scan(objNum)(v)
          }
        case Prim.Array(es) => es.foreach(scan(objNum))
        case _              => ()
      }
    ctx.byNumber.foreach { case (n, IndirectObj(Obj(_, d), _)) => scan(n)(d) }
    scan(0L)(ctx.pdf.trailer.data)
    out.toList
  }

  private def collectActions(ctx: PolicyCtx, banned: Set[String]): List[PolicyViolation] = {
    val out = scala.collection.mutable.ListBuffer.empty[PolicyViolation]
    def action(objNum: Long)(a: Prim): Unit =
      a match {
        case Prim.Dict(data) =>
          data.get("S").flatMap(nameOf).foreach { s =>
            if banned.contains(s) then out += PolicyViolation.DangerousAction(objNum, s)
          }
          data.toList.foreach { case (_, v) => scan(objNum)(v) }
        case Prim.Array(es) => es.foreach(action(objNum))
        case other          => scan(objNum)(other)
      }
    def scan(objNum: Long)(p: Prim): Unit =
      p match {
        case Prim.Dict(data) =>
          data.get("OpenAction").foreach(action(objNum))
          data.get("AA").foreach(action(objNum))
          data.get("A").foreach(action(objNum))
          data.toList.foreach { case (k, v) =>
            if k != "OpenAction" && k != "AA" && k != "A" then scan(objNum)(v)
          }
        case Prim.Array(es) => es.foreach(scan(objNum))
        case _              => ()
      }
    ctx.byNumber.foreach { case (n, IndirectObj(Obj(_, d), _)) => scan(n)(d) }
    scan(0L)(ctx.pdf.trailer.data)
    out.toList
  }

  private def collectFonts(ctx: PolicyCtx, denied: Set[String]): List[PolicyViolation] = {
    val out = scala.collection.mutable.ListBuffer.empty[PolicyViolation]
    def scan(objNum: Long)(p: Prim): Unit =
      p match {
        case Prim.Dict(data) =>
          data.get("BaseFont").flatMap(nameOf).foreach { bf =>
            if denied.contains(bf) then out += PolicyViolation.DeniedFont(objNum, bf)
          }
          data.get("DescendantFonts") match {
            case Some(Prim.Array(elems)) =>
              elems.foreach {
                case Prim.Ref(n, _) =>
                  ctx.byNumber.get(n).foreach {
                    case IndirectObj(Obj(_, child), _) => scan(n)(child)
                  }
                case other => scan(objNum)(other)
              }
            case Some(other) => scan(objNum)(other)
            case None        => ()
          }
          data.toList.foreach { case (k, v) =>
            if k != "DescendantFonts" && k != "BaseFont" then scan(objNum)(v)
          }
        case Prim.Array(es) => es.foreach(scan(objNum))
        case _              => ()
      }
    ctx.byNumber.foreach { case (n, IndirectObj(Obj(_, d), _)) => scan(n)(d) }
    out.toList
  }

  private def collectTypeSubtype(
    ctx: PolicyCtx,
    typ: Option[String],
    subtype: Option[String],
    mk: Long => PolicyViolation
  ): List[PolicyViolation] =
    ctx.byNumber.iterator.collect {
      case (n, IndirectObj(Obj(_, Prim.Dict(data)), _)) =>
        val tOk = typ.forall(t => data.get("Type").flatMap(nameOf).contains(t))
        val sOk = subtype.forall(s => data.get("Subtype").flatMap(nameOf).contains(s))
        if tOk && sOk && (typ.isDefined || subtype.isDefined) then Some(mk(n))
        else None
    }.flatten.toList

  private def collectUri(ctx: PolicyCtx): List[PolicyViolation] = {
    val out = scala.collection.mutable.ListBuffer.empty[PolicyViolation]
    def action(objNum: Long)(a: Prim): Unit =
      a match {
        case Prim.Dict(data) =>
          if data.get("S").flatMap(nameOf).contains("URI") then
            val u = data.get("URI").flatMap(stringOf).getOrElse("<non-string>")
            out += PolicyViolation.ExternalUri(objNum, u)
          data.toList.foreach { case (_, v) => scan(objNum)(v) }
        case Prim.Array(es) => es.foreach(action(objNum))
        case other          => scan(objNum)(other)
      }
    def scan(objNum: Long)(p: Prim): Unit =
      p match {
        case Prim.Dict(data) =>
          data.get("OpenAction").foreach(action(objNum))
          data.get("AA").foreach(action(objNum))
          data.get("A").foreach(action(objNum))
          data.toList.foreach { case (k, v) =>
            if k != "OpenAction" && k != "AA" && k != "A" then scan(objNum)(v)
          }
        case Prim.Array(es) => es.foreach(scan(objNum))
        case _              => ()
      }
    ctx.byNumber.foreach { case (n, IndirectObj(Obj(_, d), _)) => scan(n)(d) }
    scan(0L)(ctx.pdf.trailer.data)
    out.toList
  }

  private def runBuiltin(ctx: PolicyCtx, rule: Policy.Builtin): List[PolicyViolation] =
    rule match {
      case Policy.Builtin.BanJavaScript       => collectJavaScript(ctx)
      case Policy.Builtin.BanDangerousActions => collectActions(ctx, defaultDangerous)
      case Policy.Builtin.BanActions(names)   => collectActions(ctx, names)
      case Policy.Builtin.DenyFonts(names)    => collectFonts(ctx, names)
      case Policy.Builtin.BanEmbeddedFiles =>
        collectTypeSubtype(ctx, Some("EmbeddedFile"), None, PolicyViolation.EmbeddedFile.apply)
      case Policy.Builtin.BanFileAttachments =>
        collectTypeSubtype(ctx, None, Some("FileAttachment"), PolicyViolation.FileAttachment.apply)
      case Policy.Builtin.BanExternalUri => collectUri(ctx)
    }

  /** Evaluate a policy against a prebuilt context. */
  def evalCtx(policy: Policy)(ctx: PolicyCtx): Validation[PolicyViolation, Unit] = {
    def go(p: Policy): Validation[PolicyViolation, Unit] =
      p match {
        case Policy.Pass =>
          Validation.succeed(())
        case Policy.All(a, b) =>
          go(a).zipParRight(go(b))
        case Policy.Any(a, b) =>
          val va = go(a)
          val vb = go(b)
          if va.isSuccess || vb.isSuccess then Validation.succeed(())
          else va.zipParRight(vb)
        case Policy.When(cond, t, f) =>
          if cond.test(ctx) then go(t) else go(f)
        case Policy.Check(rule) =>
          toValidation(runBuiltin(ctx, rule))
        case Policy.Pure(run) =>
          toValidation(run(ctx))
      }
    go(policy)
  }

  def eval(policy: Policy)(pdf: Pdf): Validation[PolicyViolation, Unit] =
    evalCtx(policy)(context(pdf))

  /** Alias for [[eval]]. */
  def apply(policy: Policy)(pdf: Pdf): Validation[PolicyViolation, Unit] =
    eval(policy)(pdf)

  def fromDecoded(
    policy: Policy
  )(decoded: zio.stream.ZStream[Any, Throwable, Decoded]): zio.ZIO[Any, Throwable, Validation[PolicyViolation, Unit]] =
    AssemblePdf(decoded).map {
      _.fold(_ => Validation.succeed(()), { case ValidatedPdf(pdf, _) => eval(policy)(pdf) })
    }

  def fromChunk(policy: Policy)(decoded: zio.Chunk[Decoded]): Validation[PolicyViolation, Unit] =
    AssemblePdf.fromChunk(decoded).fold(
      _ => Validation.succeed(()),
      { case ValidatedPdf(pdf, _) => eval(policy)(pdf) }
    )
}
