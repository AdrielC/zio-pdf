package io.github.adrielc.kyopdf.pipeline

import _root_.zio.blocks.schema.json.Json
import PdfPipeline.Verdict

/** Expected failures at the materialized runtime-data boundary. */
enum DataIssue:
  case MissingPdf
  case PdfTooLarge(observed: Long, maximum: Long)
  case InvalidPdfHeader
  case MissingReport
  case ReportTooLarge(observed: Long, maximum: Long)
  case ReportTooDeep(maximum: Int)
  case InvalidJson(detail: String)
  case ObjectRequired
  case TooManyJsonNodes(maximum: Int)
  case DuplicateField(name: String)
  case UnknownProfile(code: String)
  case UnrequestedProfile(profile: PdfAProfile)
  case MissingVerdicts
  case TooManyVerdicts(observed: Int, maximum: Int)
  case MissingVerdict(profile: String)

/** The archival profiles supported by the pipeline interpreter. */
enum PdfAProfile(val code: String):
  case PdfA2b extends PdfAProfile("2b")
  case PdfA3b extends PdfAProfile("3b")
  case PdfA3u extends PdfAProfile("3u")

object PdfAProfile:
  def parse(code: String): Either[DataIssue, PdfAProfile] =
    values.find(_.code == code).toRight(DataIssue.UnknownProfile(code))

/** Immutable encoded PDF bytes with a fixed supported maximum and a PDF header.
  * This is a payload guarantee; syntax and PDF/A conformance require inspection.
  * Construction retains the existing immutable vector without copying it.
  */
final class PdfFile private (val bytes: Vector[Byte]):
  def size: Long = bytes.length.toLong
  override def equals(other: Any): Boolean = other match
    case file: PdfFile => bytes == file.bytes
    case _ => false
  override def hashCode(): Int = bytes.hashCode()
  override def toString: String = s"PdfFile($size bytes)"

object PdfFile:
  final val MaxBytes: Int = 64 * 1024 * 1024
  def checkLength(size: Long): Either[DataIssue, Unit] =
    if size < 8 then Left(DataIssue.InvalidPdfHeader)
    else if size > MaxBytes then Left(DataIssue.PdfTooLarge(size, MaxBytes))
    else Right(())

  def checked(bytes: Vector[Byte]): Either[Vector[DataIssue], PdfFile] =
    if bytes == null then Left(Vector(DataIssue.MissingPdf))
    else
      val issues = Vector.newBuilder[DataIssue]
      checkLength(bytes.length.toLong).left.foreach(issues += _)
      if bytes.length >= 8 && (!bytes.startsWith(Vector[Byte](37, 80, 68, 70, 45)) ||
          bytes(5) < 48 || bytes(5) > 57 || bytes(6) != 46 || bytes(7) < 48 || bytes(7) > 57) then
        issues += DataIssue.InvalidPdfHeader
      val found = issues.result()
      if found.nonEmpty then Left(found) else Right(new PdfFile(bytes))

/** Parsed object-shaped report data, bounded before parsing and retained rendering. */
final class ReportJson private (
    val value: Json.Object, val encoded: String, val countedBytes: Long, val nodes: Int):
  override def equals(other: Any): Boolean = other match
    case report: ReportJson => encoded == report.encoded
    case _ => false
  override def hashCode(): Int = encoded.hashCode
  override def toString: String = s"ReportJson($nodes nodes, ${encoded.length} characters)"

object ReportJson:
  final val MaxChars: Int = 1024 * 1024
  final val MaxDepth: Int = 64
  final val MaxNodes: Int = 32768

  def parse(text: String): Either[Vector[DataIssue], ReportJson] =
    if text == null then Left(Vector(DataIssue.MissingReport))
    else if text.length > MaxChars then Left(Vector(DataIssue.ReportTooLarge(text.length.toLong, MaxChars)))
    else
      var depth = 0
      var quoted = false
      var escaped = false
      var tooDeep = false
      text.foreach { c =>
        if quoted then
          if escaped then escaped = false
          else if c == '\\' then escaped = true
          else if c == '"' then quoted = false
        else if c == '"' then quoted = true
        else if c == '[' || c == '{' then
          depth += 1
          if depth > MaxDepth then tooDeep = true
        else if c == ']' || c == '}' then depth -= 1
      }
      if tooDeep then Left(Vector(DataIssue.ReportTooDeep(MaxDepth)))
      else Json.parse(text).left.map(e => Vector(DataIssue.InvalidJson(e.toString))).flatMap {
        case root: Json.Object =>
          val todo = scala.collection.mutable.ArrayDeque[Json](root)
          val issues = Vector.newBuilder[DataIssue]
          var nodes = 0
          var bytes = text.length.toLong * 2
          while todo.nonEmpty && nodes <= MaxNodes do
            val next = todo.removeLast()
            nodes += 1
            next match
              case obj: Json.Object =>
                val seen = scala.collection.mutable.HashSet.empty[String]
                obj.value.foreach { (key, value) =>
                  if !seen.add(key) then issues += DataIssue.DuplicateField(key)
                  bytes += key.length.toLong * 2
                  todo.append(value)
                }
              case arr: Json.Array => arr.value.foreach(todo.append)
              case Json.String(value) => bytes += value.length.toLong * 2
              case _ => ()
          if nodes > MaxNodes then issues += DataIssue.TooManyJsonNodes(MaxNodes)
          val found = issues.result()
          if found.nonEmpty then Left(found) else Right(new ReportJson(root, text, bytes, nodes))
        case _ => Left(Vector(DataIssue.ObjectRequired))
      }

/** Typed validator result; the runner binds it to the exact file it supplied. */
final class VerificationResult private (
    val report: ReportJson, val verdicts: Map[PdfAProfile, Verdict]):
  override def equals(other: Any): Boolean = other match
    case result: VerificationResult => report == result.report && verdicts == result.verdicts
    case _ => false
  override def hashCode(): Int = (report, verdicts).hashCode

object VerificationResult:
  def apply(report: ReportJson, verdicts: Map[PdfAProfile, Verdict]): VerificationResult =
    new VerificationResult(report, verdicts)

  /** Compatibility ingress. Independent JSON and profile failures accumulate. */
  def fromLegacy(json: String, verdicts: Map[String, Verdict]): Either[Vector[DataIssue], VerificationResult] =
    val report = ReportJson.parse(json)
    val parsed =
      if verdicts == null then Vector(Left(DataIssue.MissingVerdicts))
      else if verdicts.size > PdfAProfile.values.length then Vector(Left(DataIssue.TooManyVerdicts(verdicts.size, PdfAProfile.values.length)))
      else verdicts.toVector.sortBy(pair => Option(pair._1).getOrElse("")).map { (code, verdict) =>
        if verdict == null then Left(DataIssue.MissingVerdict(code))
        else PdfAProfile.parse(code).map(_ -> verdict)
      }
    val issues = report.left.toOption.toVector.flatten ++ parsed.collect { case Left(issue) => issue }
    if issues.nonEmpty then Left(issues)
    else Right(new VerificationResult(report.toOption.get, parsed.collect { case Right(value) => value }.toMap))

/** A receipt retains its exact immutable subject, never an unrelated verdict map. */
sealed abstract class PdfReceipt private (val subject: PdfFile, val report: ReportJson):
  override final def equals(other: Any): Boolean = (this, other) match
    case (left: PdfReceipt.Validation, right: PdfReceipt.Validation) => left.subject == right.subject && left.result == right.result
    case (left: PdfReceipt.Inspection, right: PdfReceipt.Inspection) => left.subject == right.subject && left.report == right.report
    case _ => false
  override final def hashCode(): Int = this match
    case receipt: PdfReceipt.Validation => (0, subject, receipt.result).hashCode
    case _: PdfReceipt.Inspection => (1, subject, report).hashCode

object PdfReceipt:
  final class Validation private[pipeline] (file: PdfFile, val result: VerificationResult)
      extends PdfReceipt(file, result.report)
  final class Inspection private[pipeline] (file: PdfFile, report: ReportJson)
      extends PdfReceipt(file, report)
