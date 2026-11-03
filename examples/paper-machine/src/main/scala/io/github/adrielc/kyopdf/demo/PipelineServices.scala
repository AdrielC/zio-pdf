package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*
import kyo.*
import com.google.gson.{JsonArray, JsonParser}
import scala.jdk.CollectionConverters.*

/** Static graphs batch profiles. Conditional graphs check only demanded profiles;
  * an existing receipt is reused when it already covers those exact bytes/checks.
  */
object PipelineServices:
  val BusyMessage = "Another validation is running. Wait for it to finish."

final class PipelineServices(profiles: Vector[String]) extends Services:
  private val receipts = scala.collection.mutable.ArrayBuffer.empty[(String, Set[String], String)]
  def validationPasses: Int = receipts.size
  def validationRuns: Vector[(String, Vector[String])] = receipts.toVector.map((hash, checked, _) => hash -> checked.toVector.sorted)
  def stamp(document: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) =
    PacketBuild.finish(document, PacketOptions(stamp = text, placement = placement))
  def number(document: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) =
    PacketBuild.finish(document, PacketOptions(numbered = true, start = start, style = style, prefix = prefix))
  def validate(bytes: Vector[Byte], selected: Vector[String]): String < (Abort[PdfError] & Sync) =
    Abort.catching[Exception](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse("Validation could not finish")))(Sync.defer {
    val data = bytes.toArray
    val hash = PdfValidation.sha256(data)
    val requested = if profiles.nonEmpty then profiles else selected
    val report = receipts.find((digest, checked, _) => digest == hash && selected.toSet.subsetOf(checked)).map(_._3).getOrElse {
      if receipts.length >= 3 then throw IllegalArgumentException("A run may start at most three validation passes. Combine profiles in each validation node.")
      if !PdfValidation.slots.tryAcquire() then throw IllegalStateException(PipelineServices.BusyMessage)
      try
        val result = ValidationPdf.from(data).fold(message => throw IllegalArgumentException(message), input => PdfValidation.run(input, requested))
        receipts += ((hash, requested.toSet, result))
        result
      finally PdfValidation.slots.release()
    }
    val json = JsonParser.parseString(report).getAsJsonObject
    val validation = json.getAsJsonObject("validation")
    if validation.has("results") then
      val filtered = new JsonArray()
      validation.getAsJsonArray("results").asScala.filter(value => selected.contains(value.getAsJsonObject.get("profile").getAsString))
        .foreach(filtered.add)
      validation.add("results", filtered)
    json.toString
  })
  override def validationReport(bytes: Vector[Byte], selected: Vector[String]): ValidationReport < (Abort[PdfError] & Sync) =
    validate(bytes, selected).map { json =>
      val validation = JsonParser.parseString(json).getAsJsonObject.getAsJsonObject("validation")
      val results = if validation.has("results") && !validation.has("error") then validation.getAsJsonArray("results").asScala.toVector else Vector.empty
      val verdicts = selected.map { profile =>
        val matches = results.filter(_.getAsJsonObject.get("profile").getAsString == profile)
        val verdict = if matches.length != 1 then Verdict.Unable else matches.head.getAsJsonObject.get("status").getAsString match
          case "passed" => Verdict.Passed
          case "failed" => Verdict.Failed
          case _ => Verdict.Unable
        profile -> verdict
      }.toMap
      ValidationReport(json, verdicts)
    }
