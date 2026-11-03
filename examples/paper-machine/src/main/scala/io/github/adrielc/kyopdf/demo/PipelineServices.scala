package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*
import kyo.*
import com.google.gson.{JsonArray, JsonParser}
import scala.jdk.CollectionConverters.*

/** One interpreter per run; validations of identical bytes share one profile pass. */
final class PipelineServices(profiles: Vector[String]) extends Services:
  private val receipts = scala.collection.mutable.Map.empty[String, String]
  def validationPasses: Int = receipts.size
  def stamp(document: PdfDocument, text: String, placement: String): PdfDocument < (Abort[PdfError] & Sync) =
    PacketBuild.finish(document, PacketOptions(stamp = text, placement = placement))
  def number(document: PdfDocument, start: Int, style: String, prefix: String): PdfDocument < (Abort[PdfError] & Sync) =
    PacketBuild.finish(document, PacketOptions(numbered = true, start = start, style = style, prefix = prefix))
  def validate(bytes: Vector[Byte], selected: Vector[String]): String < (Abort[PdfError] & Sync) = Sync.defer {
    val data = bytes.toArray
    val hash = PdfValidation.sha256(data)
    val report = receipts.getOrElseUpdate(hash,
      ValidationPdf.from(data).fold(message => throw IllegalArgumentException(message), input => PdfValidation.run(input, profiles)))
    val json = JsonParser.parseString(report).getAsJsonObject
    val validation = json.getAsJsonObject("validation")
    if validation.has("results") then
      val filtered = new JsonArray()
      validation.getAsJsonArray("results").asScala.filter(value => selected.contains(value.getAsJsonObject.get("profile").getAsString))
        .foreach(filtered.add)
      validation.add("results", filtered)
    json.toString
  }
