package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import java.util.concurrent.{Semaphore, TimeUnit}
import scala.util.Using

/** Byte and process budgets are fixed at this local application boundary. */
private[demo] final class ValidationPdf private (val bytes: Array[Byte])
private[demo] object ValidationPdf:
  val MaxBytes = 12 * 1024 * 1024
  def from(bytes: Array[Byte]): Either[String, ValidationPdf] =
    Either.cond(bytes.nonEmpty && bytes.length <= MaxBytes, new ValidationPdf(bytes), "Choose a PDF up to 12 MiB")

object PdfValidation:
  // These profiles share the PDF 1.7 model and can be checked in one veraPDF parse.
  val Profiles = Set("2b", "3b", "3u")
  val slots = new Semaphore(1)
  private val MaxReportBytes = 256 * 1024
  private val TimeoutSeconds = 45L
  private def quote(value: String): String = PaperMachine.quote(value)
  def sha256(bytes: Array[Byte]): String = MessageDigest.getInstance("SHA-256").digest(bytes).map(byte => f"${byte & 255}%02x").mkString
  def declarationJson(value: PdfAIdentification): String =
    def strings(values: Vector[String]): String = values.map(quote).mkString("[", ",", "]")
    val metadata = value.metadata.fold("null")(ref => quote(s"${ref.number}:${ref.generation}"))
    s"{\"state\":${quote(value.state.toString)},\"declares3b\":${value.declares3b},\"parts\":${strings(value.parts)},\"conformances\":${strings(value.conformances)},\"metadata\":$metadata,\"notes\":${strings(value.notes)}}"

  private final case class Workspace(root: Path) extends AutoCloseable:
    val input: Path = root.resolve("document.pdf")
    val output: Path = root.resolve("report.json")
    def close(): Unit =
      val _ = Files.deleteIfExists(input)
      val _ = Files.deleteIfExists(output)
      val _ = Files.deleteIfExists(root)

  private final case class Worker(process: Process) extends AutoCloseable:
    def close(): Unit =
      if process.isAlive then
        val _ = process.destroyForcibly()
        val _ = process.waitFor(2, TimeUnit.SECONDS)

  private def validate(input: ValidationPdf, profiles: Vector[String]): String =
    Using.resource(Workspace(Files.createTempDirectory("paper-machine-validation-"))) { workspace =>
      val _ = Files.write(workspace.input, input.bytes)
      val java = Path.of(System.getProperty("java.home"), "bin", "java").toString
      val command = new ProcessBuilder(java, "-Xmx384m", "-XX:+ExitOnOutOfMemoryError", "-cp", System.getProperty("java.class.path"),
        "io.github.adrielc.kyopdf.demo.ValidationWorker", workspace.input.toString, profiles.mkString(","))
        .redirectOutput(workspace.output.toFile).redirectError(ProcessBuilder.Redirect.DISCARD)
      Using.resource(Worker(command.start())) { worker =>
        if !worker.process.waitFor(TimeoutSeconds, TimeUnit.SECONDS) then
          "{\"error\":\"Validation exceeded 45 seconds. Try a smaller or simpler PDF.\"}"
        else if worker.process.exitValue() != 0 then
          "{\"error\":\"The isolated validator stopped before completion. No conformance result is available.\"}"
        else
          val bytes = Using.resource(Files.newInputStream(workspace.output))(_.readNBytes(MaxReportBytes + 1))
          val text = new String(bytes, UTF_8).trim
          if bytes.length > MaxReportBytes || !text.startsWith("{") || !text.endsWith("}") then
            "{\"error\":\"The validator report was unavailable or exceeded its limit.\"}"
          else text
      }
    }

  private[demo] def run(input: ValidationPdf, profiles: Vector[String]): String =
    require(profiles.nonEmpty && profiles.distinct == profiles && profiles.forall(Profiles.contains), "Choose distinct supported profiles")
    val structural = PaperMachine.evaluate {
      ByteLimit.fromBytes(ValidationPdf.MaxBytes).map { limit =>
        PdfDocument.decode(input.bytes, limit, RetentionLimits(maxObjects = 8000, maxNodes = 150000, maxDepth = 32, maxPayloadBytes = 32L * 1024 * 1024))
          .map(document => PdfPages.pageRefs(document).map(pages => PdfAIdentification.inspect(document).map(found => (pages.length, found))))
      }
    }
    val identification = structural.fold(_ => "{\"state\":\"Unreadable\",\"notes\":[\"Kyo could not inspect the active document metadata\"]}",
      (_, found) => declarationJson(found))
    val readability = structural.fold(message => s"{\"status\":\"unable\",\"message\":${quote(message.take(240))}}",
      (pages, _) => s"{\"status\":\"passed\",\"pages\":$pages}")
    val started = Instant.now().toString
    val report = validate(input, profiles)
    s"{\"sha256\":${quote(sha256(input.bytes))},\"bytes\":${input.bytes.length},\"checkedAt\":${quote(started)},\"engine\":\"veraPDF\",\"engineVersion\":${quote(ValidationWorker.Version)},\"identification\":$identification,\"readability\":$readability,\"validation\":$report}"
