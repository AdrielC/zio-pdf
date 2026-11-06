package io.github.adrielc.kyopdf.pipeline

import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import kyo.*
import zio.blocks.schema.json.Json
import zio.blocks.schema.{Schema as BlocksSchema}
import zio.blocks.schema.json.JsonCodecDeriver
import StepRegistry.{Error, ErrorCode}
import PdfSteps.*

/** Optional local worker adapter shared by JVM and Native. Executable and script
  * paths are operator configuration; recipes cannot execute commands or use URLs.
  * PyMuPDF is an optional, separately installed AGPL/commercial dependency.
  */
final class ProcessSemanticBackend(python: Path, worker: Path, timeoutSeconds: Int = 120) extends SemanticBackend:
  require(timeoutSeconds >= 1 && timeoutSeconds <= 600)
  private final val MaxReportBytes = 64 * 1024
  private final val MaxFileBytes = 12 * 1024 * 1024
  private final case class WorkerRequest(operation: String, settings: Json, inputSha256: String) derives BlocksSchema
  private final case class RedactSettings(dpi: Int, plan: RedactionPlan) derives BlocksSchema
  private lazy val requestCodec = BlocksSchema[WorkerRequest].derive(JsonCodecDeriver)
  private lazy val redactCodec = BlocksSchema[RedactSettings].derive(JsonCodecDeriver)
  private class Workspace(val path: Path):
    val input = path.resolve("input.pdf")
    val output = path.resolve("output.pdf")
    val request = path.resolve("request.json")
    val report = path.resolve("report.json")
    def close(): Unit =
      Vector(input, output, request, report).foreach(Files.deleteIfExists)
      Files.deleteIfExists(path)
      ()
  private def io[A](body: => A): A < (Abort[Error] & Sync) =
    Abort.run[Exception](Abort.catching[Exception](Sync.defer(body))).map {
      case Result.Success(value) => value
      case _ => Abort.fail(Error(ErrorCode.Backend, "Semantic worker I/O failed"))
    }
  private def read(path: Path, maximum: Int): Array[Byte] < (Abort[Error] & Sync) =
    io(Files.size(path)).map { size =>
      if size < 1 || size > maximum then Abort.fail(Error(ErrorCode.Limit, "Semantic worker result exceeds its byte limit"))
      else io(Files.newInputStream(path)).map { stream =>
        SyncResource.ensure[Error](Abort.catching[java.io.IOException](Sync.defer(stream.close()))) {
          io(stream.readNBytes(maximum + 1)).map { bytes =>
            if bytes.length > maximum then Abort.fail(Error(ErrorCode.Limit, "Semantic worker result grew past its byte limit")) else bytes
          }
        }
      }
    }
  private def work[A](operation: String, file: PdfFile, settings: Json)(accept: (Json.Object, Workspace) => A < (Abort[Error] & Sync)): A < (Abort[Error] & Sync) =
    if file.size > MaxFileBytes then Abort.fail(Error(ErrorCode.Limit, "Semantic worker accepts at most 12 MiB"))
    else io(new Workspace(Files.createTempDirectory("kyo-semantic-"))).map { workspace =>
      SyncResource.ensure[Error](Abort.catching[Exception](Sync.defer(workspace.close()))) {
        io {
          val stream = Files.newOutputStream(workspace.input)
          stream.close()
          // The payload is already admitted by PdfFile; avoid a second whole-file array.
          file.bytes.iterator.grouped(ExecutionPlanner.MaxChunkBytes).foreach { chunk =>
            Files.write(workspace.input, chunk.toArray, java.nio.file.StandardOpenOption.APPEND)
          }
          Files.writeString(workspace.request, requestCodec.encodeToString(WorkerRequest(operation, settings, sha256(file))))
          new ProcessBuilder(python.toAbsolutePath.toString, worker.toAbsolutePath.toString,
            workspace.path.toString).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start()
        }.map { process =>
          SyncResource.ensure[Error](Sync.defer {
            if process.isAlive then
              val _ = process.destroyForcibly()
              val _ = process.waitFor(5L, TimeUnit.SECONDS)
            ()
          }) {
            io(process.waitFor(timeoutSeconds.toLong, TimeUnit.SECONDS)).map { done =>
              if !done then Abort.fail(Error(ErrorCode.Backend, "Semantic worker timed out"))
              else read(workspace.report, MaxReportBytes).map { bytes =>
                ReportJson.parse(new String(bytes, java.nio.charset.StandardCharsets.UTF_8)) match
                  case Left(_) => Abort.fail(Error(ErrorCode.Verification, "Invalid semantic worker receipt"))
                  case Right(report) =>
                    val fields = report.value.value.toMap
                    fields.get("error") match
                      case Some(Json.String(code)) =>
                        val category = ErrorCode.values.find(_.toString == code).getOrElse(ErrorCode.Backend)
                        Abort.fail(Error(category, fields.get("message").collect { case Json.String(s) => s.take(300) }.getOrElse("Semantic worker rejected input")))
                      case _ if process.exitValue() != 0 => Abort.fail(Error(ErrorCode.Backend, "Semantic worker exited unsuccessfully"))
                      case _ => accept(report.value, workspace)
              }
            }
          }
        }
      }
    }
  private def params[A](schema: StepRegistry.DataSchema[A], value: A): Json =
    Json.parseUnsafe(schema.schema.derive(JsonCodecDeriver.withTransientDefaultValue(false)).encodeToString(value))
  private def artifact(operation: String, file: PdfFile, settings: Json): SemanticBackend.Artifact < (Abort[Error] & Sync) =
    work(operation, file, settings) { (report, workspace) =>
      read(workspace.output, MaxFileBytes).map { bytes =>
        PdfFile.checked(bytes.toVector) match
          case Left(_) => Abort.fail(Error(ErrorCode.Verification, "Worker did not return a PDF"))
          case Right(output) =>
            val fields = report.value.toMap
            def text(key: String) = fields.get(key).collect { case Json.String(value) => value }.getOrElse("")
            val verified = fields.get("verified").exists {
              case Json.Boolean(value) => value
              case _ => false
            }
            SemanticBackend.Artifact(output, text("inputSha256"), text("outputSha256"), text("operation"), verified)
      }
    }
  def locate(file: PdfFile, settings: Select): Selection < (Abort[Error] & Sync) =
    work("locate", file, Json.parseUnsafe(selectSchema.encode(settings))) { (report, _) =>
      report.value.toMap.get("selection") match
        case Some(value) => PdfSteps.selection.decode(value.print).fold(Abort.fail, identity)
        case None => Abort.fail(Error(ErrorCode.Verification, "Worker did not return a selection"))
    }
  def redact(file: PdfFile, plan: RedactionPlan, settings: Edit) =
    artifact("redact", file, Json.parseUnsafe(redactCodec.encodeToString(RedactSettings(settings.dpi, plan))))
  def replace(file: PdfFile, settings: Replace) = artifact("replace", file, params(PdfSteps.replaceText.parameters, settings))
  def ocr(file: PdfFile, settings: Ocr) = artifact("ocr", file, params(PdfSteps.ocr.parameters, settings))
  def cleanup(file: PdfFile, settings: Edit) = artifact("cleanup", file, params(PdfSteps.cleanup.parameters, settings))
  private lazy val selectSchema = new StepRegistry.DataSchema("select", 1, zio.blocks.schema.Schema.derived[Select])
