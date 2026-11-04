package io.github.adrielc.kyopdf.demo

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import io.github.adrielc.kyopdf.pipeline.{PdfPipeline, PdfPipelineModel}
import scala.jdk.CollectionConverters.*

/** Stateless MCP JSON endpoint for the local studio's existing PDF runner. */
private[demo] object PipelineTool:
  private val Name = "run_pdf_pipeline"
  private val Protocol = "2025-06-18"
  private val Check = "check_pdf_pipeline"
  private val Explain = "explain_pdf_pipeline"
  private val tools = Vector(
    Name -> "Run a PDF recipe on studio document IDs and return evidence and downloadable PDFs.",
    Check -> "Check recipe fields, references and port types without accessing a PDF.",
    Explain -> "Inspect bound PDFs and predict resolvable branches without rewriting or validating."
  )
  private val definitions = tools.map { (name, description) =>
    s"""{"name":"$name","description":"$description","inputSchema":${PdfPipelineModel.toolSchema(name != Check).print}}"""
  }.mkString("[", ",", "]")

  private def field(obj: JsonObject, key: String): Option[String] =
    Option(obj.get(key)).filter(value => value.isJsonPrimitive && value.getAsJsonPrimitive.isString).map(_.getAsString)

  def handle(bytes: Array[Byte], origin: String)(
      run: (String, PdfPipeline.Checked, Map[String, String]) => (Int, String)
  ): (Int, String) =
    import PaperMachine.quote
    def error(id: String, code: Int, message: String) =
      (200, s"""{"jsonrpc":"2.0","id":$id,"error":{"code":$code,"message":${quote(message)}}}""")
    PipelineJson.parse(bytes) match
      case Left(_) => error("null", -32700, "Invalid or oversized JSON")
      case Right(value) if !value.isJsonObject => error("null", -32600, "Expected a JSON-RPC object")
      case Right(value) =>
        val obj = value.getAsJsonObject
        val id = Option(obj.get("id")).filter(v => v.isJsonPrimitive &&
          (v.getAsJsonPrimitive.isString || v.getAsJsonPrimitive.isNumber)).map(_.toString)
        val params = Option(obj.get("params")).filter(_.isJsonObject).map(_.getAsJsonObject)
        def success(result: String) = (200, s"""{"jsonrpc":"2.0","id":${id.getOrElse("null")},"result":$result}""")
        (field(obj, "jsonrpc"), field(obj, "method"), id) match
          case (Some("2.0"), Some(_), None) if !obj.has("id") => (202, "")
          case (Some("2.0"), Some("initialize"), Some(_)) =>
            success(s"""{"protocolVersion":"$Protocol","capabilities":{"tools":{}},"serverInfo":{"name":"paper-machine","version":"1"}}""")
          case (Some("2.0"), Some("ping"), Some(_)) => success("{}")
          case (Some("2.0"), Some("tools/list"), Some(_)) => success(s"""{"tools":$definitions}""")
          case (Some("2.0"), Some("tools/call"), Some(requestId)) =>
            val invocation = for
              p <- params
              name <- field(p, "name").filter(name => tools.exists(_._1 == name))
              arguments <- Option(p.get("arguments"))
            yield (name, arguments)
            invocation match
              case Some((name, arguments)) => success(call(name, arguments, origin, run))
              case None => error(requestId, -32602, "Expected a known PDF pipeline tool and its arguments")
          case (Some("2.0"), Some(_), Some(requestId)) => error(requestId, -32601, "Unknown method")
          case _ => error("null", -32600, "Invalid JSON-RPC request")

  private def call(name: String, arguments: JsonElement, origin: String,
      run: (String, PdfPipeline.Checked, Map[String, String]) => (Int, String)): String =
    import PaperMachine.quote
    import PdfPipeline.{Code, Issue}
    val withFiles = name != Check
    val invalid = Vector(Issue("request", Code.Structure,
      if withFiles then "Supply a recipe and up to four named document IDs in files" else "Supply only a recipe"))
    val checked = for
      obj <- Option.when(arguments.isJsonObject)(arguments.getAsJsonObject).filter { obj =>
        obj.keySet().asScala.toSet == (if withFiles then Set("recipe", "files") else Set("recipe"))
      }.toRight(invalid)
      files <- if !withFiles then Right(Map.empty[String, String]) else
        Option(obj.get("files")).filter(_.isJsonObject).map(_.getAsJsonObject).filter { entries =>
          entries.size() <= 4 && entries.keySet().asScala.forall(key => field(entries, key).isDefined)
        }.map(entries => entries.keySet().asScala.iterator.map(key => key -> entries.get(key).getAsString).toMap).toRight(invalid)
      plan <- PdfPipelineModel.decode(obj.get("recipe").toString)
    yield (plan, files)
    val (isError, payload, links) = checked match
      case Left(issues) => (true, s"""{"issues":${PipelineJson.issues(issues)}}""", Vector.empty)
      case Right((plan, files)) =>
        val (status, body) = run(name, plan, files)
        val links = if status != 200 || name != Name then Vector.empty else
          JsonParser.parseString(body).getAsJsonObject.getAsJsonArray("outputs").asScala.toVector
            .map(_.getAsJsonObject).filter(field(_, "kind").contains("file")).map { output =>
              val document = output.getAsJsonObject("document")
              val id = document.get("id").getAsString
              val uri = s"$origin/api/documents/$id/file"
              s"""{"type":"resource_link","uri":${quote(uri)},"name":${quote(id + ".pdf")},"mimeType":"application/pdf"}"""
            }
        (status != 200, body, links)
    val content = (s"""{"type":"text","text":${quote(payload)}}""" +: links).mkString("[", ",", "]")
    s"""{"isError":$isError,"content":$content,"structuredContent":$payload}"""
