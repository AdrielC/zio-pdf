package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import kyo.*
import io.github.adrielc.kyopdf.pipeline.{CourtProfile, Policy, PdfPipeline, PdfPipelineModel}
import scala.jdk.CollectionConverters.*
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import java.io.ByteArrayOutputStream
import java.net.{InetSocketAddress, URLDecoder}
import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_8}
import java.util.UUID
import java.util.concurrent.Executors
import javax.imageio.ImageIO
import org.apache.pdfbox.Loader
import org.apache.pdfbox.rendering.{ImageType, PDFRenderer}
import scala.collection.mutable
import scala.util.Using

/** Bounded, loopback-only example. Core operations are interpreted at this HTTP edge. */
object PaperMachine:
  private val MaxInput = 12 * 1024 * 1024
  private val MaxPages = 64
  private final case class Document(id: String, name: String, bytes: Array[Byte], value: PdfDocument, pages: Vector[ObjectRef], identification: PdfAIdentification, sha256: String)
  private final case class Reply(bytes: Array[Byte], mime: String = "application/json", status: Int = 200)
  private val pipelineSlots = new java.util.concurrent.Semaphore(1)
  private val documents = mutable.LinkedHashMap.empty[String, Document]
  private val pipelineInputs = mutable.LinkedHashMap.empty[String, Document]
  private val bookends = mutable.Map.empty[String, Document]
  private val previews = mutable.LinkedHashMap.empty[String, Array[Byte]]

  private[demo] def evaluate[A](program: A < (Abort[PdfError] & Sync)): Either[String, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case Result.Success(value) => Right(value)
      case Result.Failure(error) => Left(error.message)
      case Result.Panic(error) => Left(Option(error.getMessage).getOrElse("Operation failed"))

  private def edge[A](value: => A): Either[String, A] =
    evaluate(Abort.catching[Exception](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse("Request failed")))(Sync.defer(value)))

  private[demo] def quote(value: String): String = "\"" + value.flatMap {
    case '"' => "\\\""
    case '\\' => "\\\\"
    case '\n' => "\\n"
    case '\r' => "\\r"
    case '\t' => "\\t"
    case c if c < ' ' => f"\\u${c.toInt}%04x"
    case c => c.toString
  } + "\""

  private def json(value: String, status: Int = 200): Reply = Reply(value.getBytes(UTF_8), status = status)
  private def error(message: String, status: Int = 400): Reply = json(s"{\"error\":${quote(message)}}", status)
  private def ref(value: ObjectRef): String = s"${value.number}:${value.generation}"
  private def references(value: PdfValue): Vector[ObjectRef] = value match
    case PdfValue.Ref(value) => Vector(value)
    case PdfValue.Array(values) => values.flatMap(references)
    case PdfValue.Dict(fields) => fields.flatMap(field => references(field.value))
    case _ => Vector.empty

  private def syntax(value: PdfValue, depth: Int = 0): String =
    if depth > 3 then "..."
    else value match
      case PdfValue.Ref(value) => s"${value.number} ${value.generation} R"
      case PdfValue.Name(value) => "/" + value
      case PdfValue.Number(value) => value.toString
      case PdfValue.Bool(value) => value.toString
      case PdfValue.Null => "null"
      case PdfValue.Literal(bytes) => "(" + new String(bytes.take(180).toArray, ISO_8859_1) + (if bytes.length > 180 then "..." else "") + ")"
      case PdfValue.Hex(bytes) => "<" + bytes.take(80).map(b => f"${b & 255}%02X").mkString + ">"
      case PdfValue.Array(values) => values.take(24).map(syntax(_, depth + 1)).mkString("[ ", " ", if values.length > 24 then " ... ]" else " ]")
      case PdfValue.Dict(fields) => fields.take(20).map(f => s"/${f.name} ${syntax(f.value, depth + 1)}")
        .mkString("<<\n  ", "\n  ", if fields.length > 20 then "\n  ...\n>>" else "\n>>")

  private def report(doc: Document): String =
    val objects = doc.value.byRef.values.toVector.sortBy(obj => (obj.index.number, obj.index.generation))
    // Page dictionaries remain inspectable even when high object numbers exceed the graph cap.
    val required = doc.pages.toSet ++ doc.value.root.toSet ++ doc.identification.metadata.toSet
    val visible = (objects.filter(obj => required(obj.index)) ++ objects.filterNot(obj => required(obj.index)))
      .take(180).sortBy(obj => (obj.index.number, obj.index.generation))
    val nodes = visible.map { obj =>
      val kind = obj.dictionary.flatMap(_.get("Type")).collect { case PdfValue.Name(value) => value }
        .getOrElse(if obj.stream.nonEmpty then "Stream" else obj.value match
          case PdfValue.Dict(_) => "Dictionary"
          case PdfValue.Array(_) => "Array"
          case PdfValue.Ref(_) => "Reference"
          case PdfValue.Null => "Null"
          case PdfValue.Bool(_) => "Boolean"
          case PdfValue.Number(_) => "Number"
          case _ => "String")
      val refs = references(obj.value).distinct.map(value => quote(ref(value))).mkString("[", ",", "]")
      s"{\"id\":${quote(ref(obj.index))},\"number\":${obj.index.number},\"generation\":${obj.index.generation},\"kind\":${quote(kind)},\"refs\":$refs,\"streamBytes\":${obj.stream.fold(0)(_.length)},\"syntax\":${quote(syntax(obj.value))}}"
    }.mkString("[", ",", "]")
    val pages = doc.pages.zipWithIndex.map((value, index) => s"{\"number\":${index + 1},\"ref\":${quote(ref(value))}}")
      .mkString("[", ",", "]")
    s"{\"id\":${quote(doc.id)},\"name\":${quote(doc.name)},\"bytes\":${doc.bytes.length},\"version\":${quote(doc.value.version)},\"root\":${quote(doc.value.root.map(ref).getOrElse(""))},\"objectCount\":${objects.length},\"streamCount\":${objects.count(_.stream.nonEmpty)},\"objects\":$nodes,\"pages\":$pages,\"trailer\":${quote(syntax(doc.value.trailer))},\"sha256\":${quote(doc.sha256)},\"identification\":${PdfValidation.declarationJson(doc.identification)}}"

  private def admit(bytes: Array[Byte], name: String, retain: Boolean = true): Either[String, Document] =
    if bytes.length > MaxInput then Left("This studio accepts PDFs up to 12 MiB. Choose a smaller file.")
    else evaluate {
      ByteLimit.fromBytes(MaxInput).map { limit =>
        PdfDocument.decode(bytes, limit, RetentionLimits(maxObjects = 8000, maxNodes = 150000, maxDepth = 32, maxPayloadBytes = 32L * 1024 * 1024))
          .map(value => PdfPages.pageRefs(value).map(pages => PdfAIdentification.inspect(value).map(found => (value, pages, found))))
      }
    }.flatMap { (value, pages, found) =>
      if pages.isEmpty || pages.length > MaxPages then Left("Choose a PDF with 1 to 64 pages.")
      else
        val doc = Document(UUID.randomUUID().toString, name.take(120), bytes, value, pages, found, PdfValidation.sha256(bytes))
        if retain then documents.synchronized {
          documents(doc.id) = doc
          while documents.size > 3 do
            val _ = documents.remove(documents.head._1)
        }
        Right(doc)
    }

  private def read(exchange: HttpExchange, limit: Int): Array[Byte] =
    Using.resource(exchange.getRequestBody)(_.readNBytes(limit + 1))

  private def preview(doc: Document, pageNumber: Int): Either[String, Array[Byte]] =
    val key = s"${doc.id}:$pageNumber"
    previews.synchronized(previews.get(key)) match
      case Some(value) => Right(value)
      case None =>
        edge {
          Using.resource(Loader.loadPDF(doc.bytes)) { pdf =>
            val box = pdf.getPage(pageNumber - 1).getCropBox
            val largest = math.max(box.getWidth, box.getHeight)
            require(largest > 0 && largest.isFinite, "This page has invalid dimensions")
            val renderer = new PDFRenderer(pdf)
            renderer.setSubsamplingAllowed(true)
            val image = renderer.renderImage(pageNumber - 1, 850f / largest, ImageType.RGB)
            val output = new ByteArrayOutputStream()
            val _ = ImageIO.write(image, "png", output)
            image.flush()
            val bytes = output.toByteArray
            previews.synchronized {
              previews(key) = bytes
              while previews.size > 12 do
                val _ = previews.remove(previews.head._1)
            }
            bytes
          }
        }

  private def route(exchange: HttpExchange): Reply =
    val path = exchange.getRequestURI.getPath
    val method = exchange.getRequestMethod
    val origin = s"http://127.0.0.1:${exchange.getLocalAddress.getPort}"
    if Option(exchange.getRequestHeaders.getFirst("Host")).exists(_ != origin.stripPrefix("http://")) ||
        Option(exchange.getRequestHeaders.getFirst("Origin")).exists(_ != origin) then
      error("Open the studio from its loopback URL.", 403)
    else if method == "GET" && Set("/", "/app.js", "/validation.js", "/pipelines.js", "/style.css", "/chivo.ttf")(path) then
      val asset = if path == "/" then "index.html" else path.drop(1)
      val mime = if asset.endsWith(".js") then "text/javascript" else if asset.endsWith(".css") then "text/css" else if asset.endsWith(".ttf") then "font/ttf" else "text/html"
      Using.resource(getClass.getResourceAsStream(s"/paper-machine/$asset")) { stream =>
        Reply(stream.readNBytes(256 * 1024), mime)
      }
    else if path == "/api/policies/schema" && method == "GET" then json(Policy.jsonSchema.print)
    else if path == "/api/policies/example" && method == "GET" then
      json(Policy.encode(CourtProfile.CaliforniaSecondDistrictPetition.rules))
    else if path == "/api/policies/check" && method == "POST" then
      Policy.decode(String(read(exchange, Policy.MaxJsonChars), UTF_8)).fold(
        issues => json(s"""{"issues":${PipelineJson.issues(issues)}}""", 400),
        plan => json(Policy.encode(plan.profile)))
    else if path == "/mcp" then
      if method != "POST" then Reply(Array.emptyByteArray, status = 405)
      else
        val (status, body) = PipelineTool.handle(read(exchange, PipelineJson.MaxBytes), origin) { (tool, plan, files) =>
          val response = tool match
            case "check_pdf_pipeline" =>
              json(s"""{"recipe":${PdfPipelineModel.encode(plan)},"order":${plan.order.map(n => quote(n.id)).mkString("[", ",", "]")}}""")
            case "explain_pdf_pipeline" => explainPipeline(plan, files)
            case _ => runPipeline(plan, files)
          (response.status, String(response.bytes, UTF_8))
        }
        json(body, status)
    else if path.startsWith("/api/pipeline-inputs/") && method == "POST" then
      val slot = path.stripPrefix("/api/pipeline-inputs/")
      if !slot.matches("[A-Za-z][A-Za-z0-9_-]{0,47}") then error("Choose a valid input slot")
      else
        val existing = Option(exchange.getRequestURI.getRawQuery).filter(_.startsWith("document=")).map(_.stripPrefix("document="))
        val source = existing match
          case Some(id) => documents.synchronized(documents.get(id))
            .orElse(bookends.synchronized(bookends.values.find(_.id == id)))
            .orElse(pipelineInputs.synchronized(pipelineInputs.values.find(_.id == id)))
            .toRight("The current PDF expired. Open it again.")
          case None =>
            val name = Option(exchange.getRequestHeaders.getFirst("X-File-Name")).map(value => URLDecoder.decode(value, UTF_8)).getOrElse("Recipe input.pdf")
            admit(read(exchange, MaxInput), name, retain = false)
        source.fold(error(_), doc =>
          pipelineInputs.synchronized {
            pipelineInputs(slot) = doc
            while pipelineInputs.size > 4 do
              val _ = pipelineInputs.remove(pipelineInputs.head._1)
          }
          json(report(doc)))
    else if Set("/api/pipelines/example", "/api/pipelines/model-example")(path) && method == "GET" then
      val example = Option(exchange.getRequestURI.getRawQuery).getOrElse("") match
        case "pages" => PipelineExamples.pages
        case "validation" => PipelineExamples.validation
        case "packet" | "bookended" => PipelineExamples.packet
        case "archive-packet" => PipelineExamples.archivePacket
        case "court-preflight" => PipelineExamples.courtPreflight
        case _ => PipelineWiring.example
      if path.endsWith("/model-example") then
        PdfPipeline.check(example).fold(issues => error(PipelineJson.issues(issues)), plan => json(PdfPipelineModel.encode(plan)))
      else json(PipelineJson.encode(example))
    else if Set("/api/pipelines/check", "/api/pipelines/run")(path) && method == "POST" then
      val parsed = PipelineJson.parse(read(exchange, PipelineJson.MaxBytes))
      val envelope = parsed.flatMap { value =>
        if path.endsWith("/check") then Right((value, Map.empty[String, String]))
        else if !value.isJsonObject then Left(Vector(PdfPipeline.Issue("request", PdfPipeline.Code.Structure, "Send a recipe and input bindings")))
        else
          val obj = value.getAsJsonObject
          if obj.keySet().asScala.toSet != Set("recipe", "bindings") || !obj.get("bindings").isJsonObject then
            Left(Vector(PdfPipeline.Issue("bindings", PdfPipeline.Code.Binding, "Send a recipe and a bindings object")))
          else
            val entries = obj.getAsJsonObject("bindings").entrySet().asScala.toVector
            if entries.length > 4 || !entries.forall(entry => entry.getValue.isJsonPrimitive && entry.getValue.getAsJsonPrimitive.isString) then
              Left(Vector(PdfPipeline.Issue("bindings", PdfPipeline.Code.Binding, "Bind at most four named inputs to document IDs")))
            else Right((obj.get("recipe"), entries.map(entry => entry.getKey -> entry.getValue.getAsString).toMap))
      }
      val checked = envelope.flatMap { (value, bindings) => PipelineJson.decode(value).flatMap(PdfPipeline.check).map(plan => (plan, bindings)) }
      checked.fold(issues => json(s"{\"error\":\"Check the recipe fields and connections\",\"issues\":${PipelineJson.issues(issues)}}", 400),
        (plan, bindings) =>
          if path.endsWith("/check") then
            json(s"{\"recipe\":${PipelineJson.encode(plan.recipe)},\"order\":${plan.order.map(n => quote(n.id)).mkString("[", ",", "]")}}")
          else runPipeline(plan, bindings))
    else if path == "/api/validate" && method == "POST" then
      val profiles = Option(exchange.getRequestURI.getRawQuery).getOrElse("").stripPrefix("profiles=").split(',').toVector
      if profiles.isEmpty || profiles.distinct != profiles || !profiles.forall(PdfValidation.Profiles.contains) then
        error("Choose distinct profiles from 2b, 3b, and 3u")
      else if !PdfValidation.slots.tryAcquire() then error("Another validation is running. Wait for it to finish.", 429)
      else Using.resource(new AutoCloseable { def close(): Unit = PdfValidation.slots.release() }) { _ =>
        ValidationPdf.from(read(exchange, ValidationPdf.MaxBytes)).fold(error(_), input => json(PdfValidation.run(input, profiles)))
      }
    else if method == "GET" && path.startsWith("/api/validation-samples/") then
      val kind = path.stripPrefix("/api/validation-samples/")
      if !Set("valid", "false-claim", "ordinary")(kind) then error("Not found", 404)
      else evaluate(PdfWriter.write(if kind == "ordinary" then Sample.document else ValidationSamples.document(kind == "false-claim")))
        .fold(error(_), bytes => Reply(bytes.toArray, "application/pdf"))
    else if path == "/api/sample" && method == "POST" then
      evaluate(PdfWriter.write(Sample.document)).flatMap(bytes => admit(bytes.toArray, "Field notes.pdf"))
        .fold(error(_), doc => json(report(doc)))
    else if path == "/api/documents" && method == "POST" then
      val bytes = read(exchange, MaxInput)
      val name = Option(exchange.getRequestHeaders.getFirst("X-File-Name")).map(value => URLDecoder.decode(value, UTF_8)).getOrElse("Untitled.pdf")
      admit(bytes, name).fold(error(_), doc => json(report(doc)))
    else if method == "POST" && Set("/api/bookends/head", "/api/bookends/tail")(path) then
      val bytes = read(exchange, MaxInput)
      val name = Option(exchange.getRequestHeaders.getFirst("X-File-Name")).map(value => URLDecoder.decode(value, UTF_8)).getOrElse("Added pages.pdf")
      admit(bytes, name, retain = false).fold(error(_), doc =>
        bookends.synchronized { bookends(path.split('/').last) = doc }
        json(report(doc)))
    else
      val parts = path.split('/').filter(_.nonEmpty).toVector
      if parts.length != 4 || parts.take(2) != Vector("api", "documents") then error("Not found", 404)
      else documents.synchronized(documents.get(parts(2))) match
        case None => error("This document has expired. Open the PDF again.", 404)
        case Some(doc) => parts(3) match
          case "policy-preflight" if method == "POST" =>
            val body = read(exchange, Policy.MaxJsonChars)
            val parsed = if body.isEmpty then Policy.compile(CourtProfile.CaliforniaSecondDistrictPetition.rules)
              else Policy.decode(String(body, UTF_8))
            parsed.fold(issues => json(s"""{"issues":${PipelineJson.issues(issues)}}""", 400), plan =>
              if !pipelineSlots.tryAcquire() then error("Another pipeline is running. Wait for it to finish.", 429)
              else
                try json(PolicyPreflight.inspect(doc.bytes, plan))
                finally pipelineSlots.release())

          case "file" if method == "GET" => Reply(doc.bytes, "application/pdf")
          case "preview" if method == "GET" =>
            val page = Option(exchange.getRequestURI.getQuery).getOrElse("").stripPrefix("page=").toIntOption
            page.filter(n => n >= 1 && n <= doc.pages.length) match
              case None => error("Choose a valid page number")
              case Some(n) => preview(doc, n).fold(error(_), bytes => Reply(bytes, "image/png"))
          case "remix" if method == "POST" =>
            val body = read(exchange, 1024)
            val numbers = new String(body, UTF_8).split(",", -1).toVector.map(_.trim.toIntOption)
            if body.length > 1024 || numbers.exists(_.isEmpty) then error("Send a comma-separated list of page numbers")
            else evaluate(PdfPages.select(doc.value, numbers.flatten).map(PdfWriter.write))
              .flatMap(bytes => admit(bytes.toArray, doc.name.stripSuffix(".pdf") + " - remix.pdf"))
              .fold(error(_), result => json(report(result)))
          case "compose" if method == "POST" =>
            val body = read(exchange, 4096)
            if body.length > 4096 then error("Packet options exceed their limit")
            else
              val entries = new String(body, UTF_8).split('&').toVector.map { field =>
                val pair = field.split("=", 2)
                URLDecoder.decode(pair(0), UTF_8) -> URLDecoder.decode(pair.lift(1).getOrElse(""), UTF_8)
              }
              val options = entries.toMap
              val allowed = Set("pages", "head", "tail", "stamp", "placement", "numbered", "start", "style", "prefix")
              val numbers = options.getOrElse("pages", "").split(",", -1).toVector.map(_.toIntOption)
              val start = options.getOrElse("start", "1").toIntOption
              if entries.length != options.size || !options.keySet.subsetOf(allowed) || numbers.exists(_.isEmpty) || start.isEmpty ||
                  !Set("true", "false")(options.getOrElse("numbered", "false")) then error("Choose valid packet options")
              else
                val chosen = bookends.synchronized {
                  Vector("head", "tail").map(slot => options.get(slot).filter(_.nonEmpty).map(id =>
                    bookends.get(slot).filter(_.id == id).toRight(s"The $slot PDF expired. Choose it again.")))
                }
                val sources = chosen.foldLeft[Either[String, Vector[Option[Document]]]](Right(Vector.empty)) { (acc, value) =>
                  acc.flatMap(found => value.fold[Either[String, Vector[Option[Document]]]](Right(found :+ None))(_.map(doc => found :+ Some(doc))))
                }
                sources.flatMap { sources =>
                  val settings = PacketOptions(options.getOrElse("stamp", ""), options.getOrElse("placement", "top"),
                    options.getOrElse("numbered", "false") == "true", start.get, options.getOrElse("style", "page"), options.getOrElse("prefix", ""))
                  evaluate(PacketBuild.run(doc.value, numbers.flatten, sources(0).map(_.value), sources(1).map(_.value), settings))
                    .flatMap(bytes => admit(bytes.toArray, doc.name.stripSuffix(".pdf") + " - packet.pdf"))
                }.fold(error(_), result => json(report(result)))
          case _ => error("Not found", 404)

  private def documentSnapshot: Map[String, Document] =
    documents.synchronized(documents.toMap) ++
      bookends.synchronized(bookends.values.map(doc => doc.id -> doc).toMap) ++
      pipelineInputs.synchronized(pipelineInputs.values.map(doc => doc.id -> doc).toMap)

  private def decisionsJson(decisions: Vector[PdfPipeline.Decision]): String =
    decisions.map(d => s"""{"node":${quote(d.node)},"outcome":${quote(d.outcome.toString.toLowerCase(java.util.Locale.ROOT))},"selected":${quote(d.selected)},"reason":${quote(d.reason)}}""").mkString("[", ",", "]")

  private def explainPipeline(plan: PdfPipeline.Checked, bindings: Map[String, String]): Reply =
    val snapshot = documentSnapshot
    val bound = bindings.flatMap((slot, id) => snapshot.get(id).map(slot -> _))
    val services = new PipelineServices(Vector.empty, bound.valuesIterator.map(doc => doc.value -> doc.bytes).toVector)
    if !pipelineSlots.tryAcquire() then error("Another pipeline is running. Wait for it to finish.", 429)
    else Using.resource(new AutoCloseable { def close(): Unit = pipelineSlots.release() }) { _ =>
      evaluate(PdfPipeline.explain(plan, bound.view.mapValues(_.value).toMap, services)).fold(error(_), explanation =>
        val outputs = explanation.outputs.map((node, kind) => s"""{"node":${quote(node)},"kind":${quote(kind.toString.toLowerCase(java.util.Locale.ROOT))}}""").mkString("[", ",", "]")
        val deferred = explanation.deferred.map(d => s"""{"node":${quote(d.node)},"reason":${quote(d.reason)}}""").mkString("[", ",", "]")
        json(s"""{"recipeVersion":${plan.recipe.version},"decisions":${decisionsJson(explanation.decisions)},"deferred":$deferred,"outputs":$outputs}"""))
    }

  private def runPipeline(plan: PdfPipeline.Checked, bindings: Map[String, String]): Reply =
    import PdfPipeline.*
    val slots = plan.recipe.nodes.collect { case Node(_, Op.Input(slot), _) => slot }.toSet
    val snapshot = documentSnapshot
    val bound = bindings.flatMap((slot, id) => snapshot.get(id).map(slot -> _))
    if !bindings.keySet.subsetOf(slots) then
      error("Bind only input slots named by the recipe")
    else if !pipelineSlots.tryAcquire() then error("Another pipeline is running. Wait for it to finish.", 429)
    else Using.resource(new AutoCloseable { def close(): Unit = pipelineSlots.release() }) { _ =>
      val profiles = plan.order.flatMap { node => node.op match
        case Op.Validate(profiles) => profiles
        case _ => Vector.empty
      }.distinct
      val services = new PipelineServices(if plan.order.exists(_.op.isInstanceOf[Op.Choose]) then Vector.empty else profiles,
        bound.valuesIterator.map(doc => doc.value -> doc.bytes).toVector)
      val inputHashes = bindings.toVector.sortBy(_._1).map { (slot, id) =>
        val digest = bound.get(slot).map(doc => quote(doc.sha256)).getOrElse("null")
        s"""{"slot":${quote(slot)},"documentId":${quote(id)},"sha256":$digest}"""
      }.mkString("[", ",", "]")
      var observedNodes = Vector.empty[String]
      var observedDecisions = Vector.empty[Decision]
      val program = PdfPipeline.runObserved(plan, bound.view.mapValues(_.value).toMap, services) { (nodes, decisions) =>
        observedNodes = nodes
        observedDecisions = decisions
      }
      evaluate(program)
        .flatMap { run =>
          val outputs = run.outputs.foldLeft[Either[String, Vector[String]]](Right(Vector.empty)) { case (acc, (id, value)) => acc.flatMap { found => value match
            case Value.File(bytes) =>
              val name = plan.recipe.name.replaceAll("[^A-Za-z0-9 _-]", "").trim.take(70)
              admit(bytes.toArray, s"${if name.nonEmpty then name else "Pipeline"} - $id.pdf")
                .map(doc => found :+ s"{\"node\":${quote(id)},\"kind\":\"file\",\"document\":${report(doc)}}")
            case Value.Receipt(receipt, _) => Right(found :+ s"{\"node\":${quote(id)},\"kind\":\"receipt\",\"report\":$receipt}")
            case _ => Left("A pipeline output must be a PDF file or validation receipt")
          }}
          val decisions = decisionsJson(run.decisions)
          val validations = services.validationEvidence.mkString("[", ",", "]")
          val validationRuns = services.validationRuns.map((hash, profiles) => s"{\"sha256\":${quote(hash)},\"profiles\":${profiles.map(quote).mkString("[", ",", "]")}}").mkString("[", ",", "]")
          outputs.map(values => s"{\"recipeVersion\":${plan.recipe.version},\"inputs\":$inputHashes,\"validationEvidence\":$validations,\"validationRuns\":$validationRuns,\"decisions\":$decisions,\"skipped\":${run.skipped.map(quote).mkString("[", ",", "]")},\"validatorPasses\":${services.validationPasses},\"recipe\":${PipelineJson.encode(plan.recipe)},\"executed\":${run.executed.map(quote).mkString("[", ",", "]")},\"outputs\":${values.mkString("[", ",", "]")}}")
        }.fold(message =>
          val executed = observedNodes.map(quote).mkString("[", ",", "]")
          val validations = services.validationEvidence.mkString("[", ",", "]")
          json(s"""{"error":${quote(message)},"recipeVersion":${plan.recipe.version},"inputs":$inputHashes,"executed":$executed,"decisions":${decisionsJson(observedDecisions)},"validationEvidence":$validations,"outputs":[]}""",
            if message == PipelineServices.BusyMessage then 429 else 400), json(_))
    }

  def main(args: Array[String]): Unit =
    val port = args.headOption.flatMap(_.toIntOption).getOrElse(0)
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 16)
    server.setExecutor(Executors.newFixedThreadPool(3))
    val _ = server.createContext("/", exchange =>
      val response = edge(route(exchange)).fold(message => error(message), identity)
      val _ = edge {
        Using.resource(exchange) { current =>
          current.getResponseHeaders.set("Content-Type", response.mime)
          current.getResponseHeaders.set("Cache-Control", "no-store")
          current.getResponseHeaders.set("X-Content-Type-Options", "nosniff")
          current.getResponseHeaders.set("Referrer-Policy", "no-referrer")
          current.getResponseHeaders.set("Content-Security-Policy", "default-src 'self'; img-src 'self' blob:; style-src 'self'; script-src 'self'; connect-src 'self'; frame-ancestors 'none'")
          current.sendResponseHeaders(response.status, response.bytes.length.toLong)
          current.getResponseBody.write(response.bytes)
        }
      }
    )
    server.start()
    println(s"PAPER_MACHINE_URL=http://127.0.0.1:${server.getAddress.getPort}")
