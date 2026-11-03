package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import kyo.*
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
  private final case class Document(id: String, name: String, bytes: Array[Byte], value: PdfDocument, pages: Vector[ObjectRef])
  private final case class Reply(bytes: Array[Byte], mime: String = "application/json", status: Int = 200)
  private val documents = mutable.LinkedHashMap.empty[String, Document]
  private val previews = mutable.LinkedHashMap.empty[String, Array[Byte]]

  private def evaluate[A](program: A < (Abort[PdfError] & Sync)): Either[String, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case Result.Success(value) => Right(value)
      case Result.Failure(error) => Left(error.message)
      case Result.Panic(error) => Left(Option(error.getMessage).getOrElse("Operation failed"))

  private def edge[A](value: => A): Either[String, A] =
    evaluate(Abort.catching[Exception](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse("Request failed")))(Sync.defer(value)))

  private def quote(value: String): String = "\"" + value.flatMap {
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
    val required = doc.pages.toSet ++ doc.value.root.toSet
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
    s"{\"id\":${quote(doc.id)},\"name\":${quote(doc.name)},\"bytes\":${doc.bytes.length},\"version\":${quote(doc.value.version)},\"root\":${quote(doc.value.root.map(ref).getOrElse(""))},\"objectCount\":${objects.length},\"streamCount\":${objects.count(_.stream.nonEmpty)},\"objects\":$nodes,\"pages\":$pages,\"trailer\":${quote(syntax(doc.value.trailer))}}"

  private def admit(bytes: Array[Byte], name: String): Either[String, Document] =
    if bytes.length > MaxInput then Left("This studio accepts PDFs up to 12 MiB. Choose a smaller file.")
    else evaluate {
      ByteLimit.fromBytes(MaxInput).map { limit =>
        PdfDocument.decode(bytes, limit, RetentionLimits(maxObjects = 8000, maxNodes = 150000, maxDepth = 32, maxPayloadBytes = 32L * 1024 * 1024))
          .map(value => PdfPages.pageRefs(value).map(pages => (value, pages)))
      }
    }.flatMap { (value, pages) =>
      if pages.isEmpty || pages.length > MaxPages then Left("Choose a PDF with 1 to 64 pages.")
      else
        val doc = Document(UUID.randomUUID().toString, name.take(120), bytes, value, pages)
        documents.synchronized {
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
    else if method == "GET" && Set("/", "/app.js", "/style.css", "/chivo.ttf")(path) then
      val asset = if path == "/" then "index.html" else path.drop(1)
      val mime = if asset.endsWith(".js") then "text/javascript" else if asset.endsWith(".css") then "text/css" else if asset.endsWith(".ttf") then "font/ttf" else "text/html"
      Using.resource(getClass.getResourceAsStream(s"/paper-machine/$asset")) { stream =>
        Reply(stream.readNBytes(256 * 1024), mime)
      }
    else if path == "/api/sample" && method == "POST" then
      evaluate(PdfWriter.write(Sample.document)).flatMap(bytes => admit(bytes.toArray, "Field notes.pdf"))
        .fold(error(_), doc => json(report(doc)))
    else if path == "/api/documents" && method == "POST" then
      val bytes = read(exchange, MaxInput)
      val name = Option(exchange.getRequestHeaders.getFirst("X-File-Name")).map(value => URLDecoder.decode(value, UTF_8)).getOrElse("Untitled.pdf")
      admit(bytes, name).fold(error(_), doc => json(report(doc)))
    else
      val parts = path.split('/').filter(_.nonEmpty).toVector
      if parts.length != 4 || parts.take(2) != Vector("api", "documents") then error("Not found", 404)
      else documents.synchronized(documents.get(parts(2))) match
        case None => error("This document has expired. Open the PDF again.", 404)
        case Some(doc) => parts(3) match
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
          case _ => error("Not found", 404)

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
