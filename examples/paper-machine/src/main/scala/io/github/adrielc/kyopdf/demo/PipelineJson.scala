package io.github.adrielc.kyopdf.demo

import com.google.gson.*
import com.google.gson.stream.{JsonReader, JsonToken}
import java.io.StringReader
import java.nio.charset.StandardCharsets.UTF_8
import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Strict, bounded JSON at the transport boundary. No class names or code loading. */
object PipelineJson:
  val MaxBytes = 32 * 1024
  private def issue(path: String, message: String): Issue = Issue(path, Code.Structure, message)
  def parse(bytes: Array[Byte]): Either[Vector[Issue], JsonElement] =
    if bytes.length > MaxBytes then Left(Vector(issue("recipe", "Recipe JSON exceeds 32 KiB")))
    else Try {
      val reader = new JsonReader(new StringReader(UTF_8.newDecoder().decode(java.nio.ByteBuffer.wrap(bytes)).toString))
      reader.setStrictness(Strictness.STRICT)
      var values = 0
      def read(depth: Int): JsonElement =
        values += 1
        require(depth <= 16 && values <= 4096, "Recipe JSON exceeds its nesting or value limit")
        reader.peek() match
          case JsonToken.BEGIN_OBJECT =>
            reader.beginObject()
            val result = new JsonObject()
            while reader.hasNext() do
              val key = reader.nextName()
              require(!result.has(key), s"Duplicate JSON field: $key")
              result.add(key, read(depth + 1))
            reader.endObject()
            result
          case JsonToken.BEGIN_ARRAY =>
            reader.beginArray()
            val result = new JsonArray()
            while reader.hasNext() do result.add(read(depth + 1))
            reader.endArray()
            result
          case JsonToken.STRING => new JsonPrimitive(reader.nextString())
          case JsonToken.NUMBER => new JsonPrimitive(new java.math.BigDecimal(reader.nextString()))
          case JsonToken.BOOLEAN => new JsonPrimitive(reader.nextBoolean())
          case JsonToken.NULL => reader.nextNull(); JsonNull.INSTANCE
          case _ => throw IllegalArgumentException("Expected JSON data")
      try
        val value = read(0)
        require(reader.peek() == JsonToken.END_DOCUMENT, "Unexpected data after JSON")
        value
      finally reader.close()
    }.toEither.left.map(_ => Vector(issue("recipe", "Use valid, bounded JSON with unique field names")))

  def decode(value: JsonElement): Either[Vector[Issue], Recipe] =
    val errors = Vector.newBuilder[Issue]
    def obj(value: JsonElement, path: String): JsonObject =
      if value != null && value.isJsonObject then value.getAsJsonObject
      else
        errors += issue(path, "Expected an object")
        new JsonObject()
    def string(value: JsonObject, key: String, path: String, default: Option[String] = None): String =
      Option(value.get(key)) match
        case Some(v) if v.isJsonPrimitive && v.getAsJsonPrimitive.isString => v.getAsString
        case None if default.nonEmpty => default.get
        case _ => errors += issue(s"$path.$key", "Expected text"); ""
    def integer(value: JsonObject, key: String, path: String, default: Option[Int] = None): Int =
      Option(value.get(key)) match
        case None if default.nonEmpty => default.get
        case Some(v) if v.isJsonPrimitive && v.getAsJsonPrimitive.isNumber =>
          Try(v.getAsBigDecimal.intValueExact()).toOption.getOrElse { errors += issue(s"$path.$key", "Expected a 32-bit integer"); 0 }
        case _ => errors += issue(s"$path.$key", "Expected an integer"); 0
    def array(value: JsonObject, key: String, path: String): Vector[JsonElement] =
      Option(value.get(key)) match
        case Some(v) if v.isJsonArray => v.getAsJsonArray.asScala.toVector
        case _ => errors += issue(s"$path.$key", "Expected an array"); Vector.empty
    def strings(value: JsonObject, key: String, path: String): Vector[String] =
      array(value, key, path).zipWithIndex.map { (v, i) =>
        if v.isJsonPrimitive && v.getAsJsonPrimitive.isString then v.getAsString
        else { errors += issue(s"$path.$key[$i]", "Expected text"); "" }
      }
    def keys(value: JsonObject, allowed: Set[String], path: String): Unit =
      value.keySet().asScala.toVector.sorted.filterNot(allowed).foreach(key => errors += issue(s"$path.$key", "Unknown field"))
    def portKind(node: JsonObject, path: String): Kind = string(node, "kind", path, Some("document")) match
      case "document" => Kind.Document
      case "file" => Kind.File
      case "receipt" => Kind.Receipt
      case _ => errors += issue(s"$path.kind", "Choose document, file or receipt"); Kind.Document
    def portKinds(node: JsonObject, path: String): Vector[Kind] =
      strings(node, "kinds", path).zipWithIndex.map { (value, index) => value match
        case "document" => Kind.Document
        case "file" => Kind.File
        case "receipt" => Kind.Receipt
        case _ => errors += issue(s"$path.kinds[$index]", "Choose document, file or receipt"); Kind.Document
      }
    def predicate(value: JsonElement, path: String): Predicate =
      val test = obj(value, path)
      val kind = string(test, "kind", path)
      val (result, fields): (Predicate, Set[String]) = kind match
        case "page-count" => (Predicate.PageCount(integer(test, "min", path), integer(test, "max", path)), Set("min", "max"))
        case "has-page-numbers" => (Predicate.HasPageNumbers, Set.empty)
        case "declares-3b" => (Predicate.Declares3b, Set.empty)
        case "validation" => (Predicate.Validation(string(test, "profile", path)), Set("profile"))
        case _ => errors += issue(s"$path.kind", "Choose page-count, declares-3b or validation"); (Predicate.Declares3b, Set.empty)
      keys(test, fields + "kind", path)
      result
    val root = obj(value, "recipe")
    keys(root, Set("version", "name", "nodes", "outputs"), "recipe")
    val version = integer(root, "version", "recipe")
    val name = string(root, "name", "recipe")
    val nodes = array(root, "nodes", "recipe").zipWithIndex.map { (value, index) =>
      val path = s"nodes[$index]"
      val node = obj(value, path)
      val id = string(node, "id", path)
      val kind = string(node, "op", path)
      val inputs = strings(node, "inputs", path)
      val (op, fields): (Op, Set[String]) = kind match
        case "input" => (Op.Input(string(node, "slot", path)), Set("slot"))
        case "select" =>
          val pages = array(node, "pages", path).zipWithIndex.map { (v, i) =>
            val wrapper = new JsonObject(); wrapper.add("page", v)
            integer(wrapper, "page", s"$path.pages[$i]")
          }
          (Op.Select(pages), Set("pages"))
        case "assemble" => (Op.Assemble(integer(node, "catalog", path, Some(0))), Set("catalog"))
        case "stamp" => (Op.Stamp(string(node, "text", path), string(node, "placement", path, Some("top"))), Set("text", "placement"))
        case "number" => (Op.NumberPages(integer(node, "start", path, Some(1)), string(node, "style", path, Some("page")), string(node, "prefix", path, Some(""))), Set("start", "style", "prefix"))
        case "convert-pdfa-3b" => (Op.ConvertPdfA3b, Set.empty)
        case "write" => (Op.Write, Set.empty)
        case "validate" => (Op.Validate(strings(node, "profiles", path)), Set("profiles"))
        case "choose" => (Op.Choose(predicate(node.get("test"), s"$path.test"), portKind(node, path)), Set("test", "kind"))
        case "stop" => (Op.Stop(string(node, "message", path), portKind(node, path)), Set("message", "kind"))
        case "keep" => (Op.Keep(integer(node, "index", path), portKinds(node, path)), Set("index", "kinds"))
        case "stop-after" => (Op.StopAfter(string(node, "message", path), portKinds(node, path), portKind(node, path)), Set("message", "kinds", "kind"))
        case _ => errors += issue(s"$path.op", "Choose a supported PDF, validation, choose or stop operation"); (Op.Write, Set.empty)
      keys(node, Set("id", "op", "inputs") ++ fields, path)
      Node(id, op, inputs)
    }
    val outputs = strings(root, "outputs", "recipe")
    val failures = errors.result()
    if failures.nonEmpty then Left(failures) else Right(Recipe(version, name, nodes, outputs))

  def encode(recipe: Recipe): String =
    def strings(values: Vector[String]): String = values.map(PaperMachine.quote).mkString("[", ",", "]")
    def predicate(value: Predicate): String = value match
      case Predicate.PageCount(min, max) => s"{\"kind\":\"page-count\",\"min\":$min,\"max\":$max}"
      case Predicate.HasPageNumbers => "{\"kind\":\"has-page-numbers\"}"
      case Predicate.Declares3b => "{\"kind\":\"declares-3b\"}"
      case Predicate.Validation(profile) => s"{\"kind\":\"validation\",\"profile\":${PaperMachine.quote(profile)}}"
    val nodes = recipe.nodes.map { node =>
      val (kind, settings) = node.op match
        case Op.Input(slot) => ("input", s",\"slot\":${PaperMachine.quote(slot)}")
        case Op.Select(pages) => ("select", s",\"pages\":${pages.mkString("[", ",", "]")}")
        case Op.Assemble(catalog) => ("assemble", s",\"catalog\":$catalog")
        case Op.Stamp(text, placement) => ("stamp", s",\"text\":${PaperMachine.quote(text)},\"placement\":${PaperMachine.quote(placement)}")
        case Op.NumberPages(start, style, prefix) => ("number", s",\"start\":$start,\"style\":${PaperMachine.quote(style)},\"prefix\":${PaperMachine.quote(prefix)}")
        case Op.ConvertPdfA3b => ("convert-pdfa-3b", "")
        case Op.Write => ("write", "")
        case Op.Validate(profiles) => ("validate", s",\"profiles\":${strings(profiles)}")
        case Op.Choose(test, kind) => ("choose", s",\"test\":${predicate(test)},\"kind\":${PaperMachine.quote(kind.toString.toLowerCase)}")
        case Op.Stop(message, kind) => ("stop", s",\"message\":${PaperMachine.quote(message)},\"kind\":${PaperMachine.quote(kind.toString.toLowerCase)}")
        case Op.Keep(index, kinds) => ("keep", s",\"index\":$index,\"kinds\":${strings(kinds.map(_.toString.toLowerCase))}")
        case Op.StopAfter(message, kinds, kind) => ("stop-after", s",\"message\":${PaperMachine.quote(message)},\"kinds\":${strings(kinds.map(_.toString.toLowerCase))},\"kind\":${PaperMachine.quote(kind.toString.toLowerCase)}")
      s"{\"id\":${PaperMachine.quote(node.id)},\"op\":${PaperMachine.quote(kind)},\"inputs\":${strings(node.inputs)}$settings}"
    }.mkString("[", ",", "]")
    s"{\"version\":${recipe.version},\"name\":${PaperMachine.quote(recipe.name)},\"nodes\":$nodes,\"outputs\":${strings(recipe.outputs)}}"

  def issues(values: Vector[Issue]): String = values.map(value =>
    s"{\"path\":${PaperMachine.quote(value.path)},\"code\":${PaperMachine.quote(value.code.toString)},\"message\":${PaperMachine.quote(value.message)}}"
  ).mkString("[", ",", "]")
