package io.github.adrielc.kyopdf.demo

import com.google.gson.*
import com.google.gson.stream.{JsonReader, JsonToken}
import java.io.StringReader
import java.nio.charset.StandardCharsets.UTF_8
import io.github.adrielc.kyopdf.pipeline.PdfPipeline.*
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
    zio.blocks.schema.json.Json.parse(value.toString)
      .left.map(_ => Vector(issue("recipe", "Invalid JSON")))
      .flatMap(json => io.github.adrielc.kyopdf.pipeline.PdfPipelineModel.decodeValue(json, namedBranches = false))
      .map(_.recipe)

  def encode(recipe: Recipe): String =
    io.github.adrielc.kyopdf.pipeline.PdfPipeline.check(recipe).fold(
      issues => throw IllegalArgumentException(issues.toString),
      checked => io.github.adrielc.kyopdf.pipeline.PdfPipelineModel.encode(checked, namedBranches = false))

  def issues(values: Vector[Issue]): String = values.map(value =>
    s"{\"path\":${PaperMachine.quote(value.path)},\"code\":${PaperMachine.quote(value.code.toString)},\"message\":${PaperMachine.quote(value.message)}}"
  ).mkString("[", ",", "]")
