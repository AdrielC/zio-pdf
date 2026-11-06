package io.github.adrielc.kyopdf.pipeline

import zio.blocks.schema.SchemaError
import zio.blocks.schema.toon.{ToonCodec, ToonReader, ToonWriter}
import PdfPipeline.{Checked, Code, Issue}

/** TOON is an alternative authoring transport for the same typed model graph.
  * Limits are checked before the upstream parser can allocate declared arrays.
  */
object PdfPipelineToon:
  private val MaxChars = 32 * 1024
  private val ArrayLength = """\[\s*(\d+)\s*[,|\t]?\s*\]""".r
  private val Decimal = """[-+]?(?:\d+\.\d*|\d*\.\d+|\d+[eE][-+]?\d+)(?:[eE][-+]?\d+)?""".r
  private[pipeline] def complete[A](codec: ToonCodec[A]): ToonCodec[A] = new ToonCodec[A]:
    def encodeValue(value: A, out: ToonWriter): Unit = codec.encodeValue(value, out)
    def decodeValue(in: ToonReader): A =
      val value = codec.decodeValue(in)
      in.skipBlankLines()
      if in.hasMoreLines then throw SchemaError("Trailing TOON content")
      value

  private def bounds(text: String): Vector[Issue] =
    if text.length > MaxChars then Vector(Issue("toon", Code.Limit, "Use at most 32 KiB of TOON characters"))
    else
      val issues = Vector.newBuilder[Issue]
      var slots = 0L
      val lines = text.linesIterator.toVector
      if lines.size > 2048 then issues += Issue("toon", Code.Limit, "Use at most 2048 TOON lines")
      lines.zipWithIndex.foreach { (line, index) =>
        val path = s"toon.lines[$index]"
        val indentation = line.takeWhile(c => c == ' ' || c == '\t')
        if indentation.contains('\t') || indentation.length > 128 then
          issues += Issue(path, Code.Limit, "Use spaces and at most 64 indentation levels")
        val outside = new StringBuilder
        var quoted = false
        var escaped = false
        line.foreach { char =>
          if escaped then escaped = false
          else if quoted && char == '\\' then escaped = true
          else if char == '"' then quoted = !quoted
          else if !quoted then outside.append(char)
        }
        ArrayLength.findAllMatchIn(outside.result()).foreach { m =>
          val digits = m.group(1)
          if digits.length > 4 || digits.toInt > 2048 then issues += Issue(path, Code.Limit, "TOON arrays have at most 2048 elements")
          else slots += digits.toInt
        }
        val plain = outside.result()
        def boundary(index: Int): Boolean = index < 0 || index >= plain.length ||
          !(plain.charAt(index).isLetterOrDigit || plain.charAt(index) == '_' || plain.charAt(index) == '.')
        Decimal.findAllMatchIn(plain).filter(m => boundary(m.start - 1) && boundary(m.end)).foreach { matched =>
          val literal = matched.matched
          val exact = scala.util.Try {
            literal.length <= 256 && java.lang.Double.isFinite(literal.toDouble) &&
              BigDecimal(literal) == BigDecimal(literal.toDouble.toString)
          }.getOrElse(false)
          if !exact then issues += Issue(path, Code.Setting, "Use JSON for numbers requiring more decimal precision")
        }
      }
      if slots > 8192 then issues += Issue("toon", Code.Limit, "TOON declares too many array elements")
      issues.result()

  def decode(text: String, registry: StepRegistry.Registry = StepRegistry.standard): Either[Vector[Issue], Checked] =
    toJson(text).flatMap(PdfPipelineModel.decode(_, registry))

  def toJson(text: String): Either[Vector[Issue], String] =
    val issues = bounds(text)
    if issues.nonEmpty then Left(issues)
    else PdfPipelineModel.toonToJson(text)

  def encode(plan: Checked): Either[Vector[Issue], String] =
    val model = PdfPipelineModel.encode(plan)
    val encoded = PdfPipelineModel.encodeToon(plan)
    for
      expected <- PdfPipelineModel.decode(model, plan.registry)
      decoded <- decode(encoded, plan.registry)
      result <- if decoded.recipe == expected.recipe then Right(encoded)
        else Left(Vector(Issue("toon", Code.Setting, "TOON did not preserve the graph exactly; use JSON")))
    yield result
