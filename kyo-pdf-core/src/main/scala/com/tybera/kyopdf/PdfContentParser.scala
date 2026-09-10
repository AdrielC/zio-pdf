package com.tybera.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1
import kyo.*

enum ContentToken derives Schema:
  case Number(value: String)
  case Name(value: String)
  case Literal(bytes: Vector[Byte])
  case Hex(bytes: Vector[Byte])
  case Array(items: Vector[ContentToken])
  case Operator(value: String)

/** Bounded PDF content-stream grammar on kyo-parse.
  *
  * Unsupported escapes, dictionaries, inline images, and excessive nesting
  * fail through `Abort[PdfError]`; the parser never guesses past them.
  */
object PdfContentParser:
  final case class Limits(maxBytes: Int = 512 * 1024, maxTokens: Int = 50000, maxDepth: Int = 16) derives Schema

  def parse(bytes: Array[Byte], limits: Limits = Limits()): Vector[ContentToken] < Abort[PdfError] =
    if bytes.length > limits.maxBytes then
      Abort.fail(PdfError.TooLarge(limits.maxBytes.toLong, bytes.length.toLong))
    else if limits.maxTokens <= 0 || limits.maxDepth < 0 then
      Abort.fail(PdfError.InvalidPdf("Invalid PDF content parser limits"))
    else
      val input = new String(bytes, ISO_8859_1)
      val program = Parse.read[Char, Vector[ContentToken]] { state =>
        scan(input, limits) match
          case Left((position, message)) => Result.fail(Chunk(ParseFailure(message, position)))
          case Right(tokens) => Result.succeed((state.advance(state.remaining.length), tokens))
      }
      val result = Parse.runResult(input)(program).eval
      result.out.toOption match
        case Some(tokens) => tokens
        case None => Abort.fail(PdfError.InvalidPdf(result.errors.map(_.message).mkString("; ")))

  private def scan(input: String, limits: Limits): Either[(Int, String), Vector[ContentToken]] =
    val output = Vector.newBuilder[ContentToken]
    var count = 0

    def skip(start: Int): Int =
      var at = start
      var again = true
      while at < input.length && again do
        if whitespace(input.charAt(at)) then at += 1
        else if input.charAt(at) == '%' then
          while at < input.length && input.charAt(at) != '\r' && input.charAt(at) != '\n' do at += 1
        else again = false
      at

    def token(start: Int, depth: Int): Either[(Int, String), (Int, ContentToken)] =
      if depth > limits.maxDepth then Left((start, "PDF content nesting exceeds the configured limit"))
      else
        val at = skip(start)
        if at >= input.length then Left((at, "Unexpected end of PDF content"))
        else input.charAt(at) match
          case '(' =>
            var cursor = at + 1
            var nesting = 1
            while cursor < input.length && nesting > 0 do
              input.charAt(cursor) match
                case '\\' => return Left((cursor, "PDF content string escapes require a richer text decoder"))
                case '(' => nesting += 1; if nesting + depth > limits.maxDepth then return Left((cursor, "PDF content string nesting exceeds the configured limit"))
                case ')' => nesting -= 1
                case _ => ()
              cursor += 1
            if nesting != 0 then Left((at, "Unterminated PDF content string"))
            else Right((cursor, ContentToken.Literal(input.substring(at + 1, cursor - 1).getBytes(ISO_8859_1).toVector)))
          case '<' =>
            if at + 1 < input.length && input.charAt(at + 1) == '<' then Left((at, "PDF content dictionaries are unsupported"))
            else
              var cursor = at + 1
              val hex = new StringBuilder
              while cursor < input.length && input.charAt(cursor) != '>' do
                val char = input.charAt(cursor)
                if !whitespace(char) then
                  if nibble(char) < 0 then return Left((cursor, "Invalid PDF hexadecimal string"))
                  hex.append(char)
                cursor += 1
              if cursor >= input.length then Left((at, "Unterminated PDF hexadecimal string"))
              else
                if hex.length % 2 == 1 then hex.append('0')
                val bytes = Vector.tabulate(hex.length / 2) { index =>
                  ((nibble(hex.charAt(index * 2)) << 4) | nibble(hex.charAt(index * 2 + 1))).toByte
                }
                Right((cursor + 1, ContentToken.Hex(bytes)))
          case '[' =>
            val items = Vector.newBuilder[ContentToken]
            var cursor = skip(at + 1)
            while cursor < input.length && input.charAt(cursor) != ']' do
              token(cursor, depth + 1) match
                case Left(error) => return Left(error)
                case Right((next, value)) =>
                  value match
                    case ContentToken.Operator(_) => return Left((cursor, "Operators are not valid inside PDF content arrays"))
                    case _ => items += value
                  cursor = skip(next)
            if cursor >= input.length then Left((at, "Unterminated PDF content array"))
            else Right((cursor + 1, ContentToken.Array(items.result())))
          case ']' | ')' | '>' | '{' | '}' => Left((at, "Unexpected PDF content delimiter"))
          case '/' =>
            val end = tokenEnd(input, at + 1)
            if end == at + 1 then Left((at, "Empty PDF name"))
            else Right((end, ContentToken.Name(input.substring(at + 1, end))))
          case _ =>
            val end = tokenEnd(input, at)
            val value = input.substring(at, end)
            if value.length > 64 then Left((at, "PDF content token exceeds 64 bytes"))
            else if number(value) then Right((end, ContentToken.Number(value)))
            else if value.matches("[A-Za-z*'\"]+") && !Set("BI", "ID", "EI")(value) then Right((end, ContentToken.Operator(value)))
            else Left((at, s"Unsupported PDF content token: $value"))

    var cursor = skip(0)
    while cursor < input.length do
      if count >= limits.maxTokens then return Left((cursor, "PDF content token count exceeds the configured limit"))
      token(cursor, 0) match
        case Left(error) => return Left(error)
        case Right((next, value)) => output += value; count += 1; cursor = skip(next)
    Right(output.result())

  private def whitespace(char: Char): Boolean = char.isWhitespace || char == 0
  private def delimiter(char: Char): Boolean = whitespace(char) || "/<>[](){}%".contains(char)
  private def tokenEnd(input: String, start: Int): Int =
    var at = start
    while at < input.length && !delimiter(input.charAt(at)) do at += 1
    at
  private def number(value: String): Boolean = value.matches("[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)")
  private def nibble(char: Char): Int =
    if char >= '0' && char <= '9' then char - '0'
    else if char >= 'a' && char <= 'f' then char - 'a' + 10
    else if char >= 'A' && char <= 'F' then char - 'A' + 10
    else -1
