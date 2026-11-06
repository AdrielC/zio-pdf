package io.github.adrielc.kyopdf

import kyo.*

enum ContentToken derives Schema:
  case Number(value: String)
  case Name(value: String)
  case Literal(bytes: Vector[Byte])
  case Hex(bytes: Vector[Byte])
  case Array(items: Vector[ContentToken])
  case Operator(value: String)

/** Bounded PDF content-stream grammar over shared byte input.
  *
  * Unsupported escapes, dictionaries, inline images, and excessive nesting
  * fail through `Abort[PdfError]`; the parser never guesses past them.
  */
object PdfContentParser:
  /** `maxTokens` includes arrays and all nested operands. `maxDepth` counts
    * open arrays and literal-string parentheses; zero permits only scalars.
    */
  final case class Limits(maxBytes: Int = 512 * 1024, maxTokens: Int = 50000, maxDepth: Int = 16) derives Schema

  def parse(bytes: Array[Byte], limits: Limits = Limits()): Vector[ContentToken] < Abort[PdfError] =
    if bytes.length > limits.maxBytes then
      Abort.fail(PdfError.TooLarge(limits.maxBytes.toLong, bytes.length.toLong))
    else if limits.maxTokens <= 0 || limits.maxDepth < 0 then
      Abort.fail(PdfError.InvalidPdf("Invalid PDF content parser limits"))
    else Abort.get(scan(PdfBytes.borrow(bytes), limits).left.map((at, message) => PdfError.InvalidPdf(s"$at: $message")))

  private def scan(input: PdfBytes, limits: Limits): Either[(Int, String), Vector[ContentToken]] =
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
      if count >= limits.maxTokens then Left((start, "PDF content token count exceeds the configured limit"))
      else if depth > limits.maxDepth then Left((start, "PDF content nesting exceeds the configured limit"))
      else
        // Reserve the node before parsing its payload or any array children.
        count += 1
        val at = skip(start)
        if at >= input.length then Left((at, "Unexpected end of PDF content"))
        else input.charAt(at) match
          case '(' | '[' if depth >= limits.maxDepth =>
            Left((at, "PDF content nesting exceeds the configured limit"))
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
            else Right((cursor, ContentToken.Literal(input.payload(at + 1, cursor - 1))))
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
            if end - at > 64 then Left((at, "PDF content token exceeds 64 bytes"))
            else
              val value = input.substring(at, end)
              if number(value) then Right((end, ContentToken.Number(value)))
              else if operator(value) then Right((end, ContentToken.Operator(value)))
              else Left((at, s"Unsupported PDF content token: $value"))

    var cursor = skip(0)
    while cursor < input.length do
      token(cursor, 0) match
        case Left(error) => return Left(error)
        case Right((next, value)) => output += value; cursor = skip(next)
    Right(output.result())

  private def whitespace(char: Char): Boolean = char.isWhitespace || char == 0
  private def delimiter(char: Char): Boolean = whitespace(char) || "/<>[](){}%".contains(char)
  private def tokenEnd(input: PdfBytes, start: Int): Int =
    var at = start
    while at < input.length && !delimiter(input.charAt(at)) do at += 1
    at
  // These tiny ASCII grammars avoid compiling two regular expressions per token.
  private def number(value: String): Boolean =
    var at = if value.nonEmpty && (value.charAt(0) == '+' || value.charAt(0) == '-') then 1 else 0
    var digits = 0
    while at < value.length && value.charAt(at) >= '0' && value.charAt(at) <= '9' do
      digits += 1
      at += 1
    if at < value.length && value.charAt(at) == '.' then
      at += 1
      while at < value.length && value.charAt(at) >= '0' && value.charAt(at) <= '9' do
        digits += 1
        at += 1
    digits > 0 && at == value.length
  private def operator(value: String): Boolean =
    var at = 0
    while at < value.length do
      val char = value.charAt(at)
      if !((char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || char == '*' || char == '\'' || char == '"') then return false
      at += 1
    value.nonEmpty && value != "BI" && value != "ID" && value != "EI"
  private def nibble(char: Char): Int =
    if char >= '0' && char <= '9' then char - '0'
    else if char >= 'a' && char <= 'f' then char - 'a' + 10
    else if char >= 'A' && char <= 'F' then char - 'A' + 10
    else -1
