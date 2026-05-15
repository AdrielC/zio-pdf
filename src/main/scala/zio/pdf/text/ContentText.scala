package zio.pdf.text

import _root_.scodec.bits.BitVector
import zio.blocks.streams.Pipeline

import java.nio.charset.StandardCharsets

object ContentText {

  val pipeline: Pipeline[TextToken, TextChunk] =
    Pipeline.collect[TextToken, TextChunk] {
      case TextToken.Str(value)    => TextChunk(value)
      case TextToken.HexStr(value) => TextChunk(value)
    }

  def extract(bits: BitVector): Either[Throwable, String] =
    Right(render(tokenize(bytesToString(bits.toByteArray))))

  private[text] def tokenize(input: String): Vector[TextToken] = {
    val out = Vector.newBuilder[TextToken]
    var i   = 0
    while (i < input.length) {
      skipWsAndComments(input, i) match {
        case next if next >= input.length =>
          i = next
        case next =>
          readToken(input, next) match {
            case Some((token, after)) =>
              out += token
              i = after
            case None =>
              i = next + 1
          }
      }
    }
    out.result()
  }

  private def render(tokens: Vector[TextToken]): String = {
    val out         = new StringBuilder
    val stack       = scala.collection.mutable.ArrayBuffer.empty[TextToken]
    var inText      = false
    var wroteOnLine = false

    def cleanLineEnd(): Unit =
      while (out.nonEmpty && (out.last == ' ' || out.last == '\t')) out.deleteCharAt(out.length - 1)

    def newline(): Unit =
      if (wroteOnLine) {
        cleanLineEnd()
        if (out.nonEmpty && out.last != '\n') out.append('\n')
        wroteOnLine = false
      }

    def appendText(value: String): Unit =
      if (inText && value.nonEmpty) {
        out.append(value)
        wroteOnLine = true
      }

    def arrayText(token: TextToken): String =
      token match {
        case TextToken.ArrayValue(values) =>
          values.map {
            case TextToken.Str(value)    => value
            case TextToken.HexStr(value) => value
            case _                       => ""
          }.mkString
        case _ => ""
      }

    tokens.foreach {
      case TextToken.Word("BT") =>
        if (out.nonEmpty && wroteOnLine) newline()
        inText = true
        stack.clear()
      case TextToken.Word("ET") =>
        newline()
        inText = false
        stack.clear()
      case TextToken.Word("Tj") =>
        stack.lastOption.foreach {
          case TextToken.Str(value)    => appendText(value)
          case TextToken.HexStr(value) => appendText(value)
          case _                       => ()
        }
        stack.clear()
      case TextToken.Word("TJ") =>
        stack.lastOption.foreach(token => appendText(arrayText(token)))
        stack.clear()
      case TextToken.Word("'") =>
        newline()
        stack.lastOption.foreach {
          case TextToken.Str(value)    => appendText(value)
          case TextToken.HexStr(value) => appendText(value)
          case _                       => ()
        }
        stack.clear()
      case TextToken.Word("\"") =>
        newline()
        stack.lastOption.foreach {
          case TextToken.Str(value)    => appendText(value)
          case TextToken.HexStr(value) => appendText(value)
          case _                       => ()
        }
        stack.clear()
      case TextToken.Word("T*") =>
        newline()
        stack.clear()
      case TextToken.Word("Td") | TextToken.Word("TD") | TextToken.Word("Tm") =>
        newline()
        stack.clear()
      case TextToken.Word(_) =>
        stack.clear()
      case other =>
        stack += other
    }
    cleanLineEnd()
    out.result().stripSuffix("\n")
  }

  private def bytesToString(bytes: Array[Byte]): String =
    String(bytes, StandardCharsets.ISO_8859_1)

  private def skipWsAndComments(input: String, start: Int): Int = {
    var i = start
    var done = false
    while (!done && i < input.length) {
      input.charAt(i) match {
        case '%' =>
          i += 1
          while (i < input.length && input.charAt(i) != '\n' && input.charAt(i) != '\r') i += 1
        case c if c.isWhitespace =>
          i += 1
        case _ =>
          done = true
      }
    }
    i
  }

  private def readToken(input: String, start: Int): Option[(TextToken, Int)] =
    input.charAt(start) match {
      case '(' => readLiteral(input, start + 1).map { case (s, i) => (TextToken.Str(s), i) }
      case '[' => readArray(input, start + 1).map { case (a, i) => (TextToken.ArrayValue(a), i) }
      case '<' if start + 1 < input.length && input.charAt(start + 1) != '<' =>
        readHex(input, start + 1).map { case (s, i) => (TextToken.HexStr(s), i) }
      case '\'' =>
        Some((TextToken.Word("'"), start + 1))
      case '"' =>
        Some((TextToken.Word("\""), start + 1))
      case _ =>
        val end = readWordEnd(input, start)
        if (end > start) Some((TextToken.Word(input.substring(start, end)), end)) else None
    }

  private def readWordEnd(input: String, start: Int): Int = {
    var i = start
    while (
      i < input.length &&
        !input.charAt(i).isWhitespace &&
        !"[]()<>{}/%".contains(input.charAt(i))
    ) i += 1
    i
  }

  private def readArray(input: String, start: Int): Option[(Vector[TextToken], Int)] = {
    val values = Vector.newBuilder[TextToken]
    var i      = start
    var done   = false
    var ok     = true
    while (ok && !done && i < input.length) {
      i = skipWsAndComments(input, i)
      if (i >= input.length) ok = false
      else if (input.charAt(i) == ']') {
        done = true
        i += 1
      } else
        readToken(input, i) match {
          case Some((token, after)) =>
            values += token
            i = after
          case None =>
            ok = false
        }
    }
    Option.when(ok && done)((values.result(), i))
  }

  private def readLiteral(input: String, start: Int): Option[(String, Int)] = {
    val out    = new StringBuilder
    var i      = start
    var depth  = 0
    var done   = false
    while (!done && i < input.length) {
      input.charAt(i) match {
        case '\\' if i + 1 < input.length =>
          val (s, next) = readEscape(input, i + 1)
          out.append(s)
          i = next
        case '(' =>
          depth += 1
          out.append('(')
          i += 1
        case ')' if depth > 0 =>
          depth -= 1
          out.append(')')
          i += 1
        case ')' =>
          done = true
          i += 1
        case c =>
          out.append(c)
          i += 1
      }
    }
    Option.when(done)((out.result(), i))
  }

  private def readEscape(input: String, start: Int): (String, Int) =
    input.charAt(start) match {
      case 'n'  => ("\n", start + 1)
      case 'r'  => ("\r", start + 1)
      case 't'  => ("\t", start + 1)
      case 'b'  => ("\b", start + 1)
      case 'f'  => ("\f", start + 1)
      case '('  => ("(", start + 1)
      case ')'  => (")", start + 1)
      case '\\' => ("\\", start + 1)
      case '\n' => ("", start + 1)
      case '\r' if start + 1 < input.length && input.charAt(start + 1) == '\n' =>
        ("", start + 2)
      case '\r' =>
        ("", start + 1)
      case c if c >= '0' && c <= '7' =>
        readOctal(input, start)
      case c =>
        (c.toString, start + 1)
    }

  private def readOctal(input: String, start: Int): (String, Int) = {
    var i     = start
    var count = 0
    var value = 0
    while (i < input.length && count < 3 && input.charAt(i) >= '0' && input.charAt(i) <= '7') {
      value = value * 8 + (input.charAt(i) - '0')
      i += 1
      count += 1
    }
    (value.toChar.toString, i)
  }

  private def readHex(input: String, start: Int): Option[(String, Int)] = {
    val nibbles = Vector.newBuilder[Int]
    var i       = start
    var done    = false
    var ok      = true
    while (ok && !done && i < input.length) {
      input.charAt(i) match {
        case '>' =>
          done = true
          i += 1
        case c if c.isWhitespace =>
          i += 1
        case c =>
          hexValue(c) match {
            case Some(v) =>
              nibbles += v
              i += 1
            case None =>
              ok = false
          }
      }
    }
    if (!ok || !done) None
    else {
      val ns  = nibbles.result()
      val arr = new Array[Byte]((ns.length + 1) / 2)
      var j   = 0
      while (j < ns.length) {
        val hi = ns(j)
        val lo = if (j + 1 < ns.length) ns(j + 1) else 0
        arr(j / 2) = ((hi << 4) | lo).toByte
        j += 2
      }
      Some((bytesToString(arr), i))
    }
  }

  private def hexValue(c: Char): Option[Int] =
    if (c >= '0' && c <= '9') Some(c - '0')
    else if (c >= 'a' && c <= 'f') Some(c - 'a' + 10)
    else if (c >= 'A' && c <= 'F') Some(c - 'A' + 10)
    else None
}
