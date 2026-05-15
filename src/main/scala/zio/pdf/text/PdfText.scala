package zio.pdf.text

sealed trait TextToken

object TextToken {
  final case class Str(value: String)              extends TextToken
  final case class HexStr(value: String)           extends TextToken
  final case class ArrayValue(values: Vector[TextToken]) extends TextToken
  final case class Word(value: String)             extends TextToken
}

final case class TextChunk(value: String)
