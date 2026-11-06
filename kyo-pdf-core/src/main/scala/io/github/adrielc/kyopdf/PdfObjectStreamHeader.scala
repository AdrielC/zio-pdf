package io.github.adrielc.kyopdf

/** Read exactly /N integer pairs without splitting the entire /First region. */
private[kyopdf] object PdfObjectStreamHeader:
  def read(input: PdfBytes, count: Int, first: Int): Either[String, Vector[(Long, Int)]] =
    var cursor = 0L
    def skip(): Unit =
      while cursor < first && (input.charAt(cursor).isWhitespace || input.charAt(cursor) == 0) do cursor += 1
    def integer(): Option[Long] =
      skip()
      val start = cursor
      while cursor < first && input.charAt(cursor) >= '0' && input.charAt(cursor) <= '9' && cursor - start < 20 do cursor += 1
      if cursor == start || cursor < first && !input.charAt(cursor).isWhitespace && input.charAt(cursor) != 0 then None
      else input.substring(start, cursor).toLongOption
    val entries = Vector.newBuilder[(Long, Int)]
    var index = 0
    while index < count do
      (integer(), integer()) match
        case (Some(number), Some(offset)) if number > 0 && offset <= Int.MaxValue => entries += number -> offset.toInt
        case _ => return Left("PDF object-stream header contains an invalid integer pair")
      index += 1
    skip()
    if cursor != first then Left("PDF object-stream header has more than /N entries")
    else Right(entries.result())
