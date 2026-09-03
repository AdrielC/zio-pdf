package zio.pdf.examples

import java.nio.file.Path

import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Attach `/Thumb` previews to an existing PDF (placeholder or PDFBox-rendered). */
object ThumbnailsFromPdf extends ZIOAppDefault:

  def run: ZIO[Any, Throwable, Unit] =
    for {
      input  <- envPath("INPUT_PDF")
      output <- envPath("OUTPUT_PDF")
      width  <- envInt("THUMB_WIDTH", 64)
      height <- envInt("THUMB_HEIGHT", 64)
      source <- PdfIO.reader(input).runCollect
      options = thumbnailOptions(source, width, height)
      updated <- PdfEngine.withThumbnailsBytes(source, options)
      _       <- ZStream.fromChunk(updated).run(PdfIO.writer(output))
      _ <- Console.printLine(
             s"wrote ${updated.size} bytes (${updated.size - source.size} delta) " +
               s"with ${if options.pixelSource.isDefined then "PDFBox-rendered" else "placeholder"} /Thumb -> $output"
           )
    } yield ()

  private def thumbnailOptions(source: Chunk[Byte], width: Int, height: Int): PdfThumbnail.Options = {
    val rendered = sys.env.get("RENDER_THUMBS").exists(_.equalsIgnoreCase("true"))
    if rendered then
      PdfThumbnail.renderedOptions(
        PdfBoxRenderer.pixelSource(source.toArray),
        width = width,
        height = height
      )
    else
      PdfThumbnail.placeholderOptions(width = width, height = height)
  }

  private def envPath(name: String): ZIO[Any, Throwable, Path] =
    ZIO.fromOption(sys.env.get(name).map(Path.of(_))).orElseFail(IllegalArgumentException(s"$name is required"))

  private def envInt(name: String, default: Int): ZIO[Any, Throwable, Int] =
    ZIO.attempt(sys.env.get(name).fold(default)(_.toInt))
