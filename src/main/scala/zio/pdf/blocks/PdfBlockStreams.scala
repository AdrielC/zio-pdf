package zio.pdf.blocks

import java.io.InputStream
import java.nio.file.Path

import zio.*
import zio.blocks.streams.{Pipeline, Sink, Stream}
import zio.pdf.io.PdfIO
import zio.pdf.page.{FirstPage, FirstPageContent}

object PdfBlockStreams {

  def fromPath(path: Path): Stream[java.io.IOException, Int] =
    Stream.fromResource[InputStream, java.io.IOException, Int](PdfIO.inputStreamResource(path)) { stream =>
      Stream.fromInputStreamUnmanaged(stream)
    }

  val normalizeTextLines: Pipeline[String, String] =
    Pipeline
      .map[String, String](_.trim.replaceAll("\\s+", " "))
      .andThen(Pipeline.filter[String](_.nonEmpty))

  val captionCandidateLines: Pipeline[String, String] =
    normalizeTextLines
      .andThen(Pipeline.take[String](80))
      .andThen(Pipeline.filter[String](isCaptionCandidate))

  val firstPageTextSink: Sink[Nothing, Int, Either[Throwable, FirstPageContent]] =
    Sink.create[Nothing, Int, Either[Throwable, FirstPageContent]] { reader =>
      val out = new java.io.ByteArrayOutputStream
      var v   = reader.read(-1)
      while (v != -1) {
        out.write(v & 0xff)
        v = reader.read(-1)
      }
      runFirstPage(Chunk.fromArray(out.toByteArray))
    }

  def firstPageText(path: Path): ZIO[Any, Throwable, FirstPageContent] =
    BlocksRuntime
      .runZIO(fromPath(path), firstPageTextSink)
      .flatMap(BlocksRuntime.eitherToZIO)

  private def runFirstPage(bytes: Chunk[Byte]): Either[Throwable, FirstPageContent] =
    Unsafe.unsafe { implicit unsafe =>
      Runtime.default.unsafe.run(FirstPage.fromBytes(bytes)) match {
        case Exit.Success(value) => Right(value)
        case Exit.Failure(cause) =>
          Left(cause.failureOption.getOrElse(new RuntimeException(cause.prettyPrint)))
      }
    }

  private def isCaptionCandidate(line: String): Boolean = {
    val lower = line.toLowerCase
    lower.contains("plaintiff") ||
    lower.contains("defendant") ||
    lower == "v." ||
    lower == "vs." ||
    lower.contains(" v. ") ||
    lower.contains(" vs. ")
  }
}
