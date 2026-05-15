package zio.pdf.page

import _root_.scodec.Attempt
import zio.*
import zio.blocks.mediatype.MediaType
import zio.pdf.*
import zio.pdf.text.ContentText
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

final case class FirstPageContent(
  page: Page,
  contentObjectNumbers: Chunk[Long],
  text: String,
  mediaType: MediaType
)

final case class FirstPagePdfSlice(
  bytes: Chunk[Byte],
  mediaType: MediaType
)

object FirstPage {

  val content: ZPipeline[Any, Throwable, Byte, FirstPageContent] =
    PdfStream.elements(Log.noop) >>> contentFromElements

  def fromBytes(bytes: Chunk[Byte]): ZIO[Any, Throwable, FirstPageContent] =
    zio.stream.ZStream
      .fromChunk(bytes)
      .via(content)
      .runHead
      .someOrFail(new NoSuchElementException("PDF has no first-page content result"))

  val contentFromElements: ZPipeline[Any, Throwable, Element, FirstPageContent] =
    StatefulPipe[Element, PageSelection.Acc, FirstPageContent](PageSelection.Acc.empty, finalize)(step)

  private val step: StatefulPipe.Step[Element, PageSelection.Acc, FirstPageContent] =
    element => ZPure.update[PageSelection.Acc, PageSelection.Acc](acc => PageSelection.add(acc, element))

  private val finalize: PageSelection.Acc => ZPure[
    FirstPageContent,
    PageSelection.Acc,
    PageSelection.Acc,
    Any,
    Throwable,
    Unit
  ] = acc =>
    build(acc) match {
      case Right(value) => ZPure.log[PageSelection.Acc, FirstPageContent](value)
      case Left(err)    => ZPure.fail(err)
    }

  private def build(acc: PageSelection.Acc): Either[Throwable, FirstPageContent] =
    PageSelection.firstPage(acc) match {
      case None =>
        Left(new NoSuchElementException("PDF has no page objects"))
      case Some(page) =>
        val refs = PageSelection.contentRefs(page)
        collectText(acc, refs).map { text =>
          FirstPageContent(page, Chunk.fromIterable(refs), text, PdfMedia.textPlain)
        }
    }

  private def collectText(acc: PageSelection.Acc, refs: List[Long]): Either[Throwable, String] = {
    val parts = refs.map { ref =>
      acc.contents.get(ref) match {
        case None =>
          Right("")
        case Some(content) =>
          content.stream.exec match {
            case Attempt.Successful(bits) => ContentText.extract(bits)
            case Attempt.Failure(cause)   =>
              Left(new RuntimeException(s"decode first-page content stream $ref: ${cause.messageWithContext}"))
          }
      }
    }
    sequence(parts).map(_.filter(_.nonEmpty).mkString("\n"))
  }

  private def sequence[A](values: List[Either[Throwable, A]]): Either[Throwable, List[A]] =
    values.foldRight[Either[Throwable, List[A]]](Right(Nil)) { (next, acc) =>
      for {
        n <- next
        a <- acc
      } yield n :: a
    }
}
