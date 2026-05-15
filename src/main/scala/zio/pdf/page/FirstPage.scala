package zio.pdf.page

import _root_.scodec.Attempt
import zio.*
import zio.blocks.mediatype.MediaType
import zio.pdf.*
import zio.pdf.text.ContentText
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.{ZPipeline, ZStream}

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
    ZStream
      .fromChunk(bytes)
      .via(content)
      .runHead
      .someOrFail(new NoSuchElementException("PDF has no first-page content result"))

  val slice: ZPipeline[Any, Throwable, Byte, FirstPagePdfSlice] =
    ZPipeline.fromFunction[Any, Throwable, Byte, FirstPagePdfSlice] { bytes =>
      ZStream.fromZIO(sliceFromBytesZIO(bytes.runCollect))
    }

  def sliceFromBytes(bytes: Chunk[Byte]): ZIO[Any, Throwable, FirstPagePdfSlice] =
    sliceFromBytesZIO(ZIO.succeed(bytes))

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

  private def sliceFromBytesZIO(bytes: ZIO[Any, Throwable, Chunk[Byte]]): ZIO[Any, Throwable, FirstPagePdfSlice] =
    for {
      input    <- bytes
      elements <- ZStream.fromChunk(input).via(PdfStream.elements(Log.noop)).runCollect
      acc       = elements.foldLeft(PageSelection.Acc.empty)(PageSelection.add)
      slice    <- buildSlice(acc)
    } yield slice

  private def buildSlice(acc: PageSelection.Acc): ZIO[Any, Throwable, FirstPagePdfSlice] =
    PageSelection.firstPage(acc) match {
      case None =>
        ZIO.fail(new NoSuchElementException("PDF has no page objects"))
      case Some(page) =>
        val refs     = PageSelection.contentRefs(page)
        val contents = refs.flatMap(acc.contents.get)
        val contentObjs = contents.zipWithIndex.map { case (content, i) =>
          val index = Obj.Index(4L + i.toLong, 0)
          IndirectObj(Obj(index, content.obj.data), Some(content.rawStream))
        }
        val contentRefs =
          contentObjs match {
            case one :: Nil => Prim.Ref(one.obj.index.number, one.obj.index.generation)
            case many       => Prim.Array(many.map(o => Prim.Ref(o.obj.index.number, o.obj.index.generation))*)
          }
        val pageData0 = Prim.Dict.updated("Parent", Prim.Ref(2, 0))(page.data)
        val pageData  = Prim.Dict.updated("Contents", contentRefs)(pageData0)
        val catalog = IndirectObj.nostream(
          1,
          Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
        )
        val pages = IndirectObj.nostream(
          2,
          Prim.dict(
            "Type"  -> Prim.Name("Pages"),
            "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
            "Count" -> Prim.Number(BigDecimal(1))
          )
        )
        val pageObj = IndirectObj.nostream(3, pageData)
        val trailer = Trailer(
          BigDecimal(4L + contentObjs.length.toLong),
          Prim.dict("Root" -> Prim.Ref(1, 0)),
          Some(Prim.Ref(1, 0))
        )
        ZStream
          .fromIterable(List(catalog, pages, pageObj) ++ contentObjs)
          .via(WritePdf.objects(trailer))
          .runFold(_root_.scodec.bits.ByteVector.empty)(_ ++ _)
          .map(bytes => FirstPagePdfSlice(Chunk.fromArray(bytes.toArray), PdfMedia.pdf))
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
