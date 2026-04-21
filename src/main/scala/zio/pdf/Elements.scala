/*
 * Port of fs2.pdf.Elements to Scala 3 + ZIO.
 *
 * Semantic analysis of decoded PDF objects: classify each into a
 * Page / Pages / FontResource / Image / Info / Array / etc. variant.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.{Cause, Chunk}
import zio.stream.{ZChannel, ZPipeline}

private[pdf] object AnalyzeData {

  val infoKeys: List[String] = List("Author", "Producer", "Creator", "CreationData", "ModDate")

  def apply(index: Obj.Index): Prim => Attempt[Element.DataKind] = {
    case Prim.tpe("Page", data) =>
      Page.fromData(index)(data).map(Element.DataKind.Page(_))
    case Prim.tpe("Pages", data) =>
      Pages.fromData(index)(data).map(Element.DataKind.Pages(_))
    case Prim.fontResources(data) =>
      Attempt.successful(Element.DataKind.FontResource(FontResource(index, data)))
    case data @ Prim.Dict(dict) if infoKeys.exists(dict.contains) =>
      Attempt.successful(Element.DataKind.Info(data))
    case data @ Prim.Array(_) =>
      Attempt.successful(Element.DataKind.Array(data))
    case _ =>
      Attempt.successful(Element.DataKind.General)
  }
}

private[pdf] object AnalyzeContent {

  object SupportedCodec {
    def unapply(data: Prim.Dict): Option[Image.Codec] = data("Filter") match {
      case Some(Prim.Name("DCTDecode"))      => Some(Image.Codec.Jpg)
      case Some(Prim.Name("CCITTFaxDecode")) => Some(Image.Codec.Ccitt)
      case _                                  => None
    }
  }

  def apply(stream: Uncompressed): Prim => Attempt[Element.ContentKind] = {
    case Prim.subtype("Image", data @ SupportedCodec(codec)) =>
      Attempt.successful(Element.ContentKind.Image(Image(data, stream, codec)))
    case _ =>
      Attempt.successful(Element.ContentKind.General)
  }
}

object Elements {

  private[pdf] def element: Decoded => Attempt[Element] = {
    case Decoded.DataObj(obj) =>
      AnalyzeData(obj.index)(obj.data).map(Element.Data(obj, _))
    case Decoded.ContentObj(obj, rawStream, stream) =>
      AnalyzeContent(stream)(obj.data).map(Element.Content(obj, rawStream, stream, _))
    case Decoded.Meta(_, trailer, version) =>
      Attempt.successful(Element.Meta(trailer, version))
  }

  /** Pipeline `Decoded -> Element`. Failures during analysis are
    * raised as `RuntimeException` on the channel error channel. */
  val pipe: ZPipeline[Any, Throwable, Decoded, Element] = {
    def loop: ZChannel[Any, Throwable, Chunk[Decoded], Any, Throwable, Chunk[Element], Unit] =
      ZChannel.readWithCause[Any, Throwable, Chunk[Decoded], Any, Throwable, Chunk[Element], Unit](
        (chunk: Chunk[Decoded]) => {
          val out = Chunk.newBuilder[Element]
          var err: Option[String] = None
          chunk.foreach { d =>
            if (err.isEmpty) element(d) match {
              case Attempt.Successful(e) => out += e
              case Attempt.Failure(c)    => err = Some(s"failed to analyze object: ${c.messageWithContext}")
            }
          }
          err match {
            case Some(msg) => ZChannel.fail(new RuntimeException(msg))
            case None      => ZChannel.write(out.result()) *> loop
          }
        },
        (cause: Cause[Throwable]) => ZChannel.refailCause(cause),
        (_: Any)                  => ZChannel.unit
      )
    ZPipeline.fromChannel(loop)
  }
}
