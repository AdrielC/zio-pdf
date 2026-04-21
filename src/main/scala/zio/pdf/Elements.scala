/*
 * Port of fs2.pdf.Elements to Scala 3 + ZIO.
 *
 * Pure step over Decoded values, classifying each into a
 * Page / Pages / FontResource / Image / Info / Array / etc.
 * variant. The pipeline plumbing is the standard StatefulPipe
 * pattern - no per-element ZChannel state machine.
 */

package zio.pdf

import _root_.scodec.Attempt
import zio.prelude.fx.ZPure
import zio.scodec.stream.StatefulPipe
import zio.stream.ZPipeline

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
    case Prim.tpe("EmbeddedFile", data) =>
      Attempt.successful(Element.ContentKind.EmbeddedFileStream(data))
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

  /** Pure step: emit one Element per Decoded, or fail if analysis
    * fails. No state. */
  private val step: StatefulPipe.Step[Decoded, Unit, Element] =
    d => element(d) match {
      case Attempt.Successful(e) => ZPure.log[Unit, Element](e)
      case Attempt.Failure(c)    =>
        ZPure.fail(new RuntimeException(s"failed to analyze object: ${c.messageWithContext}"))
    }

  /** Pipeline `Decoded -> Element`. */
  val pipe: ZPipeline[Any, Throwable, Decoded, Element] =
    StatefulPipe[Decoded, Unit, Element]((), _ => ZPure.unit[Unit])(step)
}
