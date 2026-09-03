/*
 * Port of fs2.pdf.EncodePdfTest — Generate encoder golden output.
 */

package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.*
import zio.stream.*
import zio.test.*

object GenerateSpec extends ZIOSpecDefault {

  private val page1ObjNumber = 1
  private val page2ObjNumber = 2

  private def page1Obj(contentObj: IndirectObj, pagesObj: IndirectObj, resourcesObj: IndirectObj, annotObj: IndirectObj) =
    IndirectObj.nostream(
      page1ObjNumber,
      Prim.dict(
        "Type"      -> Prim.Name("Page"),
        "Contents"  -> Prim.refT(contentObj),
        "MediaBox"  -> Prim.array(Prim.num(0), Prim.num(0), Prim.num(600), Prim.num(800)),
        "Parent"    -> Prim.refT(pagesObj),
        "Resources" -> Prim.refT(resourcesObj),
        "Annots"    -> Prim.array(Prim.refT(annotObj)),
      )
    )

  private def page2Obj(contentObj: IndirectObj, pagesObj: IndirectObj, resourcesObj: IndirectObj, annotObj: IndirectObj) =
    IndirectObj.nostream(
      page2ObjNumber,
      Prim.dict(
        "Type"      -> Prim.Name("Page"),
        "Contents"  -> Prim.refT(contentObj),
        "MediaBox"  -> Prim.array(Prim.num(0), Prim.num(0), Prim.num(600), Prim.num(800)),
        "Parent"    -> Prim.refT(pagesObj),
        "Resources" -> Prim.refT(resourcesObj),
        "Annots"    -> Prim.array(Prim.refT(annotObj)),
      )
    )

  private def pagesObj =
    IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Count" -> Prim.num(2),
        "Kids"  -> Prim.Array(Prim.Ref(page1ObjNumber, 0), Prim.Ref(page2ObjNumber, 0)),
      )
    )

  private def catalogObj(pagesObj: IndirectObj, outlinesObj: IndirectObj) =
    IndirectObj.nostream(
      4,
      Prim.dict(
        "Type"     -> Prim.Name("Catalog"),
        "Pages"    -> Prim.refT(pagesObj),
        "Outlines" -> Prim.refT(outlinesObj),
      )
    )

  private val contentStream: ByteVector =
    ByteVector(
      """BT
        |  /F1 24 Tf
        |  50 750 Td
        |  (Hello World) Tj
        |ET""".stripMargin.getBytes
    )

  private def contentObj =
    IndirectObj.stream(5, Prim.dict("Length" -> Prim.num(contentStream.size)), contentStream.bits)

  private def resourcesObj(fontObj: IndirectObj) =
    IndirectObj.nostream(
      6,
      Prim.dict(
        "ProcSet" -> Prim.array(Prim.Name("PDF"), Prim.Name("Text")),
        "Font"    -> Prim.dict("F1" -> Prim.refT(fontObj)),
      )
    )

  private def fontObj =
    IndirectObj.nostream(
      7,
      Prim.dict(
        "Type"     -> Prim.Name("Font"),
        "Subtype"  -> Prim.Name("Type1"),
        "Name"     -> Prim.Name("F1"),
        "BaseFont" -> Prim.Name("Helvetica"),
        "Encoding" -> Prim.Name("MacRomanEncoding"),
      )
    )

  private def outlinesObj =
    IndirectObj.nostream(
      8,
      Prim.dict(
        "Type"  -> Prim.Name("Outlines"),
        "Count" -> Prim.num(0),
      )
    )

  private def infoObj = IndirectObj.nostream(9, Prim.dict())

  private def annotObj =
    IndirectObj.nostream(
      10,
      Prim.dict(
        "Type"     -> Prim.Name("Annot"),
        "Subtype"  -> Prim.Name("FreeText"),
        "Rect"     -> Prim.array(Prim.num(200), Prim.num(50), Prim.num(400), Prim.num(150)),
        "Contents" -> Prim.str("the annotation"),
        "Border"   -> Prim.array(Prim.num(0), Prim.num(0), Prim.num(0)),
      )
    )

  private def objects: List[IndirectObj] = {
    val content   = contentObj
    val pages     = pagesObj
    val resources = resourcesObj(fontObj)
    List(
      page1Obj(content, pages, resources, annotObj),
      page2Obj(content, pages, resources, annotObj),
      pages,
      catalogObj(pages, outlinesObj),
      content,
      resources,
      fontObj,
      outlinesObj,
      infoObj,
      annotObj,
    )
  }

  private val id: Prim =
    Prim.HexStr(ByteVector("FF1FE073E0365E226E145B0CC9CB0758".getBytes))

  private def trailer(catalogObj: IndirectObj, infoObj: IndirectObj): Prim.Dict =
    Prim.dict(
      "Size" -> Prim.num(objects.size + 1),
      "Root" -> Prim.refT(catalogObj),
      "Id"   -> Prim.array(id, id),
      "Info" -> Prim.refT(infoObj),
    )

  def spec: Spec[Any, Throwable] = suite("Generate")(
    test("encode a pdf with free-text annotation decodes to the expected object graph") {
      val catalog = catalogObj(pagesObj, outlinesObj)
      val info    = infoObj
      for {
        encoded <- ZStream
                     .fromIterable(objects)
                     .via(Generate(trailer(catalog, info)))
                     .runCollect
                     .map(bytes => _root_.scodec.bits.ByteVector.view(bytes.toArray))
        decoded <- ZStream
                     .fromChunk(Chunk.fromArray(encoded.toArray))
                     .via(PdfStream.decode())
                     .runCollect
        data    = decoded.collect { case Decoded.DataObj(o)           => o }
        content = decoded.collect { case Decoded.ContentObj(o, _, _) => o }
        text    = new String(encoded.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.startsWith("%PDF-"),
        text.contains("%%EOF"),
        text.contains("FreeText"),
        text.contains("the annotation"),
        data.size == 9,
        content.size == 1,
        data.exists(_.index.number == 10L)
      )
    }
  )
}
