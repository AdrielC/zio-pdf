/*
 * AcroForm inventory and structural flatten.
 *
 * Flatten removes catalog /AcroForm and widget annotations. It does not bake
 * field appearances into page content streams; that remains a later visual step.
 */

package zio.pdf

import zio.{Chunk, ZIO}
import zio.blocks.chunk.ChunkMap
import zio.stream.ZStream

object PdfAcroForm {

  sealed abstract class Error(message: String) extends Exception(message)

  case object NoAcroForm extends Error("document has no /AcroForm")

  final case class Field(
    name: Option[String],
    fieldType: Option[String],
    objectNumber: Long,
    value: Option[String]
  )

  final case class Inventory(
    fields: Chunk[Field],
    catalogObjectNumber: Option[Long],
    formObjectNumber: Option[Long],
    needAppearances: Boolean
  )

  def extract(decoded: Chunk[Decoded]): Inventory = {
    val objects = objectMap(decoded)
    val catalog = objects.values.find(isCatalogWithForm)
    val formPrim = catalog.flatMap(obj => dictAt(obj.data).flatMap(_.data.get("AcroForm")))
    val (formObjectNumber, formDict) = formPrim match {
      case Some(Prim.Ref(number, _)) =>
        (Some(number), objects.get(number).flatMap(obj => dictAt(obj.data)))
      case Some(dict: Prim.Dict) =>
        (None, Some(dict))
      case _ =>
        (None, None)
    }
    val needAppearances = formDict.exists(_.data.get("NeedAppearances").contains(Prim.Bool(true)))
    val fields = formDict.flatMap(_.data.get("Fields")) match {
      case Some(Prim.Array(entries)) =>
        Chunk.fromIterable(entries.flatMap {
          case Prim.Ref(number, _) =>
            objects.get(number).flatMap(obj => dictAt(obj.data).map(fieldOf(number, _)))
          case dict: Prim.Dict =>
            Some(fieldOf(0L, dict))
          case _ =>
            None
        })
      case _ =>
        Chunk.empty
    }
    Inventory(
      fields = fields,
      catalogObjectNumber = catalog.map(_.index.number),
      formObjectNumber = formObjectNumber,
      needAppearances = needAppearances
    )
  }

  /** Structural flatten: drop /AcroForm and widget annotations, then re-encode. */
  def flatten(decoded: Chunk[Decoded]): ZIO[Any, Throwable, Chunk[Byte]] =
    ZIO.fromEither(flattenParts(decoded)).flatMap { parts =>
      ZStream
        .fromChunk(parts)
        .via(WritePdf.parts)
        .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
    }

  def flattenParts(decoded: Chunk[Decoded]): Either[Error, Chunk[Part[Trailer]]] = {
    val inventory = extract(decoded)
    if inventory.catalogObjectNumber.isEmpty && inventory.formObjectNumber.isEmpty && inventory.fields.isEmpty then
      Left(NoAcroForm)
    else
      val objects = objectMap(decoded)
      val widgets = objects.collect {
        case (number, obj) if isWidget(obj.data) => number
      }.toSet ++ inventory.formObjectNumber.toSet
      val rewritten = decoded.flatMap {
        case Decoded.Meta(_, trailer, _) =>
          trailer.toList.map(Part.Meta(_))
        case Decoded.DataObj(obj) if widgets.contains(obj.index.number) =>
          Chunk.empty
        case Decoded.ContentObj(obj, _, _) if widgets.contains(obj.index.number) =>
          Chunk.empty
        case Decoded.DataObj(obj) =>
          Chunk(Part.Obj(IndirectObj(rewriteObj(obj, widgets), None)))
        case Decoded.ContentObj(obj, rawStream, _) =>
          Chunk(Part.Obj(IndirectObj(rewriteObj(obj, widgets), Some(rawStream))))
      }
      Right(rewritten)
  }

  private def rewriteObj(obj: Obj, widgets: Set[Long]): Obj =
    dictAt(obj.data) match {
      case Some(dict) if isCatalog(dict) =>
        Obj(obj.index, withoutKey(dict, "AcroForm"))
      case Some(dict) =>
        Obj(obj.index, stripWidgetAnnots(dict, widgets))
      case None =>
        obj
    }

  private def stripWidgetAnnots(dict: Prim.Dict, widgets: Set[Long]): Prim.Dict =
    dict.data.get("Annots") match {
      case Some(Prim.Array(annots)) =>
        val kept = annots.filterNot {
          case Prim.Ref(number, _) => widgets.contains(number)
          case _                   => false
        }
        if kept.isEmpty then withoutKey(dict, "Annots")
        else Prim.Dict(dict.data.updated("Annots", Prim.Array(kept)))
      case _ =>
        dict
    }

  private def fieldOf(number: Long, dict: Prim.Dict): Field =
    Field(
      name = nameAt(dict, "T"),
      fieldType = nameAt(dict, "FT"),
      objectNumber = number,
      value = stringAt(dict, "V")
    )

  private def nameAt(dict: Prim.Dict, key: String): Option[String] =
    dict.data.get(key).collect { case Prim.Name(value) => value }.orElse(stringAt(dict, key))

  private def stringAt(dict: Prim.Dict, key: String): Option[String] =
    dict.data.get(key).collect {
      case Prim.Str(value)    => new String(value.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
      case Prim.HexStr(value) => new String(value.toArray, java.nio.charset.StandardCharsets.ISO_8859_1)
      case Prim.Name(value)   => value
      case Prim.Number(value) => value.toString
    }

  private def isCatalogWithForm(obj: Obj): Boolean =
    dictAt(obj.data).exists(dict => isCatalog(dict) && dict.data.contains("AcroForm"))

  private def isCatalog(dict: Prim.Dict): Boolean =
    dict.data.get("Type").contains(Prim.Name("Catalog"))

  private def isWidget(data: Prim): Boolean =
    Prim.tryDict("Subtype")(data).contains(Prim.Name("Widget"))

  private def dictAt(data: Prim): Option[Prim.Dict] =
    data match {
      case dict: Prim.Dict => Some(dict)
      case _               => None
    }

  private def withoutKey(dict: Prim.Dict, key: String): Prim.Dict =
    Prim.Dict(
      dict.data.iterator.filterNot(_._1 == key).foldLeft(ChunkMap.empty[String, Prim]) { (acc, pair) =>
        acc.updated(pair._1, pair._2)
      }
    )

  private def objectMap(decoded: Chunk[Decoded]): Map[Long, Obj] =
    decoded.collect {
      case Decoded.DataObj(obj)          => obj.index.number -> obj
      case Decoded.ContentObj(obj, _, _) => obj.index.number -> obj
    }.toMap
}
