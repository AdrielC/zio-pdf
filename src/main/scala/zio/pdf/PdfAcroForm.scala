/*
 * AcroForm inventory and flatten.
 *
 * Flatten bakes widget `/AP /N` Form XObjects (or a `/V` text fallback) into
 * page content, then removes catalog `/AcroForm` and widget annotations.
 */

package zio.pdf

import java.nio.charset.StandardCharsets

import _root_.scodec.bits.BitVector
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
    needAppearances: Boolean,
    fieldObjectNumbers: Set[Long] = Set.empty
  )

  final case class FlattenReport(
    appearancesPlaced: Int,
    textFallbacks: Int
  )

  final case class FieldValuesReport(
    applied: Int,
    qualifiedNames: Chunk[String]
  )

  case object EmptyFieldValues extends Error("field value map is empty")

  final case class NoMatchingFields(requested: Set[String]) extends Error(
    s"no AcroForm fields matched requested names: ${requested.mkString(", ")}"
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
    val roots = formDict.flatMap(_.data.get("Fields")) match {
      case Some(Prim.Array(entries)) => entries.toList
      case _                         => Nil
    }
    val walked = walkFields(roots, objects, None, None, None, Set.empty)
    Inventory(
      fields = Chunk.fromIterable(walked.fields),
      catalogObjectNumber = catalog.map(_.index.number),
      formObjectNumber = formObjectNumber,
      needAppearances = needAppearances,
      fieldObjectNumbers = walked.nodes
    )
  }

  /**
   * Set `/V` on AcroForm fields matched by qualified name. Widget `/AP` entries
   * are removed so a subsequent [[flatten]] uses the new value when no
   * appearance stream remains.
   */
  def applyFieldValues(decoded: Chunk[Decoded], values: Map[String, String]): ZIO[Any, Throwable, (Chunk[Byte], FieldValuesReport)] =
    ZIO.fromEither(applyFieldValuesParts(decoded, values)).flatMap { (parts, report) =>
      ZStream
        .fromChunk(parts)
        .via(WritePdf.parts)
        .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
        .map(_ -> report)
    }

  def applyFieldValuesParts(decoded: Chunk[Decoded], values: Map[String, String]): Either[Error, (Chunk[Part[Trailer]], FieldValuesReport)] =
    if values.isEmpty then Left(EmptyFieldValues)
    else {
      val inventory = extract(decoded)
      val updates = inventory.fields.flatMap { field =>
        field.name.flatMap(name => values.get(name).filter(_ => field.objectNumber > 0L).map(name -> (field.objectNumber, _)))
      }
      if updates.isEmpty then Left(NoMatchingFields(values.keySet))
      else {
        val byObject = updates.groupMap(_._2._1)(_._2._2).view.mapValues(_.last).toMap
        val names    = Chunk.fromIterable(updates.map(_._1).distinct)
        val parts = decoded.flatMap {
          case Decoded.Meta(_, trailer, _) =>
            trailer.toList.map(Part.Meta(_))
          case Decoded.DataObj(obj) =>
            byObject.get(obj.index.number) match {
              case Some(value) =>
                dictAt(obj.data).map(setFieldValue(_, value)) match {
                  case Some(dict) => Chunk(Part.Obj(IndirectObj(Obj(obj.index, dict), None)))
                  case None       => Chunk(Part.Obj(IndirectObj(obj, None)))
                }
              case None =>
                Chunk(Part.Obj(IndirectObj(obj, None)))
            }
          case Decoded.ContentObj(obj, rawStream, _) =>
            byObject.get(obj.index.number) match {
              case Some(value) =>
                dictAt(obj.data).map(setFieldValue(_, value)) match {
                  case Some(dict) => Chunk(Part.Obj(IndirectObj(Obj(obj.index, dict), Some(rawStream))))
                  case None       => Chunk(Part.Obj(IndirectObj(obj, Some(rawStream))))
                }
              case None =>
                Chunk(Part.Obj(IndirectObj(obj, Some(rawStream))))
            }
        }
        Right((parts, FieldValuesReport(applied = byObject.size, qualifiedNames = names)))
      }
    }

  /** Bake appearances into page content, then drop /AcroForm and widgets. */
  def flatten(decoded: Chunk[Decoded]): ZIO[Any, Throwable, Chunk[Byte]] =
    flattenReported(decoded).map(_._1)

  def flattenReported(decoded: Chunk[Decoded]): ZIO[Any, Throwable, (Chunk[Byte], FlattenReport)] =
    ZIO.fromEither(flattenParts(decoded)).flatMap { (parts, report) =>
      ZStream
        .fromChunk(parts)
        .via(WritePdf.parts)
        .runFold(Chunk.empty[Byte])((acc, chunk) => acc ++ Chunk.fromArray(chunk.toArray))
        .map(_ -> report)
    }

  def flattenParts(decoded: Chunk[Decoded]): Either[Error, (Chunk[Part[Trailer]], FlattenReport)] = {
    val inventory = extract(decoded)
    if inventory.catalogObjectNumber.isEmpty && inventory.formObjectNumber.isEmpty && inventory.fields.isEmpty then
      Left(NoAcroForm)
    else
      val objects = objectMap(decoded)
      val streams = streamMap(decoded)
      val drop = objects.collect {
        case (number, obj) if isWidget(obj.data) => number
      }.toSet ++ inventory.formObjectNumber.toSet ++ inventory.fieldObjectNumbers
      val baked     = bakeAppearances(objects, streams, drop)
      val rewritten = decoded.flatMap {
        case Decoded.Meta(_, trailer, _) =>
          val sized = trailer.map { meta =>
            if baked.nextNumber > meta.size.toLong then meta.copy(size = BigDecimal(baked.nextNumber))
            else meta
          }
          sized.toList.map(Part.Meta(_))
        case Decoded.DataObj(obj) if drop.contains(obj.index.number) && !baked.keptAppearances.contains(obj.index.number) =>
          Chunk.empty
        case Decoded.ContentObj(obj, _, _)
            if drop.contains(obj.index.number) && !baked.keptAppearances.contains(obj.index.number) =>
          Chunk.empty
        case Decoded.DataObj(obj) =>
          Chunk(Part.Obj(IndirectObj(rewriteObj(applyPageUpdate(obj, baked.pages), drop), None)))
        case Decoded.ContentObj(obj, rawStream, _) =>
          Chunk(Part.Obj(IndirectObj(rewriteObj(applyPageUpdate(obj, baked.pages), drop), Some(rawStream))))
      }
      Right(
        rewritten ++ baked.extra,
        FlattenReport(appearancesPlaced = baked.appearancesPlaced, textFallbacks = baked.textFallbacks)
      )
  }

  private final case class Bake(
    pages: Map[Long, Prim.Dict],
    extra: Chunk[Part[Trailer]],
    keptAppearances: Set[Long],
    nextNumber: Long,
    appearancesPlaced: Int,
    textFallbacks: Int
  )

  private final case class FieldWalk(fields: List[Field], nodes: Set[Long])

  private final class Fresh(start: Long) {
    private var current = start
    def next(): Long = {
      val number = current
      current += 1L
      number
    }
    def peek: Long = current
  }

  private final case class Placement(
    content: String,
    xobject: Option[(String, Prim.Ref)],
    needsFont: Boolean
  )

  private def bakeAppearances(
    objects: Map[Long, Obj],
    streams: Map[Long, BitVector],
    widgets: Set[Long]
  ): Bake = {
    val pages = objects.collect {
      case (number, obj) if dictAt(obj.data).exists(isPage) => number -> obj
    }
    val widgetsByPage = pageWidgets(pages, objects, widgets)
    val startNumber   = (objects.keys.iterator ++ streams.keys.iterator).maxOption.getOrElse(0L) + 1L
    val fresh         = Fresh(startNumber)
    val extras        = Chunk.newBuilder[Part[Trailer]]
    val pageUpdates   = scala.collection.mutable.Map.empty[Long, Prim.Dict]
    val kept          = scala.collection.mutable.Set.empty[Long]
    var appearancesPlaced = 0
    var textFallbacks     = 0

    widgetsByPage.foreach { (pageNumber, widgetNumbers) =>
      val pageObj  = pages(pageNumber)
      val pageDict = dictAt(pageObj.data).get
      var usedNames = xobjectNames(pageDict, objects)
      val placements = widgetNumbers.zipWithIndex.flatMap { (widgetNumber, index) =>
        objects.get(widgetNumber).flatMap(obj => dictAt(obj.data)).flatMap { widget =>
          val placed = placementOf(widget, objects, streams, widgets, usedNames, index, fresh)
          placed.foreach { (placement, extra) =>
            extra.foreach(extras += _)
            kept ++= placement.xobject.map(_._2.number)
            usedNames ++= placement.xobject.map(_._1)
          }
          placed.map(_._1)
        }
      }
      if placements.nonEmpty then
        appearancesPlaced += placements.count(_.xobject.nonEmpty)
        textFallbacks += placements.count(_.needsFont)
        val bytes  = placements.map(_.content).mkString("\n").getBytes(StandardCharsets.ISO_8859_1)
        val stream = fresh.next()
        extras += Part.Obj(IndirectObj.stream(stream, Prim.dict(), BitVector(bytes)))
        var updated = appendContents(pageDict, Prim.Ref(stream, 0))
        placements.foreach { placed =>
          placed.xobject.foreach { (name, ref) =>
            updated = addXObject(updated, objects, name, ref)
          }
          if placed.needsFont then updated = addHelvetica(updated, objects)
        }
        pageUpdates.update(pageNumber, updated)
    }

    Bake(pageUpdates.toMap, extras.result(), kept.toSet, fresh.peek, appearancesPlaced, textFallbacks)
  }

  private def placementOf(
    widget: Prim.Dict,
    objects: Map[Long, Obj],
    streams: Map[Long, BitVector],
    widgets: Set[Long],
    usedNames: Set[String],
    index: Int,
    fresh: Fresh
  ): Option[(Placement, Option[Part[Trailer]])] = {
    val rect = rectAt(widget)
    appearanceRef(widget, objects).flatMap { appearanceNumber =>
      materializeAppearance(appearanceNumber, widget, objects, streams, widgets, fresh).map {
        (refNumber, extraForm) =>
          val bbox = objects
            .get(appearanceNumber)
            .flatMap(obj => dictAt(obj.data).flatMap(bboxAt))
            .orElse(rect.map(rectAsBBox))
            .getOrElse((0.0, 0.0, 1.0, 1.0))
          val box    = rect.getOrElse(bbox)
          val matrix = objects.get(appearanceNumber).flatMap(obj => dictAt(obj.data)).map(matrixAt).getOrElse(identityMatrix)
          val name   = unusedName(usedNames, s"Ff${index + 1}")
          (
            Placement(
              content = appearanceContent(name, box, bbox, matrix),
              xobject = Some((name, Prim.Ref(refNumber, 0))),
              needsFont = false
            ),
            extraForm
          )
      }
    }.orElse {
      rect.zip(stringAt(widget, "V")).map { (box, value) =>
        (Placement(textContent(box, value), None, needsFont = true), None)
      }
    }
  }

  private def materializeAppearance(
    appearanceNumber: Long,
    widget: Prim.Dict,
    objects: Map[Long, Obj],
    streams: Map[Long, BitVector],
    widgets: Set[Long],
    fresh: Fresh
  ): Option[(Long, Option[Part[Trailer]])] =
    if !widgets.contains(appearanceNumber) && streams.contains(appearanceNumber) then
      Some((appearanceNumber, None))
    else
      streams.get(appearanceNumber).map { raw =>
        val number = fresh.next()
        val bbox   = objects
          .get(appearanceNumber)
          .flatMap(obj => dictAt(obj.data).flatMap(bboxAt))
          .orElse(rectAt(widget).map(rectAsBBox))
          .getOrElse((0.0, 0.0, 1.0, 1.0))
        val source = objects.get(appearanceNumber).flatMap(obj => dictAt(obj.data))
        val dict = List("Matrix", "Resources").foldLeft(
          Prim.dict(
            "Type"    -> Prim.Name("XObject"),
            "Subtype" -> Prim.Name("Form"),
            "BBox"    -> Prim.Array.nums(bbox._1, bbox._2, bbox._3, bbox._4)
          )
        ) { (acc, key) =>
          source.flatMap(_.data.get(key)) match {
            case Some(value) => Prim.Dict(acc.data.updated(key, value))
            case None        => acc
          }
        }
        (number, Some(Part.Obj(IndirectObj.stream(number, dict, raw))))
      }

  private def appearanceContent(
    name: String,
    rect: (Double, Double, Double, Double),
    bbox: (Double, Double, Double, Double),
    matrix: (Double, Double, Double, Double, Double, Double)
  ): String = {
    val (x1, y1, x2, y2) = normalize(rect)
    val (bx1, by1, bx2, by2) = transformBox(bbox, matrix)
    val bboxW = math.max(bx2 - bx1, 0.0001)
    val bboxH = math.max(by2 - by1, 0.0001)
    val sx    = (x2 - x1) / bboxW
    val sy    = (y2 - y1) / bboxH
    val tx    = x1 - bx1 * sx
    val ty    = y1 - by1 * sy
    s"q ${pdfNum(sx)} 0 0 ${pdfNum(sy)} ${pdfNum(tx)} ${pdfNum(ty)} cm /$name Do Q"
  }

  private def textContent(rect: (Double, Double, Double, Double), value: String): String = {
    val (x1, y1, _, y2) = normalize(rect)
    val size            = math.max(8.0, math.min(12.0, (y2 - y1) * 0.7))
    s"BT /Helv ${pdfNum(size)} Tf ${pdfNum(x1)} ${pdfNum(y1 + 2.0)} Td ${pdfLiteral(value)} Tj ET"
  }

  private def pageWidgets(
    pages: Map[Long, Obj],
    objects: Map[Long, Obj],
    widgets: Set[Long]
  ): Map[Long, List[Long]] = {
    val fromAnnots = pages.iterator.flatMap { (pageNumber, obj) =>
      dictAt(obj.data).toList.flatMap { dict =>
        annotRefs(dict).filter(widgets.contains).map(widget => pageNumber -> widget)
      }
    }.toList
    val fromParent = widgets.iterator.flatMap { number =>
      objects.get(number).flatMap(obj => dictAt(obj.data)).flatMap(_.data.get("P")).collect {
        case Prim.Ref(page, _) if pages.contains(page) => page -> number
      }
    }.toList
    (fromAnnots ++ fromParent)
      .groupMap(_._1)(_._2)
      .view
      .mapValues(_.distinct)
      .toMap
  }

  private def annotRefs(dict: Prim.Dict): List[Long] =
    dict.data.get("Annots") match {
      case Some(Prim.Array(entries)) =>
        entries.iterator.collect { case Prim.Ref(number, _) => number }.toList
      case Some(Prim.Ref(number, _)) =>
        List(number)
      case _ =>
        Nil
    }

  private def appearanceRef(widget: Prim.Dict, objects: Map[Long, Obj]): Option[Long] = {
    def fromNormal(normal: Prim): Option[Long] =
      normal match {
        case Prim.Ref(number, _) =>
          Some(number)
        case dict: Prim.Dict =>
          val state = nameAt(widget, "AS")
          state
            .flatMap(key => dict.data.get(key))
            .orElse(dict.data.get("Off"))
            .orElse(dict.data.values.headOption)
            .collect { case Prim.Ref(number, _) => number }
        case _ =>
          None
      }

    widget.data.get("AP") match {
      case Some(Prim.Ref(number, _)) =>
        objects.get(number).flatMap(obj => dictAt(obj.data)).flatMap { ap =>
          ap.data.get("N").flatMap(fromNormal).orElse(Some(number))
        }
      case Some(ap: Prim.Dict) =>
        ap.data.get("N").flatMap(fromNormal)
      case _ =>
        None
    }
  }

  private def appendContents(page: Prim.Dict, overlay: Prim.Ref): Prim.Dict =
    page.data.get("Contents") match {
      case Some(existing: Prim.Ref) =>
        Prim.Dict(page.data.updated("Contents", Prim.Array(existing, overlay)))
      case Some(Prim.Array(entries)) =>
        Prim.Array((entries.iterator.toSeq :+ overlay)*) match {
          case array => Prim.Dict(page.data.updated("Contents", array))
        }
      case _ =>
        Prim.Dict(page.data.updated("Contents", overlay))
    }

  private def addXObject(
    page: Prim.Dict,
    objects: Map[Long, Obj],
    name: String,
    ref: Prim.Ref
  ): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val xobjects  = resolvedDict(resources, "XObject", objects).getOrElse(Prim.dict())
    val updatedX  = Prim.Dict(xobjects.data.updated(name, ref))
    val updatedR  = Prim.Dict(resources.data.updated("XObject", updatedX))
    Prim.Dict(page.data.updated("Resources", updatedR))
  }

  private def addHelvetica(page: Prim.Dict, objects: Map[Long, Obj]): Prim.Dict = {
    val resources = resolvedDict(page, "Resources", objects).getOrElse(Prim.dict())
    val fonts     = resolvedDict(resources, "Font", objects).getOrElse(Prim.dict())
    if fonts.data.contains("Helv") then
      if page.data.get("Resources").exists(_.isInstanceOf[Prim.Dict]) &&
        resources.data.get("Font").exists(_.isInstanceOf[Prim.Dict])
      then page
      else
        val updatedR = Prim.Dict(resources.data.updated("Font", fonts))
        Prim.Dict(page.data.updated("Resources", updatedR))
    else
      val helv = Prim.dict(
        "Type"     -> Prim.Name("Font"),
        "Subtype"  -> Prim.Name("Type1"),
        "BaseFont" -> Prim.Name("Helvetica")
      )
      val updatedFonts = Prim.Dict(fonts.data.updated("Helv", helv))
      val updatedR     = Prim.Dict(resources.data.updated("Font", updatedFonts))
      Prim.Dict(page.data.updated("Resources", updatedR))
  }

  private def resolvedDict(owner: Prim.Dict, key: String, objects: Map[Long, Obj]): Option[Prim.Dict] =
    owner.data.get(key) match {
      case Some(dict: Prim.Dict)     => Some(dict)
      case Some(Prim.Ref(number, _)) => objects.get(number).flatMap(obj => dictAt(obj.data))
      case _                         => None
    }

  private def xobjectNames(page: Prim.Dict, objects: Map[Long, Obj]): Set[String] =
    resolvedDict(page, "Resources", objects)
      .flatMap(resources => resolvedDict(resources, "XObject", objects))
      .map(_.data.keys.toSet)
      .getOrElse(Set.empty)

  private def unusedName(used: Set[String], candidate: String): String =
    if !used.contains(candidate) then candidate
    else
      Iterator
        .from(2)
        .map(n => s"${candidate}_$n")
        .find(name => !used.contains(name))
        .getOrElse(candidate)

  private def applyPageUpdate(obj: Obj, pages: Map[Long, Prim.Dict]): Obj =
    pages.get(obj.index.number) match {
      case Some(dict) => Obj(obj.index, dict)
      case None       => obj
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

  private def walkFields(
    entries: List[Prim],
    objects: Map[Long, Obj],
    inheritedName: Option[String],
    inheritedType: Option[String],
    inheritedValue: Option[String],
    seen: Set[Long]
  ): FieldWalk =
    entries.foldLeft(FieldWalk(Nil, Set.empty)) { (acc, entry) =>
      val next = entry match {
        case Prim.Ref(number, _) if !seen.contains(number) =>
          objects.get(number).flatMap(obj => dictAt(obj.data)) match {
            case Some(dict) =>
              walkFieldDict(number, dict, objects, inheritedName, inheritedType, inheritedValue, seen + number)
            case None =>
              FieldWalk(Nil, Set.empty)
          }
        case dict: Prim.Dict =>
          walkFieldDict(0L, dict, objects, inheritedName, inheritedType, inheritedValue, seen)
        case _ =>
          FieldWalk(Nil, Set.empty)
      }
      FieldWalk(acc.fields ++ next.fields, acc.nodes ++ next.nodes)
    }

  private def walkFieldDict(
    number: Long,
    dict: Prim.Dict,
    objects: Map[Long, Obj],
    inheritedName: Option[String],
    inheritedType: Option[String],
    inheritedValue: Option[String],
    seen: Set[Long]
  ): FieldWalk = {
    val name  = qualifyName(inheritedName, nameAt(dict, "T"))
    val ft    = nameAt(dict, "FT").orElse(inheritedType)
    val value = stringAt(dict, "V").orElse(inheritedValue)
    val kids  = kidsOf(dict)
    val child = walkFields(kids, objects, name, ft, value, seen)
    val leaf  = kids.isEmpty || isWidget(dict) || child.fields.isEmpty
    val self  =
      if leaf && (name.nonEmpty || ft.nonEmpty || value.nonEmpty || isWidget(dict)) then
        List(Field(name, ft, number, value))
      else Nil
    FieldWalk(self ++ child.fields, child.nodes + number)
  }

  private def kidsOf(dict: Prim.Dict): List[Prim] =
    dict.data.get("Kids") match {
      case Some(Prim.Array(entries)) => entries.toList
      case _                         => Nil
    }

  private def qualifyName(parent: Option[String], local: Option[String]): Option[String] =
    (parent, local) match {
      case (Some(prefix), Some(name)) => Some(s"$prefix.$name")
      case (Some(prefix), None)       => Some(prefix)
      case (None, Some(name))         => Some(name)
      case (None, None)               => None
    }

  private val identityMatrix: (Double, Double, Double, Double, Double, Double) =
    (1.0, 0.0, 0.0, 1.0, 0.0, 0.0)

  private def matrixAt(dict: Prim.Dict): (Double, Double, Double, Double, Double, Double) =
    dict.data.get("Matrix") match {
      case Some(Prim.Array(entries)) if entries.length >= 6 =>
        (
          asDouble(entries(0)),
          asDouble(entries(1)),
          asDouble(entries(2)),
          asDouble(entries(3)),
          asDouble(entries(4)),
          asDouble(entries(5))
        ) match {
          case (Some(a), Some(b), Some(c), Some(d), Some(e), Some(f)) => (a, b, c, d, e, f)
          case _                                                    => identityMatrix
        }
      case _ =>
        identityMatrix
    }

  private def transformBox(
    box: (Double, Double, Double, Double),
    matrix: (Double, Double, Double, Double, Double, Double)
  ): (Double, Double, Double, Double) = {
    val (x1, y1, x2, y2) = normalize(box)
    val (a, b, c, d, e, f) = matrix
    def apply(x: Double, y: Double): (Double, Double) =
      (a * x + c * y + e, b * x + d * y + f)
    val corners = List(apply(x1, y1), apply(x2, y1), apply(x1, y2), apply(x2, y2))
    val xs      = corners.map(_._1)
    val ys      = corners.map(_._2)
    (xs.min, ys.min, xs.max, ys.max)
  }

  private def nameAt(dict: Prim.Dict, key: String): Option[String] =
    dict.data.get(key).collect { case Prim.Name(value) => value }.orElse(stringAt(dict, key))

  private def stringAt(dict: Prim.Dict, key: String): Option[String] =
    dict.data.get(key).collect {
      case Prim.Str(value)    => new String(value.toArray, StandardCharsets.ISO_8859_1)
      case Prim.HexStr(value) => new String(value.toArray, StandardCharsets.ISO_8859_1)
      case Prim.Name(value)   => value
      case Prim.Number(value) => value.toString
    }

  private def rectAt(dict: Prim.Dict): Option[(Double, Double, Double, Double)] =
    nums4(dict.data.get("Rect"))

  private def bboxAt(dict: Prim.Dict): Option[(Double, Double, Double, Double)] =
    nums4(dict.data.get("BBox"))

  private def nums4(value: Option[Prim]): Option[(Double, Double, Double, Double)] =
    value.collect {
      case Prim.Array(entries) if entries.length >= 4 =>
        (asDouble(entries(0)), asDouble(entries(1)), asDouble(entries(2)), asDouble(entries(3)))
    }.collect { case (Some(a), Some(b), Some(c), Some(d)) =>
      (a, b, c, d)
    }

  private def asDouble(value: Prim): Option[Double] =
    value match {
      case Prim.Number(number) => Some(number.toDouble)
      case _                   => None
    }

  private def rectAsBBox(rect: (Double, Double, Double, Double)): (Double, Double, Double, Double) = {
    val (x1, y1, x2, y2) = normalize(rect)
    (0.0, 0.0, x2 - x1, y2 - y1)
  }

  private def normalize(box: (Double, Double, Double, Double)): (Double, Double, Double, Double) = {
    val (a, b, c, d) = box
    (math.min(a, c), math.min(b, d), math.max(a, c), math.max(b, d))
  }

  private def pdfNum(value: Double): String =
    if value.isWhole && value.abs <= Long.MaxValue.toDouble then value.toLong.toString
    else BigDecimal(value).bigDecimal.stripTrailingZeros.toPlainString

  private def pdfLiteral(value: String): String = {
    val escaped = value.flatMap {
      case '\\' => "\\\\"
      case '('  => "\\("
      case ')'  => "\\)"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case char => char.toString
    }
    s"($escaped)"
  }

  private def isCatalogWithForm(obj: Obj): Boolean =
    dictAt(obj.data).exists(dict => isCatalog(dict) && dict.data.contains("AcroForm"))

  private def isCatalog(dict: Prim.Dict): Boolean =
    dict.data.get("Type").contains(Prim.Name("Catalog"))

  private def isPage(dict: Prim.Dict): Boolean =
    dict.data.get("Type").contains(Prim.Name("Page"))

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

  private def setFieldValue(dict: Prim.Dict, value: String): Prim.Dict =
    Prim.Dict(
      withoutKey(dict, "AP").data.updated(
        "V",
        Prim.Str(_root_.scodec.bits.ByteVector(value.getBytes(StandardCharsets.ISO_8859_1)))
      )
    )

  private def objectMap(decoded: Chunk[Decoded]): Map[Long, Obj] =
    decoded.collect {
      case Decoded.DataObj(obj)          => obj.index.number -> obj
      case Decoded.ContentObj(obj, _, _) => obj.index.number -> obj
    }.toMap

  private def streamMap(decoded: Chunk[Decoded]): Map[Long, BitVector] =
    decoded.collect { case Decoded.ContentObj(obj, rawStream, _) =>
      obj.index.number -> rawStream
    }.toMap
}
