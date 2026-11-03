package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1
import kyo.*

/** Page assembly keeps content as PDF objects; it never rasterizes source pages. */
object PdfAssembly:
  import PdfValue.*

  private def checked[A](value: => A): A < (Abort[PdfError] & Sync) =
    Abort.catching[Exception](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse("Invalid PDF assembly")))(Sync.defer(value))

  private def normalized(document: PdfDocument): PdfDocument < Abort[PdfError] =
    PdfPages.pageRefs(document).map(pages => PdfPages.select(document, Vector.tabulate(pages.length)(_ + 1)))

  private def collect(documents: Vector[PdfDocument], index: Int = 0, found: Vector[PdfDocument] = Vector.empty): Vector[PdfDocument] < Abort[PdfError] =
    if index == documents.length then found
    else normalized(documents(index)).map(value => collect(documents, index + 1, found :+ value))

  /** Concatenate pages in input order. Document-level metadata comes from catalogIndex.
    * Other catalogs' outlines, names and document metadata are not merged. Interactive
    * forms are rejected when combining documents rather than silently losing fields.
    */
  def concatenate(documents: Vector[PdfDocument], catalogIndex: Int = 0): PdfDocument < (Abort[PdfError] & Sync) =
    if documents.isEmpty || catalogIndex < 0 || catalogIndex >= documents.length then
      Abort.fail(PdfError.InvalidPdf("Choose PDFs and a valid primary catalog"))
    else collect(documents).map { inputs => checked {
      if inputs.length > 1 then require(inputs.forall(doc => catalog(doc).get("AcroForm").isEmpty), "Combining interactive PDF forms is unsupported")
      var next = 3L
      val mappings = inputs.map { doc =>
        val pairs = doc.objects.filterNot(obj => obj.index == ObjectRef(1) || obj.index == ObjectRef(2)).map { obj =>
          val pair = obj.index -> ObjectRef(next)
          next += 1
          pair
        }
        pairs.toMap ++ Map(ObjectRef(1) -> ObjectRef(1), ObjectRef(2) -> ObjectRef(2))
      }
      val imported = inputs.zip(mappings).flatMap { (doc, mapping) =>
        doc.objects.filterNot(obj => obj.index == ObjectRef(1) || obj.index == ObjectRef(2))
          .map(obj => obj.copy(index = mapping(obj.index), value = rewrite(obj.value, mapping)))
      }
      val kids = inputs.zip(mappings).flatMap { (doc, mapping) =>
        val source = doc.byRef(ObjectRef(2)).dictionary.get.get("Kids").get.asInstanceOf[Array]
        source.values.map(value => rewrite(value, mapping))
      }
      val primary = inputs(catalogIndex)
      val root = rewrite(catalog(primary), mappings(catalogIndex)).asInstanceOf[Dict].updated("Pages", Ref(ObjectRef(2)))
      val trailer = rewrite(primary.trailer, mappings(catalogIndex)).asInstanceOf[Dict]
        .removed("Size").updated("Root", Ref(ObjectRef(1)))
      PdfDocument(inputs.map(_.version).maxBy(_.toDoubleOption.getOrElse(1.7)),
        Vector(PdfObject(ObjectRef(1), root), PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Kids" -> Array(kids), "Count" -> Number(kids.length)))) ++ imported, trailer)
    }}

  /** Overlay one artwork page per source page. Artwork uses displayed crop coordinates
    * with an origin at zero. Source rotation/crop offsets are respected. A Form XObject
    * isolates artwork resources, and q/Q isolates the source graphics state.
    */
  def overlay(document: PdfDocument, artwork: PdfDocument): PdfDocument < (Abort[PdfError] & Sync) =
    normalized(document).map { base => normalized(artwork).map { art =>
      PdfPages.pageRefs(base).map { pages => PdfPages.pageRefs(art).map { artPages =>
        if pages.length != artPages.length then Abort.fail(PdfError.InvalidPdf("Artwork must have one page per output page"))
        else
          def decode(index: Int, found: Vector[Vector[Byte]]): Vector[Vector[Byte]] < (Abort[PdfError] & Sync) =
            if index == artPages.length then found
            else checked(contentObjects(art, art.byRef(artPages(index)).dictionary.get)).map { streams =>
              def join(position: Int, chunks: Vector[Vector[Byte]], size: Int): Vector[Byte] < (Abort[PdfError] & Sync) =
                if position == streams.length then chunks.flatten
                else PdfDocument.decodedStream(streams(position), 1024 * 1024).map { next =>
                  val total = size.toLong + next.length + 1
                  if total > 1024 * 1024 then Abort.fail(PdfError.TooLarge(1024 * 1024, total))
                  else join(position + 1, chunks :+ next :+ Vector('\n'.toByte), total.toInt)
                }
              join(0, Vector.empty, 0).map(bytes => decode(index + 1, found :+ bytes))
            }
          decode(0, Vector.empty).map(streams => checked(applyArtwork(base, art, pages, artPages, streams)))
      }}
    }}

  private def catalog(document: PdfDocument): Dict = document.byRef(document.root.get).dictionary.get

  private def rewrite(value: PdfValue, mapping: Map[ObjectRef, ObjectRef]): PdfValue = value match
    case Ref(ref) => Ref(mapping.getOrElse(ref, throw IllegalArgumentException(s"Missing assembly reference ${ref.number}")))
    case Array(values) => Array(values.map(rewrite(_, mapping)))
    case Dict(fields) => Dict(fields.map(field => field.copy(value = rewrite(field.value, mapping))))
    case other => other

  private def resolve(document: PdfDocument, value: PdfValue, seen: Set[ObjectRef] = Set.empty): PdfValue = value match
    case Ref(ref) =>
      require(!seen(ref) && seen.size < 32, "Cyclic or over-deep PDF resource reference")
      resolve(document, document.byRef.getOrElse(ref, throw IllegalArgumentException("Missing PDF resource")).value, seen + ref)
    case other => other

  private def contentObjects(document: PdfDocument, page: Dict): Vector[PdfObject] =
    def walk(value: PdfValue, seen: Set[ObjectRef]): Vector[PdfObject] = value match
      case Ref(ref) =>
        require(!seen(ref) && seen.size < 32, "Cyclic or over-deep PDF contents")
        val obj = document.byRef.getOrElse(ref, throw IllegalArgumentException("Missing PDF content stream"))
        if obj.stream.nonEmpty then Vector(obj) else walk(obj.value, seen + ref)
      case Array(values) =>
        require(values.length <= 64, "Too many artwork content streams")
        values.flatMap(walk(_, seen))
      case _ => throw IllegalArgumentException("PDF contents must reference streams")
    page.get("Contents").fold(Vector.empty[PdfObject])(walk(_, Set.empty))

  private def dictionary(document: PdfDocument, value: Option[PdfValue]): Dict =
    value.fold(dict()) { source => resolve(document, source) match
      case found: Dict => found
      case _ => throw IllegalArgumentException("PDF resources must be dictionaries")
    }

  private def box(document: PdfDocument, page: Dict): Vector[BigDecimal] =
    def rectangle(raw: PdfValue): Vector[BigDecimal] = resolve(document, raw) match
        case Array(values) if values.length == 4 => values.map(value => resolve(document, value) match
          case Number(number) => number
          case _ => throw IllegalArgumentException("Invalid PDF page box"))
        case _ => throw IllegalArgumentException("Invalid PDF page box")
    val media = rectangle(page.get("MediaBox").getOrElse(throw IllegalArgumentException("PDF page has no media box")))
    val crop = page.get("CropBox").fold(media)(rectangle)
    val numbers = Vector(media(0).max(crop(0)), media(1).max(crop(1)), media(2).min(crop(2)), media(3).min(crop(3)))
    require(numbers.forall(n => n.abs <= 100000) && numbers(2) > numbers(0) && numbers(3) > numbers(1), "PDF page box is outside assembly bounds")
    numbers

  private def applyArtwork(base: PdfDocument, art: PdfDocument, pages: Vector[ObjectRef], artPages: Vector[ObjectRef], streams: Vector[Vector[Byte]]): PdfDocument =
    var next = base.objects.map(_.index.number).max + 1
    def allocate(): ObjectRef =
      val ref = ObjectRef(next)
      next += 1
      ref
    def refs(value: PdfValue): Vector[ObjectRef] = value match
      case Ref(ref) => Vector(ref)
      case Array(values) => values.flatMap(refs)
      case Dict(fields) => fields.flatMap(field => refs(field.value))
      case _ => Vector.empty
    val retained = scala.collection.mutable.LinkedHashSet.empty[ObjectRef]
    var pending = artPages.flatMap { ref =>
      val page = art.byRef(ref).dictionary.get
      refs(dictionary(art, page.get("Resources"))) ++ page.get("Group").toVector.flatMap(refs)
    }.toList
    while pending.nonEmpty do
      val ref = pending.head
      pending = pending.tail
      if !retained(ref) then
        retained += ref
        pending = refs(art.byRef(ref).value).toList ::: pending
    val dependencies = art.objects.filter(obj => retained(obj.index))
    val imported = dependencies.map(obj => obj.index -> allocate()).toMap
    val objects = scala.collection.mutable.ArrayBuffer.from(base.objects)
    objects ++= dependencies.map(obj => obj.copy(index = imported(obj.index), value = rewrite(obj.value, imported)))
    pages.zipWithIndex.foreach { (pageRef, index) =>
      val page = base.byRef(pageRef).dictionary.get
      val overlay = art.byRef(artPages(index)).dictionary.get
      val bounds = box(base, page)
      val x = bounds(0); val y = bounds(1); val width = bounds(2) - x; val height = bounds(3) - y
      val rotation = page.get("Rotate").map(resolve(base, _)).fold(0) {
        case Number(n) if n.isValidInt => ((n.toInt % 360) + 360) % 360
        case _ => throw IllegalArgumentException("Invalid PDF page rotation")
      }
      val matrix = rotation match
        case 0 => s"1 0 0 1 $x $y"
        case 90 => s"0 1 -1 0 ${x + width} $y"
        case 180 => s"-1 0 0 -1 ${x + width} ${y + height}"
        case 270 => s"0 -1 1 0 $x ${y + height}"
        case _ => throw IllegalArgumentException("Page rotation must be a multiple of 90")
      val artBox = box(art, overlay)
      val expected = if rotation == 90 || rotation == 270 then Vector(height, width) else Vector(width, height)
      require(artBox.take(2) == Vector(BigDecimal(0), BigDecimal(0)) &&
        (artBox(2) - expected(0)).abs < BigDecimal("0.01") && (artBox(3) - expected(1)).abs < BigDecimal("0.01"), "Artwork must match the displayed crop box")
      val form = allocate(); val before = allocate(); val after = allocate()
      val resources = dictionary(base, page.get("Resources"))
      val xobjects = dictionary(base, resources.get("XObject"))
      val name = Iterator.from(0).map(i => s"PMOverlay$i").find(value => xobjects.get(value).isEmpty).get
      val artResources = rewrite(dictionary(art, overlay.get("Resources")), imported)
      val formDictionary = dict("Type" -> Name("XObject"), "Subtype" -> Name("Form"), "FormType" -> Number(1),
        "BBox" -> Array(artBox.map(Number.apply)), "Resources" -> artResources)
      objects += PdfObject(form, overlay.get("Group").fold(formDictionary)(group => formDictionary.updated("Group", rewrite(group, imported))), Some(streams(index)))
      objects += PdfObject(before, dict(), Some("q\n".getBytes(ISO_8859_1).toVector))
      objects += PdfObject(after, dict(), Some(s"\nQ\nq\n$matrix cm\n/$name Do\nQ\n".getBytes(ISO_8859_1).toVector))
      val original = contentObjects(base, page).map(obj => Ref(obj.index))
      val updated = page.updated("Resources", resources.updated("XObject", xobjects.updated(name, Ref(form))))
        .updated("Contents", Array(Vector(Ref(before)) ++ original :+ Ref(after)))
      val position = objects.indexWhere(_.index == pageRef)
      objects(position) = PdfObject(pageRef, updated)
    }
    base.copy(objects = objects.toVector, trailer = base.trailer.removed("Size"))
