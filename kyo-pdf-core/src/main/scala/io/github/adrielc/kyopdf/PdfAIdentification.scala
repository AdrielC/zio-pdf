package io.github.adrielc.kyopdf

import kyo.*
import scala.collection.mutable.ArrayBuffer

/** Producer declarations only. Conformance requires an independent validator. */
enum PdfADeclarationState derives Schema:
  case NotDeclared, Declared, Conflicting, Unreadable

final case class PdfAIdentification(
  state: PdfADeclarationState,
  metadata: Option[ObjectRef],
  parts: Vector[String],
  conformances: Vector[String],
  notes: Vector[String]
) derives Schema:
  def declares3b: Boolean = state == PdfADeclarationState.Declared && parts == Vector("3") && conformances == Vector("B")

object PdfAIdentification:
  val Namespace = "http://www.aiim.org/pdfa/ns/id/"
  private val Rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  private val MaxMetadataBytes = 512 * 1024

  /** Read only the current catalog's XMP; orphaned and embedded-file metadata cannot identify this document. */
  def inspect(document: PdfDocument): PdfAIdentification < (Abort[PdfError] & Sync) =
    val catalog = document.root.flatMap(document.byRef.get).flatMap(_.dictionary)
    catalog match
      case None => Abort.fail(PdfError.InvalidPdf("PDF catalog is unreadable"))
      case Some(value) => value.get("Metadata") match
        case None => PdfAIdentification(PdfADeclarationState.NotDeclared, None, Vector.empty, Vector.empty, Vector.empty)
        case Some(PdfValue.Ref(ref)) => document.byRef.get(ref) match
          case Some(obj) if obj.dictionary.exists(dict =>
              dict.get("Type").contains(PdfValue.Name("Metadata")) && dict.get("Subtype").contains(PdfValue.Name("XML"))) =>
            val parsed = PdfDocument.decodedStream(obj, MaxMetadataBytes).map { bytes =>
              Abort.catching[Exception](_ => PdfError.InvalidPdf("Malformed, unsafe, or over-complex XMP metadata")) {
                Sync.defer(parse(bytes.toArray, ref))
              }
            }
            Abort.run[PdfError](parsed).map {
              case Result.Success(found) => found
              case Result.Failure(error) => unreadable(Some(ref), error.message.take(240))
              case Result.Panic(_) => unreadable(Some(ref), "XMP metadata could not be read")
            }
          case _ => unreadable(Some(ref), "Catalog Metadata does not resolve to an XML metadata stream")
        case _ => unreadable(None, "Catalog Metadata is not an indirect stream reference")

  private def unreadable(ref: Option[ObjectRef], note: String): PdfAIdentification =
    PdfAIdentification(PdfADeclarationState.Unreadable, ref, Vector.empty, Vector.empty, Vector(note))

  private def parse(bytes: Array[Byte], ref: ObjectRef): PdfAIdentification =
    val parts = ArrayBuffer.empty[String]
    val levels = ArrayBuffer.empty[String]
    var depth = 0
    var rdfDepth = -1
    var descriptionDepth = -1
    var property: Option[String] = None
    val text = new StringBuilder
    def add(name: String, value: String): Unit =
      val trimmed = value.trim
      require(trimmed.length <= 64, "PDF/A identification value exceeds its bound")
      if name == "part" then parts += trimmed.toIntOption.filter(_ >= 0).fold(trimmed)(_.toString)
      else if name == "conformance" then levels += trimmed
      require(parts.length + levels.length <= 32, "Too many PDF/A identification properties")
    XmpReader.read(bytes, new XmpEvents:
      def start(namespace: String, local: String, attributes: => Iterator[(String, String, String)]): Unit =
        depth += 1
        require(depth <= 32 && property.isEmpty, "Nested or over-deep XMP identification")
        if namespace == Rdf && local == "RDF" && depth <= 2 && rdfDepth == -1 then rdfDepth = depth
        else if namespace == Rdf && local == "Description" && rdfDepth >= 0 && depth == rdfDepth + 1 then
          val values = attributes.toVector
          if values.find(a => a._1 == Rdf && a._2 == "about").forall(_._3.isEmpty) then
            descriptionDepth = depth
            values.foreach { (ns, name, value) => if ns == Namespace then add(name, value) }
        else if descriptionDepth >= 0 && depth == descriptionDepth + 1 && namespace == Namespace &&
            (local == "part" || local == "conformance") then
          property = Some(local)
          text.clear()
      def characters(value: => String): Unit =
        if property.nonEmpty then
          text.append(value)
          require(text.length <= 64, "PDF/A identification value exceeds its bound")
      def end(): Unit =
        property.foreach(name => add(name, text.toString))
        property = None
        if depth == descriptionDepth then descriptionDepth = -1
        if depth == rdfDepth then rdfDepth = -1
        depth -= 1
    )
    val uniqueParts = parts.toVector.distinct
    val uniqueLevels = levels.toVector.distinct
    val state =
      if uniqueParts.length > 1 || uniqueLevels.length > 1 then PdfADeclarationState.Conflicting
      else if uniqueParts.isEmpty && uniqueLevels.isEmpty then PdfADeclarationState.NotDeclared
      else if uniqueParts.isEmpty || uniqueParts.head.isEmpty ||
          (Set("1", "2", "3")(uniqueParts.head) && (uniqueLevels.isEmpty || uniqueLevels.head.isEmpty)) then PdfADeclarationState.Unreadable
      else PdfADeclarationState.Declared
    val notes = Vector.newBuilder[String]
    if state == PdfADeclarationState.Conflicting then notes += "Conflicting PDF/A identification properties"
    if state == PdfADeclarationState.Unreadable then notes += "Incomplete PDF/A identification properties"
    if parts.length != uniqueParts.length || levels.length != uniqueLevels.length then notes += "Repeated identification properties"
    PdfAIdentification(state, Some(ref), uniqueParts, uniqueLevels, notes.result())
