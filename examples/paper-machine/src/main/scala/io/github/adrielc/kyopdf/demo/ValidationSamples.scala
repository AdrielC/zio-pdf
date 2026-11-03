package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import java.nio.charset.StandardCharsets.UTF_8

/** Original, tiny specimens. The false claim differs by a forbidden JavaScript action. */
object ValidationSamples:
  def document(falseClaim: Boolean): PdfDocument =
    import PdfValue.*
    val xml = ("<?xpacket begin=\"\uFEFF\" id=\"W5M0MpCehiHzreSzNTczkc9d\"?>" +
      "<x:xmpmeta xmlns:x=\"adobe:ns:meta/\"><rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\">" +
      s"<rdf:Description rdf:about=\"\" xmlns:pdfaid=\"${PdfAIdentification.Namespace}\" pdfaid:part=\"3\" pdfaid:conformance=\"B\"/>" +
      "</rdf:RDF></x:xmpmeta><?xpacket end=\"w\"?>").getBytes(UTF_8).toVector
    val name = Literal("notes.txt".getBytes(UTF_8).toVector)
    val attachment = "An original synthetic attachment for PDF/A-3 validation.\n".getBytes(UTF_8).toVector
    val catalog = dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)), "Metadata" -> Ref(ObjectRef(4)),
      "AF" -> Array(Vector(Ref(ObjectRef(5)))), "Names" -> dict("EmbeddedFiles" -> dict("Names" -> Array(Vector(name, Ref(ObjectRef(5)))))))
    val objects = Vector(
      PdfObject(ObjectRef(1), if falseClaim then catalog.updated("OpenAction", Ref(ObjectRef(7))) else catalog),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "Resources" -> dict(),
        "MediaBox" -> Array(Vector(Number(0), Number(0), Number(595), Number(420))))),
      PdfObject(ObjectRef(4), dict("Type" -> Name("Metadata"), "Subtype" -> Name("XML")), Some(xml)),
      PdfObject(ObjectRef(5), dict("Type" -> Name("Filespec"), "F" -> name, "UF" -> name, "AFRelationship" -> Name("Data"),
        "Desc" -> Literal("Synthetic text attachment".getBytes(UTF_8).toVector), "EF" -> dict("F" -> Ref(ObjectRef(6)), "UF" -> Ref(ObjectRef(6))))),
      PdfObject(ObjectRef(6), dict("Type" -> Name("EmbeddedFile"), "Subtype" -> Name("text/plain"),
        "Params" -> dict("Size" -> Number(attachment.length), "ModDate" -> Literal("D:20260101000000Z".getBytes(UTF_8).toVector))), Some(attachment))
    ) ++ (if falseClaim then Vector(PdfObject(ObjectRef(7), dict("Type" -> Name("Action"), "S" -> Name("JavaScript"),
      "JS" -> Literal("void(0)".getBytes(UTF_8).toVector)))) else Vector.empty)
    PdfDocument("1.7", objects, dict("Root" -> Ref(ObjectRef(1)),
      "ID" -> Array(Vector(Hex(Vector.tabulate(16)(_.toByte)), Hex(Vector.tabulate(16)(i => (i + 1).toByte))))))
