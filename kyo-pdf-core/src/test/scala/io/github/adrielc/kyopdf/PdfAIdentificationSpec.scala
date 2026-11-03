package io.github.adrielc.kyopdf

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.{UTF_8, UTF_16}
import java.util.zip.DeflaterOutputStream
import kyo.{Abort, Result, Sync, `<`}
import zio.test.*

object PdfAIdentificationSpec extends ZIOSpecDefault:
  import PdfValue.*
  private def xmp(properties: String, prefix: String = "pdfaid", namespace: String = PdfAIdentification.Namespace): String =
    s"""<x:xmpmeta xmlns:x="adobe:ns:meta/"><r:RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><r:Description r:about="" xmlns:$prefix="$namespace">$properties</r:Description></r:RDF></x:xmpmeta>"""
  private def document(bytes: scala.Array[Byte], filter: Option[String] = None): PdfDocument =
    val fields = dict("Type" -> Name("Metadata"), "Subtype" -> Name("XML"))
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Metadata" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), filter.fold(fields)(value => fields.updated("Filter", Name(value))), Some(bytes.toVector))
    ), dict("Root" -> Ref(ObjectRef(1))))
  private def run(program: PdfAIdentification < (Abort[PdfError] & Sync)): PdfAIdentification =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case Result.Success(value) => value
      case _ => throw AssertionError("Identification unexpectedly failed")
  private val declaration = "<pdfaid:part>3</pdfaid:part><pdfaid:conformance>B</pdfaid:conformance>"

  def spec = suite("PDF/A declaration identification")(
    test("namespaced element declarations are evidence, not conformance") {
      val found = run(PdfAIdentification.inspect(document(xmp(declaration).getBytes(UTF_8))))
      assertTrue(found.declares3b, found.metadata.contains(ObjectRef(2)), found.state == PdfADeclarationState.Declared)
    },
    test("alternate prefixes, attributes, and UTF-16 BOM are supported") {
      val xml = s"""<r:RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#"><r:Description xmlns:a="${PdfAIdentification.Namespace}" a:part="3" a:conformance="B"/></r:RDF>"""
      assertTrue(run(PdfAIdentification.inspect(document(xml.getBytes(UTF_16)))).declares3b)
    },
    test("compressed XMP is decoded before identification") {
      val output = new ByteArrayOutputStream()
      val compressor = new DeflaterOutputStream(output)
      compressor.write(xmp(declaration).getBytes(UTF_8))
      compressor.close()
      assertTrue(run(PdfAIdentification.inspect(document(output.toByteArray, Some("FlateDecode")))).declares3b)
    },
    test("a familiar prefix in the wrong namespace is not evidence") {
      val found = run(PdfAIdentification.inspect(document(xmp(declaration, namespace = "urn:example:unrelated").getBytes(UTF_8))))
      assertTrue(found.state == PdfADeclarationState.NotDeclared, !found.declares3b)
    },
    test("orphaned metadata and stale declarations do not identify the active catalog") {
      val original = document(xmp(declaration).getBytes(UTF_8))
      val noMetadata = original.copy(objects = original.objects.updated(0, PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog")))))
      val replacement = original.copy(objects = original.objects :+ PdfObject(ObjectRef(2), dict("Type" -> Name("Metadata"), "Subtype" -> Name("XML")), Some(xmp("").getBytes(UTF_8).toVector)))
      assertTrue(run(PdfAIdentification.inspect(noMetadata)).state == PdfADeclarationState.NotDeclared,
        run(PdfAIdentification.inspect(replacement)).state == PdfADeclarationState.NotDeclared)
    },
    test("conflicting and incomplete properties stay unverified") {
      val conflict = xmp(declaration + "<pdfaid:part>2</pdfaid:part>")
      val partial = xmp("<pdfaid:part>3</pdfaid:part>")
      assertTrue(run(PdfAIdentification.inspect(document(conflict.getBytes(UTF_8)))).state == PdfADeclarationState.Conflicting,
        run(PdfAIdentification.inspect(document(partial.getBytes(UTF_8)))).state == PdfADeclarationState.Unreadable)
    },
    test("malformed XML, DTD, external entities, and nested property values are rejected") {
      val inputs = Vector("<broken>", "<!DOCTYPE x [<!ENTITY e SYSTEM 'file:///unavailable'>]>" + xmp(declaration),
        xmp("<pdfaid:part><r:Description>3</r:Description></pdfaid:part>"))
      assertTrue(inputs.forall(xml => run(PdfAIdentification.inspect(document(xml.getBytes(UTF_8)))).state == PdfADeclarationState.Unreadable))
    },
    test("metadata expansion, depth, and field lengths have bounds") {
      val inputs = Vector(" " * (512 * 1024 + 1), "<x>" * 33 + "</x>" * 33,
        xmp("<pdfaid:part>" + "3" * 65 + "</pdfaid:part>"))
      assertTrue(inputs.forall(xml => run(PdfAIdentification.inspect(document(xml.getBytes(UTF_8)))).state == PdfADeclarationState.Unreadable))
    },
    test("non-document RDF subjects cannot declare this PDF") {
      val xml = xmp(declaration).replace("r:about=\"\"", "r:about=\"urn:example:another-resource\"")
      assertTrue(run(PdfAIdentification.inspect(document(xml.getBytes(UTF_8)))).state == PdfADeclarationState.NotDeclared)
    }
  )
