package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1
import kyo.*
import zio.test.*

object PdfAssemblySpec extends ZIOSpecDefault:
  import PdfValue.*
  private def document(label: String, rotation: Int = 0, offset: Int = 0): PdfDocument =
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)), "Label" -> Name(label))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(1), "Kids" -> Array(Vector(Ref(ObjectRef(3)))),
        "MediaBox" -> Array(Vector(Number(offset), Number(offset), Number(offset + 300), Number(offset + 400))), "Rotate" -> Number(rotation))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "Contents" -> Ref(ObjectRef(4)),
        "Resources" -> dict("XObject" -> dict("PMOverlay0" -> Ref(ObjectRef(5)))))),
      PdfObject(ObjectRef(4), dict(), Some(s"% $label\n1 0 0 1 9 8 cm\n".getBytes(ISO_8859_1).toVector)),
      PdfObject(ObjectRef(5), dict("Type" -> Name("XObject"), "Subtype" -> Name("Form"), "BBox" -> Array(Vector(Number(0), Number(0), Number(1), Number(1)))), Some(Vector.empty))
    ), dict("Root" -> Ref(ObjectRef(1)), "Prev" -> Number(100)))
  private def result[A](program: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(program)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def run[A](program: A < (Abort[PdfError] & Sync)): A = result(program).fold(error => throw AssertionError(error.message), identity)
  private def page(document: PdfDocument): Dict = document.byRef(run(PdfPages.pageRefs(document)).head).dictionary.get

  def spec = suite("PDF assembly")(
    test("bookends and body keep source order, resources and a single parent despite colliding object numbers") {
      val assembled = run(PdfAssembly.concatenate(Vector(document("HEAD"), document("BODY"), document("TAIL")), 1))
      val pages = run(PdfPages.pageRefs(assembled))
      val labels = pages.map(ref => assembled.byRef(ref).dictionary.get.get("Contents").get).map {
        case Ref(ref) => new String(assembled.byRef(ref).stream.get.toArray, ISO_8859_1)
        case _ => ""
      }
      assertTrue(labels.zip(Vector("HEAD", "BODY", "TAIL")).forall((text, label) => text.contains(label)),
        pages.distinct.length == 3, pages.forall(ref => assembled.byRef(ref).dictionary.get.get("Parent").contains(Ref(ObjectRef(2)))),
        assembled.byRef(ObjectRef(1)).dictionary.get.get("Label").contains(Name("BODY")), assembled.trailer.get("Prev").isEmpty)
    },
    test("overlays isolate resource names and graphics state without replacing source content") {
      val base = document("SOURCE")
      val output = run(PdfAssembly.overlay(base, document("STAMP")))
      val resources = page(output).get("Resources").get.asInstanceOf[Dict]
      val names = resources.get("XObject").get.asInstanceOf[Dict]
      val content = page(output).get("Contents").get.asInstanceOf[Array].values.collect { case Ref(ref) => output.byRef(ref).stream.get }
      assertTrue(names.get("PMOverlay0").nonEmpty, names.get("PMOverlay1").nonEmpty, content.length == 3,
        new String(content.head.toArray, ISO_8859_1) == "q\n", new String(content(1).toArray, ISO_8859_1).contains("SOURCE"),
        new String(content.last.toArray, ISO_8859_1).contains("Q\nq\n1 0 0 1 0 0 cm\n/PMOverlay1 Do"))
    },
    test("rotated offset pages use displayed crop coordinates") {
      val art = document("ART").copy(objects = document("ART").objects.updated(1,
        document("ART").objects(1).copy(value = document("ART").objects(1).dictionary.get.updated("MediaBox", Array(Vector(Number(0), Number(0), Number(400), Number(300)))))))
      val output = run(PdfAssembly.overlay(document("SOURCE", 90, 20), art))
      val last = page(output).get("Contents").get.asInstanceOf[Array].values.last.asInstanceOf[Ref].value
      assertTrue(new String(output.byRef(last).stream.get.toArray, ISO_8859_1).contains("0 1 -1 0 320 20 cm"))
    },
    test("assembly roundtrips through the writer with a rewritten trailer and active pages") {
      val output = run(PdfAssembly.concatenate(Vector(document("FIRST"), document("SECOND"))))
      val bytes = run(PdfWriter.write(run(PdfAssembly.overlay(output, output))))
      val decoded = run(ByteLimit.fromBytes(1024 * 1024).map(limit => PdfDocument.decode(bytes.toArray, limit)))
      assertTrue(run(PdfPages.pageRefs(decoded)).length == 2, decoded.trailer.get("Prev").isEmpty,
        decoded.objects.map(_.index).distinct.length == decoded.objects.length)
    },
    test("crop boxes extending outside the media box use their visible intersection") {
      val source = document("SOURCE")
      val cropped = source.copy(objects = source.objects.updated(2, source.objects(2).copy(value =
        source.objects(2).dictionary.get.updated("CropBox", Array(Vector(Number(-20), Number(-30), Number(500), Number(600)))))))
      val output = run(PdfAssembly.overlay(cropped, document("ART")))
      val last = page(output).get("Contents").get.asInstanceOf[Array].values.last.asInstanceOf[Ref].value
      assertTrue(new String(output.byRef(last).stream.get.toArray, ISO_8859_1).contains("1 0 0 1 0 0 cm"))
    },
    test("invalid assembly, mismatched artwork, encrypted input and interactive form merges fail explicitly") {
      val source = document("SOURCE")
      val form = source.copy(objects = source.objects.updated(0, source.objects.head.copy(value = source.objects.head.dictionary.get.updated("AcroForm", dict()))))
      val encrypted = source.copy(trailer = source.trailer.updated("Encrypt", dict()))
      val double = run(PdfAssembly.concatenate(Vector(source, source)))
      assertTrue(result(PdfAssembly.concatenate(Vector.empty)).isLeft, result(PdfAssembly.concatenate(Vector(source), 1)).isLeft,
        result(PdfAssembly.concatenate(Vector(source, form))).isLeft, result(PdfAssembly.concatenate(Vector(encrypted))).isLeft,
        result(PdfAssembly.overlay(double, source)).isLeft, result(PdfAssembly.overlay(source, document("BAD", 0, 1))).isLeft)
    }
  )
