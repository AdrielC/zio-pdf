import io.github.adrielc.kyopdf.*
import io.github.adrielc.scan.Scan
import java.nio.charset.StandardCharsets.UTF_8
import kyo.*

object Consumer:
  def main(args: Array[String]): Unit =
    val scans: Scan.Aux[(Long, Long), Int, Either[Long, Long]] =
      Scan.count[Int].broadcast(Scan.foldLong[Int](0L)((sum, value) => sum + value))
    val compiled = scans.compile().toOption.get
    var observed = (0L, 0L)
    val run = compiled.open {
      case Left(count) => observed = (count, observed._2)
      case Right(sum) => observed = (observed._1, sum)
    }
    assert(run.feed(emit => List(1, 2, 3).foreach(emit(_))) == Right(()))
    val state: Either[Scan.RunIssue, scans.State] = run.finish()
    assert(state == Right((3L, 6L)) && observed == (3L, 6L))
    assert(compiled.layout.longSlots == 2 && compiled.layout.referenceSlots == 0)
    println("Native consumer composed generic scans with typed register state.")

    import kyo.AllowUnsafe.embrace.danger
    import PdfValue.*
    val xmp = """<r:RDF xmlns:r="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
      <r:Description xmlns:a="http://www.aiim.org/pdfa/ns/id/" a:part="3" a:conformance="B"/>
      </r:RDF>""".getBytes(UTF_8).toVector
    val source = PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)), "Metadata" -> Ref(ObjectRef(4)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Kids" -> Array(Vector(Ref(ObjectRef(3)))), "Count" -> Number(1))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)),
        "MediaBox" -> Array(Vector(Number(0), Number(0), Number(72), Number(72))))),
      PdfObject(ObjectRef(4), dict("Type" -> Name("Metadata"), "Subtype" -> Name("XML"),
        "Length" -> Number(xmp.length)), Some(xmp))
    ), dict("Root" -> Ref(ObjectRef(1)), "Size" -> Number(5)))
    val checked = for
      bytes <- PdfWriter.write(source)
      limit <- ByteLimit.fromBytes(bytes.length.toLong)
      document <- PdfDocument.decode(bytes.toArray, limit)
      pages <- PdfPages.pageRefs(document)
      identification <- PdfAIdentification.inspect(document)
    yield pages.size == 1 && identification.declares3b
    Abort.run[PdfError](Sync.Unsafe.run(checked)).eval match
      case Result.Success(true) => println("Native consumer wrote, decoded, and identified a synthetic PDF.")
      case other => sys.error(s"Unexpected Native PDF result: $other")
