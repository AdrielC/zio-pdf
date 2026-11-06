package io.github.adrielc.kyopdf

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.{ISO_8859_1, UTF_16BE}
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import kyo.*
import zio.test.*

object PdfPacketSpec extends ZIOSpecDefault:
  import PdfValue.*
  private def run[A](value: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(value)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => throw error
  private def get[A](value: A < (Abort[PdfError] & Sync)): A = run(value).fold(e => throw AssertionError(e.message), identity)
  private def document(label: String, count: Int = 1, named: Boolean = false): PdfDocument =
    val pages = Vector.tabulate(count)(i => ObjectRef(3 + i * 2))
    val font = ObjectRef(3 + count * 2)
    val catalog = dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)),
      "Names" -> dict("Dests" -> dict("Names" -> PdfValue.Array(Vector(Literal("target".getBytes(ISO_8859_1).toVector),
        PdfValue.Array(Vector(Ref(pages.last), Name("Fit"))))))),
      "StructTreeRoot" -> Ref(ObjectRef(font.number + 1)))
    val objects = Vector(PdfObject(ObjectRef(1), catalog), PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"),
      "Count" -> Number(count), "Kids" -> PdfValue.Array(pages.map(Ref.apply)),
      "MediaBox" -> PdfValue.Array(Vector(Number(0), Number(0), Number(300), Number(200))),
      "Resources" -> dict("Font" -> dict("F1" -> Ref(font)))))) ++ pages.zipWithIndex.flatMap { (ref, i) =>
      val page = dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)), "Contents" -> Ref(ObjectRef(ref.number + 1)), "StructParents" -> Number(17))
      val link = dict("Type" -> Name("Annot"), "Subtype" -> Name("Link"),
        "Rect" -> PdfValue.Array(Vector(Number(20), Number(20), Number(100), Number(40))), "Dest" -> Name("target"))
      Vector(PdfObject(ref, if named && i == 0 then page.updated("Annots", PdfValue.Array(Vector(link))) else page),
        PdfObject(ObjectRef(ref.number + 1), dict(), Some(s"BT /F1 14 Tf 24 120 Td ($label page ${i + 1}) Tj ET\n".getBytes(ISO_8859_1).toVector)))
    } ++ Vector(PdfObject(font, dict("Type" -> Name("Font"), "Subtype" -> Name("Type1"), "BaseFont" -> Name("Helvetica"))),
      PdfObject(ObjectRef(font.number + 1), dict("Type" -> Name("StructTreeRoot"), "OldTag" -> Name("stale"))))
    PdfDocument("1.7", objects, dict("Root" -> Ref(ObjectRef(1)), "Prev" -> Number(123), "XRefStm" -> Number(456)))
  private def consume(session: PdfPacket.Session)(pull: session.Pull, out: ByteArrayOutputStream): Unit =
    var done = false
    while !done do get(pull.read()) match
      case None => done = true
      case Some(chunk) => out.write(chunk.array, 0, chunk.size)
  private def packet(parts: Vector[(String, PdfDocument)]): (scala.Array[Byte], PdfPacket.Receipt) =
    val session = get(PdfPacket.open(PdfPacket.Settings("Filing packet")))
    val out = new ByteArrayOutputStream()
    parts.foreach((title, doc) => consume(session)(get(session.append(title, doc)), out))
    consume(session)(get(session.finish()), out)
    val receipt = session.receipt.get
    session.close()
    out.toByteArray -> receipt
  private def decode(bytes: scala.Array[Byte]): PdfDocument = get(ByteLimit.fromBytes(bytes.length).map(limit => PdfDocument.decode(bytes, limit)))

  def spec = suite("Single-pass PDF packet stitching")(
    test("cover, contents, outlines, inherited resources, and named links target the rewritten pages") {
      val (bytes, receipt) = packet(Vector("Petition" -> document("PETITION", 2, named = true),
        "Declaration" -> document("DECLARATION"), "Exhibits" -> document("EXHIBIT", 2)))
      val output = decode(bytes)
      val pages = get(PdfPages.pageRefs(output))
      val cover = output.byRef(pages.head).dictionary.get
      val links = cover.get("Annots").toVector.collect { case PdfValue.Array(values) => values }.flatten.collect { case d: Dict => d }
      val targets = links.map(_.get("Dest").collect { case PdfValue.Array(Ref(page) +: _) => page }.get)
      val sourceLink = output.byRef(pages(1)).dictionary.get.get("Annots").collect { case PdfValue.Array(Vector(d: Dict)) => d }.get
      val wire = new String(bytes, ISO_8859_1)
      sys.env.get("KYO_PDF_PACKET_OUTPUT").foreach(path => Files.write(Path.of(path), bytes))
      assertTrue(pages.size == 6, receipt.coverPages == 1, receipt.entries.map(_.firstPage) == Vector(2, 4, 5),
        targets == Vector(pages(1), pages(3), pages(4)), sourceLink.get("Dest").contains(PdfValue.Array(Vector(Ref(pages(2)), Name("Fit")))),
        pages.tail.forall(ref => output.byRef(ref).dictionary.get.get("Resources").nonEmpty),
        !wire.contains("StructTreeRoot"), !wire.contains("StructParents"), output.trailer.get("Prev").isEmpty,
        receipt.bytes == bytes.length, receipt.sha256 == MessageDigest.getInstance("SHA-256").digest(bytes).map(b => f"${b & 255}%02x").mkString)
    },
    test("multi-page contents offsets include every generated front-matter page") {
      val (bytes, receipt) = packet(Vector.tabulate(37)(i => s"Document ${i + 1}" -> document(s"BODY ${i + 1}")))
      val output = decode(bytes)
      assertTrue(receipt.coverPages == 3, receipt.pages == 40, receipt.entries.head.firstPage == 4,
        receipt.entries.last.firstPage == 40, get(PdfPages.pageRefs(output)).length == 40)
    },
    test("bookmark text uses PDF document-string encoding independently of the cover font") {
      val title = "Résumé — déclaration"
      val (bytes, _) = packet(Vector(title -> document("BODY")))
      val output = decode(bytes)
      val titles = output.objects.flatMap(_.dictionary.toVector.flatMap(_.get("Title"))).collect {
        case Hex(bytes) if bytes.take(2) == Vector(0xfe.toByte, 0xff.toByte) => new String(bytes.drop(2).toArray, UTF_16BE)
      }
      assertTrue(titles == Vector(title))
    },
    test("a source catalog version override survives a streamed rewrite") {
      val source = document("VERSION").copy(objects = document("VERSION").objects.map { obj =>
        if obj.index == ObjectRef(1) then obj.copy(value = obj.dictionary.get.updated("Version", Name("2.0"))) else obj
      })
      val (bytes, _) = packet(Vector("Body" -> source))
      val output = decode(bytes)
      assertTrue(output.byRef(output.root.get).dictionary.get.get("Version").contains(Name("2.0")))
    },
    test("pulls enforce sequencing, output quotas, and immutable chunk ownership") {
      val session = get(PdfPacket.open(bounds = PdfPacket.Bounds(outputBytes = 80)))
      val pull = get(session.append("Body", document("BODY")))
      val exceeded = run(pull.read())
      val after = run(session.finish())
      val busy = get(PdfPacket.open())
      val first = get(busy.append("First", document("ONE")))
      val second = run(busy.append("Second", document("TWO")))
      val closed = run(first.read())
      assertTrue(exceeded.left.toOption.exists(_.isInstanceOf[PdfError.TooLarge]), after.isLeft,
        session.receipt.isEmpty, second.isLeft, closed.isLeft,
        PdfPacket.validBounds(PdfPacket.Bounds(outputBytes = PdfPacket.DefaultOutputBytes + 1)),
        !PdfPacket.validLabel("Emoji \ud83d\udcc4"))
    },
    test("page/document limits and unresolved links fail without a successful receipt") {
      val session = get(PdfPacket.open(bounds = PdfPacket.Bounds(documents = 1, pages = 2)))
      val out = new ByteArrayOutputStream()
      consume(session)(get(session.append("First", document("ONE"))), out)
      val failed = run(session.append("Second", document("TWO")))
      val malformed = document("BAD", named = true).copy(objects = document("BAD", named = true).objects.map { obj =>
        if obj.index == ObjectRef(1) then obj.copy(value = dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))) else obj
      })
      val other = get(PdfPacket.open())
      assertTrue(failed.isLeft, session.receipt.isEmpty, run(other.append("Bad link", malformed)).isLeft,
        run(other.finish()).isLeft)
    }
  )
