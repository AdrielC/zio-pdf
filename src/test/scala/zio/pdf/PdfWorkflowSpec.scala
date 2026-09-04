package zio.pdf

import _root_.scodec.bits.BitVector
import zio.*
import zio.stream.ZStream
import zio.test.*

import java.nio.charset.StandardCharsets

object PdfWorkflowSpec extends ZIOSpecDefault {

  private def singlePageParts(label: String): Chunk[Part[Trailer]] = {
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0)))
    Chunk(
      Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0)))),
      Part.Obj(
        IndirectObj.nostream(
          2L,
          Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
        )
      ),
      Part.Obj(
        IndirectObj.nostream(
          3L,
          Prim.dict(
            "Type"     -> Prim.Name("Page"),
            "Parent"   -> Prim.Ref(2L, 0),
            "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
            "Contents" -> Prim.Ref(4L, 0)
          )
        )
      ),
      Part.Obj(
        IndirectObj.stream(4L, Prim.dict(), BitVector(s"BT 72 720 Td ($label) Tj ET\n".getBytes))
      ),
      Part.Meta(trailer)
    )
  }

  private def singlePagePdf(label: String): ZIO[Any, Throwable, Chunk[Byte]] =
    PdfEngine.writeBytes(singlePageParts(label))

  def spec: Spec[Any, Throwable] = suite("PdfWorkflow")(
    test("merge combines pages from two filings") {
      for {
        left  <- singlePagePdf("A")
        right <- singlePagePdf("B")
        leftDecoded  <- ZStream.fromChunk(left).via(PdfStream.decode()).runCollect
        rightDecoded <- ZStream.fromChunk(right).via(PdfStream.decode()).runCollect
        merged <- PdfMerge.bytes(NonEmptyChunk(leftDecoded, rightDecoded))
        pages  <- ZStream.fromChunk(merged).via(PdfStream.decode()).runCollect
        pageCount = TextExtract.orderedPageObjectNumbers(pages).size
      } yield assertTrue(pageCount == 2)
    },
    test("append preserves the original prefix and adds /Prev") {
      for {
        base <- singlePagePdf("sign-me")
        revision = Chunk(
          Part.Obj(
            IndirectObj.nostream(
              99L,
              Prim.dict("Producer" -> Prim.Name("zio-pdf-append"))
            )
          ),
          Part.Meta(Trailer(BigDecimal(100), Prim.dict("Info" -> Prim.Ref(99L, 0)), None))
        )
        updated <- PdfAppend.append(base, revision)
        text = new String(updated.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        updated.size > base.size,
        updated.startsWith(base),
        text.contains("/Prev"),
        text.indexOf("startxref") != text.lastIndexOf("startxref")
      )
    },
    test("linearize emits /Linearized, optional /H hint tables, and measured /L") {
      val trailerData = Prim.dict("Root" -> Prim.Ref(1L, 0))
      for {
        bytes <- PdfLinearize.bytes(trailerData, singlePageParts("web"))
        text = new String(bytes.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("/Linearized"),
        text.contains("/L")
      )
    },
    test("linearize fromBytes on court corpus stays near source size with /Linearized") {
      val path = java.nio.file.Path.of("src/test/resources/court-corpus/cafc-janich-v-collins.pdf")
      for {
        source     <- zio.pdf.io.PdfIO.readAll(path)
        linearized <- PdfLinearize.fromBytes(source)
        endOffset  <- ZIO.fromEither(PdfLinearize.firstPageByteLength(linearized).left.map(new RuntimeException(_)))
        text        = new String(linearized.toArray.take(4096), StandardCharsets.ISO_8859_1)
        validation <- PdfEngine.validate(ZStream.fromChunk(linearized)).provide(PdfEngine.live)
      } yield assertTrue(
        linearized.size <= source.size * 11L / 10L,
        linearized.size >= source.size,
        endOffset > 0L,
        endOffset < linearized.size,
        text.contains("/Linearized"),
        validation.isSuccess
      )
    },
    test("withThumbnails adds inspectable /Thumb references") {
      val parts = singlePageParts("thumb")
      for {
        enriched <- PdfEngine.withThumbnails(parts, thumbStartNumber = 10L, PdfThumbnail.Options(scope = PdfThumbnail.Scope.AllPages))
        bytes    <- PdfEngine.writeBytes(enriched)
        outcome  <- PdfEngine.inspect(bytes, PdfInspection.thumbnail)
      } yield assertTrue(outcome match {
        case PdfInspection.Outcome.Accepted(report) => report.thumbnail.nonEmpty
        case PdfInspection.Outcome.Rejected(report, _) => report.thumbnail.nonEmpty
      })
    },
    test("withThumbnails FirstPageOnly skips thumbs on multi-page re-encode") {
      for {
        left  <- singlePagePdf("one")
        right <- singlePagePdf("two")
        leftDecoded  <- ZStream.fromChunk(left).via(PdfStream.decode()).runCollect
        rightDecoded <- ZStream.fromChunk(right).via(PdfStream.decode()).runCollect
        merged <- PdfMerge.bytes(NonEmptyChunk(leftDecoded, rightDecoded))
        parts  <- ZStream.fromChunk(merged).via(PdfStream.decode()).via(Decoded.parts).runCollect
        maxObj  = parts.collect { case Part.Obj(obj) => obj.obj.index.number }.maxOption.getOrElse(0L)
        enriched <- PdfEngine.withThumbnails(
                      parts,
                      maxObj + 1L,
                      PdfThumbnail.Options(scope = PdfThumbnail.Scope.FirstPageOnly, largeDocPageThreshold = 50)
                    )
        thumbParts = enriched.collect { case Part.Obj(obj) => obj.obj.index.number }
      } yield assertTrue(thumbParts.count(_ >= maxObj + 1L) == 1)
    },
    test("rendered enrichBytes produces a larger incremental /Thumb preview") {
      for {
        source  <- singlePagePdf("rendered")
        updated <- PdfThumbnail.enrichBytes(
                     source,
                     PdfThumbnail.Options(
                       scope = PdfThumbnail.Scope.FirstPageOnly,
                       pixelSource = Some(PdfRenderer.pixelSource(source.toArray)),
                       width = 32,
                       height = 32
                     )
                   )
        outcome <- PdfEngine.inspect(updated, PdfInspection.thumbnail)
      } yield assertTrue(
        updated.size > source.size + 100,
        outcome match {
          case PdfInspection.Outcome.Accepted(report) => report.thumbnail.nonEmpty
          case PdfInspection.Outcome.Rejected(report, _) => report.thumbnail.nonEmpty
        }
      )
    },
    test("enrichBytes appends an inspectable first-page /Thumb incrementally") {
      for {
        source  <- singlePagePdf("incremental-thumb")
        updated <- PdfThumbnail.enrichBytes(source, PdfThumbnail.Options(scope = PdfThumbnail.Scope.FirstPageOnly))
        outcome <- PdfEngine.inspect(updated, PdfInspection.thumbnail)
      } yield assertTrue(
        updated.size > source.size,
        updated.startsWith(source),
        outcome match {
          case PdfInspection.Outcome.Accepted(report) => report.thumbnail.nonEmpty
          case PdfInspection.Outcome.Rejected(report, _) => report.thumbnail.nonEmpty
        }
      )
    },
    test("extractPages keeps only the requested 1-based range") {
      for {
        left    <- singlePagePdf("A")
        right   <- singlePagePdf("B")
        merged  <- PdfEngine.mergeBytes(NonEmptyChunk(left, right))
        second  <- PdfEngine.extractPages(merged, 2, 2)
        first   <- PdfEngine.extractPages(merged, 1, 1)
        decoded <- ZStream.fromChunk(second).via(PdfStream.decode()).runCollect
        text    <- PdfEngine.extractText(ZStream.fromChunk(second)).runCollect
        firstText <- PdfEngine.extractText(ZStream.fromChunk(first)).runCollect
      } yield assertTrue(
        TextExtract.orderedPageObjectNumbers(decoded).size == 1,
        text.exists(_.text.contains("B")),
        firstText.exists(_.text.contains("A"))
      )
    },
    test("splitPages emits one PDF per page") {
      for {
        left   <- singlePagePdf("A")
        right  <- singlePagePdf("B")
        merged <- PdfEngine.mergeBytes(NonEmptyChunk(left, right))
        parts  <- PdfEngine.splitPages(merged)
        counts <- ZIO.foreach(parts) { pdf =>
                    ZStream.fromChunk(pdf).via(PdfStream.decode()).runCollect.map(TextExtract.orderedPageObjectNumbers(_).size)
                  }
      } yield assertTrue(parts.size == 2, counts.forall(_ == 1))
    },
    test("rotatePages writes /Rotate on the selected range") {
      for {
        left    <- singlePagePdf("A")
        right   <- singlePagePdf("B")
        merged  <- PdfEngine.mergeBytes(NonEmptyChunk(left, right))
        rotated <- PdfEngine.rotatePages(merged, 90, 2, 2)
        decoded <- PdfEngine.decode(rotated)
        pages    = TextExtract.orderedPageObjectNumbers(decoded)
        rotations = pages.map { number =>
                      decoded.collectFirst {
                        case Decoded.DataObj(obj) if obj.index.number == number =>
                          obj.data match {
                            case dict: Prim.Dict =>
                              dict.data.get("Rotate").collect { case Prim.Number(value) => value.toInt }.getOrElse(0)
                            case _ => 0
                          }
                      }.getOrElse(0)
                    }
      } yield assertTrue(rotations == List(0, 90))
    },
    test("extractPages rejects an empty or inverted range") {
      for {
        source <- singlePagePdf("only")
        empty  <- PdfEngine.extractPages(source, 2, 2).either
        invert <- PdfEngine.extractPages(source, 2, 1).either
      } yield assertTrue(empty.isLeft, invert.isLeft)
    },
    test("mergeBytes combines two caller-owned PDFs") {
      for {
        left   <- singlePagePdf("A")
        right  <- singlePagePdf("B")
        merged <- PdfEngine.mergeBytes(NonEmptyChunk(left, right))
        pages  <- ZStream.fromChunk(merged).via(PdfStream.decode()).runCollect
      } yield assertTrue(TextExtract.orderedPageObjectNumbers(pages).size == 2)
    },
    test("trailerFromTail reads the last conventional trailer without a full decode") {
      for {
        base <- singlePagePdf("tail")
        trailer <- ZIO.fromEither(PdfAppend.trailerFromTail(base))
      } yield assertTrue(trailer.size == BigDecimal(5), trailer.root.contains(Prim.Ref(1L, 0)))
    },
    test("linearize and merge reject encrypted PDFs") {
      val encryptedParts = singlePageParts("secret").map {
        case Part.Meta(trailer) =>
          Part.Meta(
            Trailer(
              trailer.size,
              Prim.Dict(trailer.data.data.updated("Encrypt", Prim.Ref(5L, 0))),
              trailer.root
            )
          )
        case other => other
      }
      for {
        encrypted <- PdfEngine.writeBytes(encryptedParts)
        plain     <- singlePagePdf("open")
        linearized <- PdfEngine.linearize(encrypted).either
        merged     <- PdfEngine.mergeBytes(NonEmptyChunk(encrypted, plain)).either
        appended   <- PdfEngine.appendRevision(
                        encrypted,
                        Chunk(Part.Meta(Trailer(BigDecimal(6), Prim.dict(), None)))
                      ).either
      } yield assertTrue(
        linearized.isLeft,
        merged.isLeft,
        appended.isLeft
      )
    },
    test("linearize fromBytes fails before decode when the document exceeds ByteLimit") {
      for {
        source <- singlePagePdf("bound")
        limit  <- ZIO.fromEither(ByteLimit.fromBytes(32L))
        result <- PdfLinearize.fromBytes(source, PdfEngine.Options(maxMaterializedDocumentBytes = limit)).either
      } yield result match {
        case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
          assertTrue(observed == source.size.toLong, source.size.toLong > 32L)
        case _ =>
          assertTrue(false)
      }
    },
    test("flatten removes catalog /AcroForm and widget annotations") {
      val formParts = Chunk(
        Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0), "AcroForm" -> Prim.Ref(5L, 0)))),
        Part.Obj(
          IndirectObj.nostream(
            2L,
            Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            3L,
            Prim.dict(
              "Type"     -> Prim.Name("Page"),
              "Parent"   -> Prim.Ref(2L, 0),
              "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
              "Annots"   -> Prim.Array(Prim.Ref(6L, 0))
            )
          )
        ),
        Part.Obj(IndirectObj.nostream(4L, Prim.dict())),
        Part.Obj(
          IndirectObj.nostream(
            5L,
            Prim.dict("Fields" -> Prim.Array(Prim.Ref(6L, 0)), "NeedAppearances" -> Prim.Bool(true))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            6L,
            Prim.dict("Subtype" -> Prim.Name("Widget"), "T" -> Prim.str("Name"), "FT" -> Prim.Name("Tx"), "V" -> Prim.str("Ada"))
          )
        ),
        Part.Meta(Trailer(BigDecimal(7), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0))))
      )
      for {
        source    <- PdfEngine.writeBytes(formParts)
        decoded   <- PdfEngine.decode(source)
        inventory  = PdfAcroForm.extract(decoded)
        flattened <- PdfEngine.flattenForms(source)
        after     <- PdfEngine.decode(flattened)
        outcome   <- PdfEngine.inspect(flattened, PdfInspection.acroForm)
      } yield assertTrue(
        inventory.fields.nonEmpty,
        inventory.fields.exists(_.name.contains("Name")),
        PdfAcroForm.extract(after).catalogObjectNumber.isEmpty,
        outcome match {
          case PdfInspection.Outcome.Accepted(report) => report.acroForm.forall(_.fieldCount == 0)
          case PdfInspection.Outcome.Rejected(report, _) => report.acroForm.forall(_.fieldCount == 0)
        }
      )
    },
    test("flatten bakes widget /AP Form XObjects onto the page") {
      val formParts = Chunk(
        Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0), "AcroForm" -> Prim.Ref(5L, 0)))),
        Part.Obj(
          IndirectObj.nostream(
            2L,
            Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            3L,
            Prim.dict(
              "Type"     -> Prim.Name("Page"),
              "Parent"   -> Prim.Ref(2L, 0),
              "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
              "Annots"   -> Prim.Array(Prim.Ref(6L, 0))
            )
          )
        ),
        Part.Obj(IndirectObj.nostream(4L, Prim.dict())),
        Part.Obj(IndirectObj.nostream(5L, Prim.dict("Fields" -> Prim.Array(Prim.Ref(6L, 0))))),
        Part.Obj(
          IndirectObj.nostream(
            6L,
            Prim.dict(
              "Subtype" -> Prim.Name("Widget"),
              "T"       -> Prim.str("Name"),
              "FT"      -> Prim.Name("Tx"),
              "V"       -> Prim.str("Ada"),
              "Rect"    -> Prim.Array.nums(72, 700, 272, 720),
              "AP"      -> Prim.dict("N" -> Prim.Ref(7L, 0))
            )
          )
        ),
        Part.Obj(
          IndirectObj.stream(
            7L,
            Prim.dict(
              "Type"    -> Prim.Name("XObject"),
              "Subtype" -> Prim.Name("Form"),
              "BBox"    -> Prim.Array.nums(0, 0, 200, 20)
            ),
            BitVector("BT /F1 12 Tf 2 2 Td (Ada) Tj ET\n".getBytes(StandardCharsets.ISO_8859_1))
          )
        ),
        Part.Meta(Trailer(BigDecimal(8), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0))))
      )
      for {
        source    <- PdfEngine.writeBytes(formParts)
        flattened <- PdfEngine.flattenForms(source)
        after     <- PdfEngine.decode(flattened)
        text       = new String(flattened.toArray, StandardCharsets.ISO_8859_1)
        page       = after.collectFirst {
                       case Decoded.DataObj(obj) if Prim.tryDict("Type")(obj.data).contains(Prim.Name("Page")) =>
                         obj.data
                     }
      } yield assertTrue(
        text.contains("/Ff1"),
        text.contains(" Do"),
        text.contains("/XObject"),
        PdfAcroForm.extract(after).catalogObjectNumber.isEmpty,
        page.exists {
          case dict: Prim.Dict =>
            dict.data.get("Annots").isEmpty &&
            dict.data.get("Resources").exists {
              case resources: Prim.Dict =>
                resources.data.get("XObject").exists {
                  case xobjects: Prim.Dict => xobjects.data.get("Ff1").contains(Prim.Ref(7L, 0))
                  case _                   => false
                }
              case _ => false
            }
          case _ => false
        }
      )
    },
    test("flatten falls back to /V text when a widget has no /AP") {
      val formParts = Chunk(
        Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0), "AcroForm" -> Prim.Ref(5L, 0)))),
        Part.Obj(
          IndirectObj.nostream(
            2L,
            Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            3L,
            Prim.dict(
              "Type"     -> Prim.Name("Page"),
              "Parent"   -> Prim.Ref(2L, 0),
              "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
              "Annots"   -> Prim.Array(Prim.Ref(6L, 0))
            )
          )
        ),
        Part.Obj(IndirectObj.nostream(4L, Prim.dict())),
        Part.Obj(IndirectObj.nostream(5L, Prim.dict("Fields" -> Prim.Array(Prim.Ref(6L, 0))))),
        Part.Obj(
          IndirectObj.nostream(
            6L,
            Prim.dict(
              "Subtype" -> Prim.Name("Widget"),
              "T"       -> Prim.str("Name"),
              "FT"      -> Prim.Name("Tx"),
              "V"       -> Prim.str("Ada"),
              "Rect"    -> Prim.Array.nums(72, 700, 172, 720)
            )
          )
        ),
        Part.Meta(Trailer(BigDecimal(7), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0))))
      )
      for {
        source    <- PdfEngine.writeBytes(formParts)
        flattened <- PdfEngine.flattenForms(source)
        text       = new String(flattened.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        text.contains("(Ada)"),
        text.contains("/Helv"),
        !text.contains("/AcroForm"),
        !text.contains("/Widget")
      )
    },
    test("extract walks nested AcroForm /Kids with qualified names") {
      val formParts = Chunk(
        Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0), "AcroForm" -> Prim.Ref(5L, 0)))),
        Part.Obj(
          IndirectObj.nostream(
            2L,
            Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            3L,
            Prim.dict(
              "Type"     -> Prim.Name("Page"),
              "Parent"   -> Prim.Ref(2L, 0),
              "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
              "Annots"   -> Prim.Array(Prim.Ref(6L, 0), Prim.Ref(9L, 0))
            )
          )
        ),
        Part.Obj(IndirectObj.nostream(4L, Prim.dict())),
        Part.Obj(IndirectObj.nostream(5L, Prim.dict("Fields" -> Prim.Array(Prim.Ref(8L, 0))))),
        Part.Obj(
          IndirectObj.nostream(
            6L,
            Prim.dict(
              "Subtype" -> Prim.Name("Widget"),
              "T"       -> Prim.str("Street"),
              "FT"      -> Prim.Name("Tx"),
              "V"       -> Prim.str("Main"),
              "Rect"    -> Prim.Array.nums(72, 700, 272, 720),
              "Parent"  -> Prim.Ref(8L, 0)
            )
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            8L,
            Prim.dict("T" -> Prim.str("Address"), "Kids" -> Prim.Array(Prim.Ref(6L, 0), Prim.Ref(9L, 0)))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            9L,
            Prim.dict(
              "Subtype" -> Prim.Name("Widget"),
              "T"       -> Prim.str("City"),
              "FT"      -> Prim.Name("Tx"),
              "V"       -> Prim.str("Rome"),
              "Rect"    -> Prim.Array.nums(72, 670, 272, 690),
              "Parent"  -> Prim.Ref(8L, 0)
            )
          )
        ),
        Part.Meta(Trailer(BigDecimal(10), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0))))
      )
      for {
        source    <- PdfEngine.writeBytes(formParts)
        decoded   <- PdfEngine.decode(source)
        inventory  = PdfAcroForm.extract(decoded)
        flattened <- PdfEngine.flattenForms(source)
        after     <- PdfEngine.decode(flattened)
        text       = new String(flattened.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        inventory.fields.exists(_.name.contains("Address.Street")),
        inventory.fields.exists(_.name.contains("Address.City")),
        inventory.fieldObjectNumbers.contains(8L),
        PdfAcroForm.extract(after).fields.isEmpty,
        text.contains("(Main)") || text.contains("/Helv"),
        !text.contains("/AcroForm")
      )
    },
    test("flatten honors Form XObject /Matrix when mapping /BBox to /Rect") {
      val formParts = Chunk(
        Part.Obj(IndirectObj.nostream(1L, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2L, 0), "AcroForm" -> Prim.Ref(5L, 0)))),
        Part.Obj(
          IndirectObj.nostream(
            2L,
            Prim.dict("Type" -> Prim.Name("Pages"), "Kids" -> Prim.Array(Prim.Ref(3L, 0)), "Count" -> Prim.Number(1))
          )
        ),
        Part.Obj(
          IndirectObj.nostream(
            3L,
            Prim.dict(
              "Type"     -> Prim.Name("Page"),
              "Parent"   -> Prim.Ref(2L, 0),
              "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
              "Annots"   -> Prim.Array(Prim.Ref(6L, 0))
            )
          )
        ),
        Part.Obj(IndirectObj.nostream(4L, Prim.dict())),
        Part.Obj(IndirectObj.nostream(5L, Prim.dict("Fields" -> Prim.Array(Prim.Ref(6L, 0))))),
        Part.Obj(
          IndirectObj.nostream(
            6L,
            Prim.dict(
              "Subtype" -> Prim.Name("Widget"),
              "T"       -> Prim.str("Name"),
              "FT"      -> Prim.Name("Tx"),
              "Rect"    -> Prim.Array.nums(72, 700, 272, 720),
              "AP"      -> Prim.dict("N" -> Prim.Ref(7L, 0))
            )
          )
        ),
        Part.Obj(
          IndirectObj.stream(
            7L,
            Prim.dict(
              "Type"    -> Prim.Name("XObject"),
              "Subtype" -> Prim.Name("Form"),
              "BBox"    -> Prim.Array.nums(0, 0, 100, 10),
              "Matrix"  -> Prim.Array.nums(2, 0, 0, 2, 0, 0)
            ),
            BitVector("BT /F1 12 Tf 0 0 Td (Ada) Tj ET\n".getBytes(StandardCharsets.ISO_8859_1))
          )
        ),
        Part.Meta(Trailer(BigDecimal(8), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0))))
      )
      for {
        source    <- PdfEngine.writeBytes(formParts)
        decoded   <- PdfEngine.decode(source)
        reported  <- PdfAcroForm.flattenReported(decoded)
        (flattened, report) = reported
        text       = new String(flattened.toArray, StandardCharsets.ISO_8859_1)
      } yield assertTrue(
        report.appearancesPlaced == 1,
        text.contains("/Matrix"),
        text.contains("1 0 0 1 72 700 cm"),
        text.contains("/Ff1"),
        text.contains(" Do")
      )
    },
    test("append fails before rewrite when the base exceeds ByteLimit") {
      for {
        source <- singlePagePdf("append-bound")
        limit  <- ZIO.fromEither(ByteLimit.fromBytes(32L))
        result <- PdfAppend
                    .append(
                      source,
                      Chunk(Part.Meta(Trailer(BigDecimal(6), Prim.dict(), None))),
                      opts = PdfEngine.Options(maxMaterializedDocumentBytes = limit)
                    )
                    .either
      } yield result match {
        case Left(PdfEngine.MaterializedDocumentLimitExceeded(`limit`, observed)) =>
          assertTrue(observed == source.size.toLong)
        case _ =>
          assertTrue(false)
      }
    },
    test("evidence records encrypted filings as cannot-process blockers") {
      val parts = singlePageParts("secret").map {
        case Part.Meta(trailer) =>
          Part.Meta(
            Trailer(
              trailer.size,
              Prim.Dict(trailer.data.data.updated("Encrypt", Prim.Ref(5L, 0))),
              trailer.root
            )
          )
        case other => other
      }
      for {
        encrypted <- PdfEngine.writeBytes(parts)
        bundle    <- PdfEngine.evidence(encrypted).provide(PdfEngine.live)
      } yield assertTrue(
        bundle.cannotProcess,
        bundle.processingBlockers == Chunk(PdfEvidence.ProcessingBlocker.Encrypted(Some(Prim.Ref(5L, 0)))),
        bundle.canonicalJson.contains("\"cannotProcess\":true"),
        bundle.canonicalJson.contains("\"kind\":\"Encrypted\""),
        bundle.inspection match {
          case PdfInspection.Outcome.Accepted(report) => report.encryption.nonEmpty
          case PdfInspection.Outcome.Rejected(report, _) => report.encryption.nonEmpty
        }
      )
    },
    test("govinfo court order exposes an AcroForm inventory") {
      val path = java.nio.file.Path.of("src/test/resources/court-corpus/govinfo-district-court-order.pdf")
      for {
        source    <- zio.pdf.io.PdfIO.readAll(path)
        decoded   <- PdfEngine.decode(source)
        inventory  = PdfAcroForm.extract(decoded)
        outcome   <- PdfEngine.inspect(source, PdfInspection.acroForm)
      } yield assertTrue(
        inventory.fields.nonEmpty || inventory.catalogObjectNumber.nonEmpty || inventory.formObjectNumber.nonEmpty,
        outcome match {
          case PdfInspection.Outcome.Accepted(report) => report.acroForm.nonEmpty
          case PdfInspection.Outcome.Rejected(report, _) => report.acroForm.nonEmpty
        }
      )
    },
    test("Preencoded graft preserves donor object bytes") {
      for {
        donor <- singlePagePdf("graft")
        grafted <- PdfGraft.graft(donor, Set(4L))
        trailer = Trailer(BigDecimal(6), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0)))
        rebuilt <- PdfEngine.writeBytes(
                     singlePageParts("graft").filter {
                       case Part.Obj(obj) if obj.obj.index.number == 4L => false
                       case _                                           => true
                     } ++ grafted :+ Part.Meta(trailer)
                   )
        donorContent   <- PdfGraft.graft(donor, Set(4L)).map(_.find(_.index.number == 4L).map(_.bytes))
        rebuiltContent <- PdfGraft.graft(rebuilt, Set(4L)).map(_.find(_.index.number == 4L).map(_.bytes))
      } yield assertTrue(donorContent == rebuiltContent)
    }
  ).provide(PdfEngine.live)
}
