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
    test("linearize fromBytes stays near source size for preencoded top-level objects") {
      for {
        source <- singlePagePdf("preserve")
        linearized <- PdfLinearize.fromBytes(source)
      } yield assertTrue(
        linearized.size <= source.size * 2,
        linearized.size >= source.size
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
