/*
 * PdfEngine façade — path decode / stream / policy / text / compare.
 */

package zio.pdf

import java.nio.file.Files

import _root_.scodec.bits.BitVector
import zio.pdf.io.PdfIO
import zio.*
import zio.stream.ZStream
import zio.test.*

object PdfEngineSpec extends ZIOSpecDefault {

  private val contentPayload: BitVector =
    BitVector("BT /F1 24 Tf 100 700 Td (hi) Tj ET\n".getBytes)

  private def minimalPdfBytes: ZIO[Any, Throwable, Array[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 612, 792),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.dict(), contentPayload)
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
      .map(_.toArray)
  }

  private def jsPdfBytes: ZIO[Any, Throwable, Array[Byte]] = {
    val catalog = IndirectObj.nostream(
      1,
      Prim.dict(
        "Type"       -> Prim.Name("Catalog"),
        "Pages"      -> Prim.Ref(2, 0),
        "OpenAction" -> Prim.dict(
          "S"  -> Prim.Name("JavaScript"),
          "JS" -> Prim.str("app.alert('x');")
        )
      )
    )
    val pages = IndirectObj.nostream(
      2,
      Prim.dict(
        "Type"  -> Prim.Name("Pages"),
        "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
        "Count" -> Prim.Number(BigDecimal(1))
      )
    )
    val page = IndirectObj.nostream(
      3,
      Prim.dict(
        "Type"     -> Prim.Name("Page"),
        "Parent"   -> Prim.Ref(2, 0),
        "MediaBox" -> Prim.Array.nums(0, 0, 1, 1),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.dict(), BitVector("BT (x) Tj ET\n".getBytes))
    val trailer =
      Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))
    ZStream(catalog, pages, page, content)
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((acc, bv) => acc ++ Chunk.fromArray(bv.toArray))
      .map(_.toArray)
  }

  private def withTempBytes(bytes: Array[Byte])(use: java.nio.file.Path => ZIO[Any, Throwable, TestResult]) =
    for {
      path   <- ZIO.attemptBlocking(Files.createTempFile("pdf-engine-", ".pdf"))
      _      <- ZIO.attemptBlocking(Files.write(path, bytes))
      result <- use(path).ensuring(ZIO.attemptBlocking(Files.deleteIfExists(path)).ignore)
    } yield result

  private def loadFixture(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing")
      val b = is.readAllBytes()
      is.close()
      b
    }

  def spec: Spec[Any, Throwable] = suite("PdfEngine")(
    test("decode path matches Hyperdrive sync on xref-stream.pdf") {
      loadFixture("xref-stream.pdf").flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            engine <- PdfEngine.decode(path).provide(PdfEngine.live)
            direct  = PdfHyperdrive.decodeSync(bytes)
          } yield assertTrue(engine == direct)
        }
      }
    },
    test("sinkZIO counts match decode size") {
      loadFixture("xref-stream.pdf").flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            n   <- PdfEngine.sinkZIO(path)(_ => ZIO.unit).provide(PdfEngine.live)
            all <- PdfEngine.decode(path).provide(PdfEngine.live)
          } yield assertTrue(n == all.size.toLong)
        }
      }
    },
    test("policy.strict flags OpenAction JavaScript") {
      jsPdfBytes.flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            result <- PdfEngine.policy(path, PdfPolicy.strict).provide(PdfEngine.live)
          } yield assertTrue(!result.isSuccess)
        }
      }
    },
    test("extractText pulls literal Tj string") {
      minimalPdfBytes.flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            pages <- PdfEngine.extractText(path).runCollect.provide(PdfEngine.live)
          } yield assertTrue(pages.exists(_.text.contains("hi")))
        }
      }
    },
    test("reader.via(decoded) matches fused path decode count") {
      loadFixture("xref-stream.pdf").flatMap { bytes =>
        withTempBytes(bytes) { path =>
          for {
            viaPipe <- PdfIO.reader(path).via(PdfEngine.decoded()).runCount.provide(PdfEngine.live)
            fused   <- PdfEngine.decode(path).map(_.size.toLong).provide(PdfEngine.live)
          } yield assertTrue(viaPipe == fused)
        }
      }
    },
    test("compare identical paths succeeds") {
      minimalPdfBytes.flatMap { bytes =>
        for {
          a <- ZIO.attemptBlocking(Files.createTempFile("pdf-engine-a-", ".pdf"))
          b <- ZIO.attemptBlocking(Files.createTempFile("pdf-engine-b-", ".pdf"))
          _ <- ZIO.attemptBlocking(Files.write(a, bytes))
          _ <- ZIO.attemptBlocking(Files.write(b, bytes))
          result <- PdfEngine
                      .compare(a, b)
                      .provide(PdfEngine.live)
                      .ensuring(ZIO.attemptBlocking {
                        Files.deleteIfExists(a)
                        Files.deleteIfExists(b)
                      }.ignore)
        } yield assertTrue(result.isSuccess)
      }
    }
  )
}
