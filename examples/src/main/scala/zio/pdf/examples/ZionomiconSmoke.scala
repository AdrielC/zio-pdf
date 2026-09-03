package zio.pdf.examples

import java.nio.file.{Files, Path}

import zio.*
import zio.pdf.*
import zio.pdf.io.PdfIO
import zio.stream.ZStream

/** Smoke-test workflow APIs on a caller-supplied PDF (defaults to PDF_PATH env). */
object ZionomiconSmoke extends ZIOAppDefault:

  private val defaultPath =
    "/Users/adrielcasellas/Downloads/Zionomicon - Digital Book - Edition 8.28.2025 (1).pdf"

  def run: ZIO[Any, Throwable, Unit] =
    for {
      path <- ZIO.attempt(Path.of(sys.env.getOrElse("PDF_PATH", defaultPath)))
      _    <- Console.printLine(s"Testing: $path (${Files.size(path)} bytes)")
      decoded <- PdfEngine.decode(path).provide(PdfEngine.live)
      elements <- PdfEngine.elements(path).provide(PdfEngine.live)
      pageText <- PdfEngine.extractText(path).runCollect.provide(PdfEngine.live)
      inspection <- PdfEngine.inspect(path, PdfInspection.documentProfile).provide(PdfEngine.live)
      validation <- PdfEngine.validate(path).provide(PdfEngine.live)
      digest     <- PdfEngine.digest(path).provide(PdfEngine.live)
      _ <- Console.printLine(s"decode:    ${decoded.size} events, ${countObjs(decoded)} objects")
      _ <- Console.printLine(s"elements:  ${elements.size} classified")
      _ <- Console.printLine(s"pages:     ${pageText.size} (${pageText.count(_.text.nonEmpty)} with text)")
      _ <- Console.printLine(s"text sample: ${pageText.find(_.text.nonEmpty).map(_.text.take(120)).getOrElse("<none>")}")
      _ <- Console.printLine(s"inspect:   $inspection")
      _ <- Console.printLine(s"validate:  $validation")
      _ <- Console.printLine(s"digest:    ${digest.size} bytes SHA-256")
      outDir = Files.createTempDirectory("zio-pdf-zionomicon-")
      linearized = outDir.resolve("linearized.pdf")
      appended   = outDir.resolve("appended.pdf")
      thumbnailed = outDir.resolve("thumbnailed.pdf")
      sourceBytes <- PdfIO.reader(path).runCollect
      _ <- smokeLinearize(sourceBytes, linearized)
      _ <- smokeAppend(path, appended)
      _ <- smokeThumbnails(sourceBytes, thumbnailed)
      _ <- Console.printLine("--- inspect outputs ---")
      _ <- inspectFile("original", path)
      _ <- inspectFile("linearized", linearized)
      _ <- inspectFile("appended", appended)
      _ <- inspectFile("thumbnailed", thumbnailed)
      _ <- Console.printLine(s"outputs:   $outDir")
    } yield ()

  private def countObjs(chunk: Chunk[Decoded]): Int =
    chunk.count { case Decoded.DataObj(_) | Decoded.ContentObj(_, _, _) => true; case _ => false }

  private def smokeLinearize(source: Chunk[Byte], output: Path): ZIO[Any, Throwable, Unit] =
    for {
      bytes <- PdfLinearize.fromBytes(source)
      ratio  = bytes.size.toDouble / source.size.toDouble
      firstPage <- ZIO.fromEither(PdfLinearize.firstPageByteLength(bytes).left.map(new RuntimeException(_)))
      _     <- ZStream.fromChunk(bytes).run(PdfIO.writer(output))
      text  = new String(bytes.toArray.take(8192), java.nio.charset.StandardCharsets.ISO_8859_1)
      _    <- Console.printLine(
                s"linearize: ${bytes.size} bytes (${f"$ratio%.2f"}x source, first-page prefix ~$firstPage bytes) -> $output"
              )
      _ <- Console.printLine(
             s"           has /Linearized=${text.contains("/Linearized")}, /H=${text.contains("/H")}, hint cap respected=${bytes.size < source.size * 2}"
           )
    } yield ()

  private def smokeThumbnails(source: Chunk[Byte], output: Path): ZIO[Any, Throwable, Unit] =
    for {
      rendered <- ZIO.succeed(sys.env.get("RENDER_THUMBS").exists(_.equalsIgnoreCase("true")))
      options =
        if rendered then
          PdfThumbnail.renderedOptions(
            PdfBoxRenderer.pixelSource(source.toArray),
            width = 64,
            height = 64
          )
        else
          PdfThumbnail.placeholderOptions(width = 64, height = 64)
      bytes <- PdfEngine.withThumbnailsBytes(source, options)
      _     <- ZStream.fromChunk(bytes).run(PdfIO.writer(output))
      mode   = if rendered then "PDFBox-rendered" else "placeholder"
      _     <- Console.printLine(s"thumbnail: ${bytes.size} bytes ($mode first-page /Thumb) -> $output")
    } yield ()

  private def inspectFile(label: String, path: Path): ZIO[Any, Throwable, Unit] =
    for {
      size <- ZIO.attempt(Files.size(path))
      profile <- PdfEngine.inspect(path, PdfInspection.documentProfile).provide(PdfEngine.live)
      thumbs  <- PdfEngine.inspect(path, PdfInspection.thumbnail).provide(PdfEngine.live)
      linear  <- PdfEngine.inspect(path, PdfInspection.linearized).provide(PdfEngine.live)
      valid   <- PdfEngine.validate(path).provide(PdfEngine.live)
      pages   <- PdfEngine.extractText(path).runCollect.provide(PdfEngine.live)
      _ <- Console.printLine(
             s"[$label] ${size} bytes | pages=${pages.size} | validate=$valid | linearized=$linear | thumbnail=$thumbs | profile=$profile"
           )
    } yield ()

  private def smokeAppend(source: Path, output: Path): ZIO[Any, Throwable, Unit] =
    for {
      base <- PdfIO.reader(source).runCollect
      revision = Chunk(
        Part.Obj(IndirectObj.nostream(999_999L, Prim.dict("Producer" -> Prim.Name("zio-pdf-smoke")))),
        Part.Meta(Trailer(BigDecimal(1_000_000), Prim.dict("Info" -> Prim.Ref(999_999L, 0)), None))
      )
      updated <- PdfAppend.append(base, revision)
      _       <- ZStream.fromChunk(updated).run(PdfIO.writer(output))
      _       <- Console.printLine(s"append:    ${updated.size} bytes (+${updated.size - base.size}) -> $output")
    } yield ()
