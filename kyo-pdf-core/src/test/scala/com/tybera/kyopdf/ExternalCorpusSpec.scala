package com.tybera.kyopdf

import java.io.File
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.nio.file.{Files, Path}
import kyo.*
import scala.jdk.CollectionConverters.*
import zio.test.*

/** Opt-in, non-mutating compatibility gate for private or local PDF corpora.
  *
  * Set `KYO_PDF_CORPUS_DIR` to a directory containing PDFs. Encrypted files
  * must be classified and rejected through the typed API. Every unencrypted
  * file must decode, expose pages, rewrite its first and last page, pass qpdf,
  * and decode again.
  */
object ExternalCorpusSpec extends ZIOSpecDefault:
  private val corpusRoot = sys.env.get("KYO_PDF_CORPUS_DIR").map(Path.of(_).toAbsolutePath.normalize)
  private val qpdf = sys.env.get("PATH").toVector.flatMap(_.split(File.pathSeparator)).map(directory => Path.of(directory, "qpdf"))
    .find(path => Files.isRegularFile(path) && Files.isExecutable(path))

  private val fixtures = corpusRoot.toVector.flatMap { root =>
    if !Files.isDirectory(root) then throw IllegalArgumentException(s"KYO_PDF_CORPUS_DIR is not a directory: $root")
    val paths = Files.walk(root)
    try paths.iterator.asScala.filter(Files.isRegularFile(_)).filter(_.getFileName.toString.toLowerCase.endsWith(".pdf")).toVector.sorted
    finally paths.close()
  }

  private def result[A](value: A < Abort[PdfError]): Either[PdfError, A] =
    Abort.run[PdfError](value).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => Left(PdfError.InvalidPdf(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  private def syncResult[A](value: A < (Abort[PdfError] & Sync)): Either[PdfError, A] =
    import kyo.AllowUnsafe.embrace.danger
    Abort.run[PdfError](Sync.Unsafe.run(value)).eval match
      case kyo.Result.Success(value) => Right(value)
      case kyo.Result.Failure(error) => Left(error)
      case kyo.Result.Panic(error) => Left(PdfError.InvalidPdf(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))

  private def qpdfCheck(bytes: Array[Byte], fixture: Path): Either[PdfError, Unit] = qpdf match
    case None => Left(PdfError.InvalidPdf("qpdf is required for the external corpus gate"))
    case Some(executable) =>
      val output = Files.write(Files.createTempFile("kyo-pdf-corpus-", ".pdf"), bytes)
      try
        val process = ProcessBuilder(executable.toString, "--check", output.toString).redirectErrorStream(true).start()
        val message = new String(process.getInputStream.readAllBytes(), ISO_8859_1)
        val exit = process.waitFor()
        Either.cond(exit == 0, (), PdfError.InvalidPdf(s"qpdf rejected ${fixture.getFileName}: $message"))
      finally
        val _ = Files.deleteIfExists(output)

  private def rewritePage(document: PdfDocument, page: Int, fixture: Path): Either[PdfError, Unit] =
    for
      selected <- result(PdfPages.select(document, page, page))
      written <- syncResult(PdfWriter.write(selected))
      _ <- qpdfCheck(written.toArray, fixture)
      outputLimit <- result(ByteLimit.fromBytes(math.max(1L, written.length.toLong)))
      decoded <- result(PdfDocument.decode(written.toArray, outputLimit))
      pages <- result(PdfPages.pageRefs(decoded))
      _ <- Either.cond(pages.length == 1, (), PdfError.InvalidPdf(s"${fixture.getFileName} page $page rewrite has ${pages.length} pages"))
    yield ()

  private def check(path: Path): Either[PdfError, Unit] =
    val bytes = Files.readAllBytes(path)
    val limit = result(ByteLimit.fromBytes(math.max(1L, bytes.length.toLong))).toOption.get
    result(PdfParser.scan(bytes, limit)).flatMap { report =>
      if report.encrypted then
        Either.cond(
          result(PdfDocument.decode(bytes, limit)).left.exists(_.message.toLowerCase.contains("encrypted")),
          (),
          PdfError.InvalidPdf(s"${path.getFileName} was not rejected as encrypted")
        )
      else
        for
          document <- result(PdfDocument.decode(bytes, limit))
          pages <- result(PdfPages.pageRefs(document))
          _ <- Either.cond(pages.nonEmpty, (), PdfError.InvalidPdf(s"${path.getFileName} has no pages"))
          _ <- Vector(1, pages.length).distinct.foldLeft[Either[PdfError, Unit]](Right(())) { (checked, page) =>
            checked.flatMap(_ => rewritePage(document, page, path))
          }
        yield ()
    }

  def spec =
    val tests = if corpusRoot.isEmpty then
      Vector(test("KYO_PDF_CORPUS_DIR is not configured")(assertCompletes) @@ TestAspect.ignore)
    else if fixtures.isEmpty then
      Vector(test("configured corpus contains PDFs")(assertTrue(false)))
    else fixtures.map { path =>
      val relative = corpusRoot.fold(path)(_.relativize(path))
      test(relative.toString) {
        val checked = check(path)
        assertTrue(checked.isRight)
      }
    }
    suite("external Kyo PDF corpus")(tests*)
