package io.github.adrielc.kyopdf

import java.nio.charset.StandardCharsets.ISO_8859_1
import kyo.*
import zio.test.*

/** Paired, warmed comparison against frozen public v0.6.0 source. Synthetic
  * inputs are built before timing. This is a local workload, not a CI speed gate.
  */
object ParserBenchmark extends ZIOSpecDefault:
  private def checked[A](effect: A < Abort[PdfError]): A =
    Abort.run[PdfError](effect).eval match
      case kyo.Result.Success(value) => value
      case kyo.Result.Failure(error) => throw AssertionError(error.message)
      case kyo.Result.Panic(error) => throw error
  private val limit = checked(ByteLimit.mebibytes(32))
  private var sink = 0L
  private def sample(pages: Int, payload: Int): Array[Byte] =
    val out = new StringBuilder("%PDF-1.7\n")
    val offsets = scala.collection.mutable.ArrayBuffer(0)
    def obj(number: Int, body: String): Unit =
      offsets += out.length
      out.append(s"$number 0 obj\n$body\nendobj\n")
      ()
    obj(1, "<< /Type /Catalog /Pages 2 0 R >>")
    val kids = (0 until pages).map(i => s"${3 + i * 2} 0 R").mkString(" ")
    obj(2, s"<< /Type /Pages /Kids [$kids] /Count $pages >>")
    (0 until pages).foreach { i =>
      obj(3 + i * 2, s"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents ${4 + i * 2} 0 R >>")
      obj(4 + i * 2, s"<< /Length $payload >>\nstream\n${"x" * payload}\nendstream")
    }
    val xref = out.length
    out.append(s"xref\n0 ${offsets.length}\n0000000000 65535 f \n")
    offsets.drop(1).foreach(offset => out.append(f"$offset%010d 00000 n \n"))
    out.append(s"trailer << /Size ${offsets.length} /Root 1 0 R >>\nstartxref\n$xref\n%%EOF\n")
    out.result().getBytes(ISO_8859_1)

  private def compare(label: String, size: Int)(old: () => Long, current: () => Long): Unit =
    scala.Predef.assert(old() == current())
    (0 until 10).foreach { _ => sink ^= old(); sink ^= current() }
    def measured(f: () => Long): (Long, Long) =
      val allocated = Allocation.bytes()
      val start = java.lang.System.nanoTime()
      sink ^= f()
      val elapsed = java.lang.System.nanoTime() - start
      (elapsed, Allocation.bytes().zip(allocated).map((after, before) => after - before).getOrElse(-1L))
    val timings = (0 until 15).map { i =>
      if i % 2 == 0 then
        val before = measured(old)
        val after = measured(current)
        (before, after)
      else
        val after = measured(current)
        val before = measured(old)
        (before, after)
    }
    def median(values: Seq[Long]): Long = values.sorted.apply(values.length / 2)
    val oldTime = median(timings.map(_._1._1))
    val newTime = median(timings.map(_._2._1))
    val oldAlloc = median(timings.map(_._1._2))
    val newAlloc = median(timings.map(_._2._2))
    println(s"PARSER_BENCH {\"case\":\"$label\",\"bytes\":$size,\"oldNs\":$oldTime,\"newNs\":$newTime,\"oldAllocated\":$oldAlloc,\"newAllocated\":$newAlloc}")

  def spec = suite("Public parser comparison")(
    test("paired synthetic PDF and content workloads") {
      Vector(("small", 1, 32), ("stream-heavy", 4, 256 * 1024), ("object-heavy", 128, 32)).foreach { (name, pages, payload) =>
        val input = sample(pages, payload)
        val oldDocument = checked(baseline.PdfDocumentParser.decode(input, limit))
        val newDocument = checked(PdfDocumentParser.decode(input, limit))
        scala.Predef.assert(oldDocument == newDocument)
        compare(s"$name-scan", input.length)(
          () => checked(baseline.PdfParser.scan(input, limit)).facts.objects,
          () => checked(PdfParser.scan(input, limit)).facts.objects)
        compare(s"$name-decode", input.length)(
          () => checked(baseline.PdfDocumentParser.decode(input, limit)).objects.length.toLong,
          () => checked(PdfDocumentParser.decode(input, limit)).objects.length.toLong)
        val chunks = input.grouped(64 * 1024).toVector
        compare(s"$name-chunked-decode", input.length)(
          () => {
            val accumulated = chunks.foldLeft(baseline.PdfInput.empty(limit))((state, chunk) => checked(state.feed(chunk)))
            checked(accumulated.finish()).objects.length.toLong
          },
          () => {
            val reader = PdfRead.open(limit)
            chunks.foreach(chunk => scala.Predef.assert(reader.offer(chunk.length)(chunk.apply).isRight))
            reader.decode().toOption.get.objects.length.toLong
          })
      }
      val content = ("BT /F1 12 Tf (Synthetic) Tj ET\n" * 2000).getBytes(ISO_8859_1)
      def normalize(token: baseline.ContentToken): ContentToken = token match
        case baseline.ContentToken.Number(value) => ContentToken.Number(value)
        case baseline.ContentToken.Name(value) => ContentToken.Name(value)
        case baseline.ContentToken.Literal(bytes) => ContentToken.Literal(bytes)
        case baseline.ContentToken.Hex(bytes) => ContentToken.Hex(bytes)
        case baseline.ContentToken.Array(items) => ContentToken.Array(items.map(normalize))
        case baseline.ContentToken.Operator(value) => ContentToken.Operator(value)
      scala.Predef.assert(checked(baseline.PdfContentParser.parse(content)).map(normalize) == checked(PdfContentParser.parse(content)))
      compare("content", content.length)(
        () => checked(baseline.PdfContentParser.parse(content)).length.toLong,
        () => checked(PdfContentParser.parse(content)).length.toLong)
      assertTrue(sink >= Long.MinValue)
    }
  )
