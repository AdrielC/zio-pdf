package zio.pdf.examples

import java.nio.file.{Files, Paths}
import _root_.scodec.bits.BitVector
import zio.*
import zio.stream.ZStream
import zio.pdf.*

/** Writes a tiny PDF that requires visual substitution (mismatched /ToUnicode maps). */
object WriteFontSwapDemo extends ZIOAppDefault {

  private def font(
    baseFont: String,
    width: Int,
    objectNumber: Long,
    toUnicode: Option[Long]
  ): IndirectObj = {
    val data = Prim.dict(
      "Type"      -> Prim.Name("Font"),
      "Subtype"   -> Prim.Name("Type1"),
      "BaseFont"  -> Prim.Name(baseFont),
      "Encoding"  -> Prim.Name("WinAnsiEncoding"),
      "FirstChar" -> Prim.Number(BigDecimal(32)),
      "LastChar"  -> Prim.Number(BigDecimal(33)),
      "Widths"    -> Prim.Array(Prim.Number(BigDecimal(width)), Prim.Number(BigDecimal(width)))
    )
    val withCMap = toUnicode.fold(data)(number => Prim.Dict(data.data.updated("ToUnicode", Prim.Ref(number, 0))))
    IndirectObj.nostream(objectNumber, withCMap)
  }

  private val sourceCMap = BitVector(
    """/CIDInit /ProcSet findresource begin
      |2 beginbfchar
      |<41> <0041>
      |<42> <0042>
      |endbfchar
      |end""".stripMargin.getBytes
  )

  private val targetCMap = BitVector(
    """/CIDInit /ProcSet findresource begin
      |3 beginbfchar
      |<41> <0041>
      |<42> <0043>
      |<43> <0042>
      |endbfchar
      |end""".stripMargin.getBytes
  )

  private val pdfEffect: ZIO[Any, Throwable, Chunk[Byte]] =
    val catalog = IndirectObj.nostream(1, Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0)))
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
        "Type"      -> Prim.Name("Page"),
        "Parent"    -> Prim.Ref(2, 0),
        "MediaBox"  -> Prim.Array.nums(0, 0, 612, 792),
        "Resources" -> Prim.dict(
          "Font" -> Prim.dict(
            "F1" -> Prim.Ref(5, 0),
            "F2" -> Prim.Ref(6, 0)
          )
        ),
        "Contents" -> Prim.Ref(4, 0)
      )
    )
    val content = IndirectObj.stream(4, Prim.Dict.empty, BitVector("BT /F1 12 Tf (AB) Tj ET\n".getBytes))
    val sourceCMapObject = IndirectObj.stream(7, Prim.Dict.empty, sourceCMap)
    val targetCMapObject = IndirectObj.stream(8, Prim.Dict.empty, targetCMap)
    val trailer = Trailer(BigDecimal(9), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

    ZStream(
      catalog,
      pages,
      page,
      content,
      font("SourceFace", 500, 5, Some(7)),
      font("TargetFace", 500, 6, Some(8)),
      sourceCMapObject,
      targetCMapObject
    )
      .via(WritePdf.objects(trailer))
      .runFold(Chunk.empty[Byte])((all, next) => all ++ Chunk.fromArray(next.toArray))

  override def run: ZIO[Any, Throwable, ExitCode] =
    val out =
      sys.props
        .get("zio.pdf.swapDemoOut")
        .map(Paths.get(_))
        .getOrElse(Paths.get("examples-js/frontend/public/font-swap-demo.pdf"))

    pdfEffect.flatMap { bytes =>
      ZIO.attempt {
        Files.createDirectories(out.getParent)
        Files.write(out, bytes.toArray)
      }.as {
        println(s"Wrote visual font swap demo PDF to $out (${bytes.length} bytes)")
        ExitCode.success
      }
    }
}
