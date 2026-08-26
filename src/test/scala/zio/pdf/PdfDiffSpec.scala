package zio.pdf

import _root_.scodec.bits.BitVector
import zio.Chunk
import zio.blocks.schema.Schema
import zio.stream.ZStream
import zio.test.*

object PdfDiffSpec extends ZIOSpecDefault:

  private def data(number: Long, value: Prim): Decoded =
    Decoded.DataObj(Obj(Obj.Index(number, 0), value))

  private def content(number: Long, dictionary: Prim, raw: String): Decoded =
    Decoded.ContentObj(Obj(Obj.Index(number, 0), dictionary), BitVector(raw.getBytes), Uncompressed.now(BitVector.empty))

  private def diff(left: Seq[Decoded], right: Seq[Decoded]): zio.ZIO[Any, Throwable, Chunk[PdfDiff.Window]] =
    PdfDiff.fromDecoded(ZStream.fromIterable(left), ZStream.fromIterable(right)).runCollect

  def spec: Spec[Any, Throwable] = suite("PdfDiff")(

    test("aligns equal schema values even when a rewrite renumbers objects") {
      for windows <- diff(
        Seq(data(1, Prim.Dict("Type" -> Prim.Name("Catalog")))),
        Seq(data(41, Prim.Dict("Type" -> Prim.Name("Catalog"))))
      ) yield assertTrue(
        windows.size == 1,
        windows.head.edits == Chunk(
          PdfDiff.Edit.Same(
            PdfDiff.Component(PdfDiff.Location.Object(PdfDiff.ObjectRef(1, 0)), PdfDiff.Value.Primitive(Prim.Dict("Type" -> Prim.Name("Catalog")))),
            PdfDiff.Component(PdfDiff.Location.Object(PdfDiff.ObjectRef(41, 0)), PdfDiff.Value.Primitive(Prim.Dict("Type" -> Prim.Name("Catalog"))))
          )
        )
      )
    },

    test("emits an LCS insertion without collecting either input") {
      for windows <- diff(Seq(
        data(1, Prim.Name("A")),
        data(2, Prim.Name("B")),
        data(3, Prim.Name("C"))
      ), Seq(
        data(10, Prim.Name("A")),
        data(11, Prim.Name("X")),
        data(12, Prim.Name("B")),
        data(13, Prim.Name("C"))
      )) yield {
        val edits = windows.flatMap(_.edits)
        assertTrue(
          edits.count(_.isInstanceOf[PdfDiff.Edit.Same]) == 3,
          edits.collect { case PdfDiff.Edit.Added(component) => component.value } == Chunk(PdfDiff.Value.Primitive(Prim.Name("X")))
        )
      }
    },

    test("attaches a DynamicPatch to a changed primitive component") {
      val oldValue = Prim.Dict("Count" -> Prim.Number(BigDecimal(1)), "Label" -> Prim.Str(_root_.scodec.bits.ByteVector("old".getBytes)))
      val newValue = Prim.Dict("Count" -> Prim.Number(BigDecimal(2)), "Label" -> Prim.Str(_root_.scodec.bits.ByteVector("new".getBytes)))
      for windows <- diff(Seq(data(1, oldValue)), Seq(data(7, newValue)))
      yield windows.head.edits.head match
        case PdfDiff.Edit.Changed(left, right, patch, false) =>
          assertTrue(
            !patch.isEmpty,
            patch(PdfDiff.dynamicValue(left)) == Right(PdfDiff.dynamicValue(right))
          )
        case _ => assertTrue(false)
    },

    test("coalesces a contiguous replacement run into DynamicPatch edits") {
      for windows <- diff(
        Seq(data(1, Prim.Name("A")), data(2, Prim.Name("B"))),
        Seq(data(3, Prim.Name("C")), data(4, Prim.Name("D")))
      ) yield {
        val changed = windows.head.edits.collect { case change: PdfDiff.Edit.Changed => change }
        assertTrue(
          changed.size == 2,
          changed.forall(change => change.patch(PdfDiff.dynamicValue(change.left)) == Right(PdfDiff.dynamicValue(change.right)))
        )
      }
    },

    test("surfaces a changed content-stream fingerprint as a semantic edit") {
      val dictionary = Prim.Dict("Length" -> Prim.Number(BigDecimal(3)))
      for windows <- diff(Seq(content(1, dictionary, "one")), Seq(content(2, dictionary, "two")))
      yield windows.head.edits.head match
        case PdfDiff.Edit.Changed(_, _, patch, true) => assertTrue(!patch.isEmpty)
        case _                                       => assertTrue(false)
    },

    test("exports every component through a Blocks schema") {
      val component = PdfDiff.Component(
        PdfDiff.Location.Object(PdfDiff.ObjectRef(4, 0)),
        PdfDiff.Value.Primitive(Prim.Name("Page"))
      )
      val schema = summon[Schema[PdfDiff.Component]]
      val valueSchema = summon[Schema[PdfDiff.Value]]
      assertTrue(
        schema.fromDynamicValue(PdfDiff.componentDynamicValue(component)) == Right(component),
        valueSchema.fromDynamicValue(PdfDiff.dynamicValue(component)) == Right(component.value)
      )
    },

    test("caps every LCS alignment at the configured component window") {
      val config = PdfDiff.Config(windowSize = 2, maximumCells = 9)
      val values = (1L to 5L).map(number => data(number, Prim.Number(BigDecimal(number))))
      for windows <- PdfDiff.fromDecoded(ZStream.fromIterable(values), ZStream.fromIterable(values), config).runCollect
      yield assertTrue(
        windows.size == 3,
        windows.forall(window => window.leftSize <= 2 && window.rightSize <= 2 && window.exactWithinWindow)
      )
    }
  )
