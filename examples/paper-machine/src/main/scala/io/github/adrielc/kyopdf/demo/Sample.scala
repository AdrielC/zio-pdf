package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import java.nio.charset.StandardCharsets.ISO_8859_1

/** Original synthetic vector artwork, constructed through the Kyo document algebra. */
object Sample:
  private def text(x: Int, y: Int, size: Int, value: String): String =
    s"BT /F1 $size Tf $x $y Td ($value) Tj ET\n"
  private val titles = Vector("ORBIT", "FLOW", "SIGNAL", "FORM")
  private val colors = Vector("0.88 0.24 0.14", "0.16 0.33 0.82", "0.67 0.42 0.04", "0.18 0.47 0.33")
  private def artwork(index: Int): String = index match
    case 0 => "3 w 480 160 m 480 210 440 250 390 250 c 340 250 300 210 300 160 c " +
      "300 110 340 70 390 70 c 440 70 480 110 480 160 c S\n315 80 18 18 re f\n"
    case 1 => "4 w 290 80 m 500 80 300 245 510 245 c S 290 115 m 500 115 300 280 510 280 c S 290 150 m 500 150 300 315 510 315 c S\n"
    case 2 => (0 until 12).map(i => s"${290 + i * 18} 80 10 ${30 + ((i * 47) % 175)} re f").mkString("\n") + "\n"
    case _ => "3 w 300 80 180 180 re S 330 110 180 180 re S 360 140 180 180 re S\n"

  val document: PdfDocument =
    import PdfValue.*
    val pageObjects = titles.indices.toVector.flatMap { index =>
      val pageRef = ObjectRef(4 + index * 2L)
      val streamRef = ObjectRef(5 + index * 2L)
      val content = ("0.97 0.96 0.92 rg 0 0 595 420 re f\n0.10 0.13 0.13 rg\n" +
        text(34, 370, 10, "PAPER MACHINE / FIELD NOTES") + text(34, 270, 70, f"${index + 1}%02d") +
        text(34, 225, 29, titles(index)) + text(34, 180, 10, "A small study in PDF primitives.") +
        text(34, 163, 10, "Select. Rearrange. Make it yours.") + text(34, 32, 9, "SYNTHETIC DEMO / MADE WITH KYOPDF") +
        s"${colors(index)} RG ${colors(index)} rg\n" + artwork(index)).getBytes(ISO_8859_1).toVector
      Vector(
        PdfObject(pageRef, dict("Type" -> Name("Page"), "Parent" -> Ref(ObjectRef(2)),
          "MediaBox" -> Array(Vector(Number(0), Number(0), Number(595), Number(420))),
          "Resources" -> dict("Font" -> dict("F1" -> Ref(ObjectRef(3)))), "Contents" -> Ref(streamRef))),
        PdfObject(streamRef, dict("Length" -> Number(content.length)), Some(content))
      )
    }
    PdfDocument("1.7", Vector(
      PdfObject(ObjectRef(1), dict("Type" -> Name("Catalog"), "Pages" -> Ref(ObjectRef(2)))),
      PdfObject(ObjectRef(2), dict("Type" -> Name("Pages"), "Count" -> Number(4),
        "Kids" -> Array(titles.indices.map(i => Ref(ObjectRef(4 + i * 2L))).toVector))),
      PdfObject(ObjectRef(3), dict("Type" -> Name("Font"), "Subtype" -> Name("Type1"), "BaseFont" -> Name("Helvetica")))
    ) ++ pageObjects, dict("Root" -> Ref(ObjectRef(1)), "Size" -> Number(12),
      "ID" -> Array(Vector(Hex(Vector(1, 2, 3, 4).map(_.toByte)), Hex(Vector(5, 6, 7, 8).map(_.toByte))))))
