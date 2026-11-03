package io.github.adrielc.kyopdf.demo

import io.github.adrielc.kyopdf.*
import kyo.*
import java.awt.color.{ColorSpace, ICC_Profile}
import java.io.ByteArrayOutputStream
import org.apache.pdfbox.cos.COSName
import org.apache.pdfbox.pdmodel.{PDDocument, PDPage, PDPageContentStream}
import org.apache.pdfbox.pdmodel.common.PDRectangle
import org.apache.pdfbox.pdmodel.font.PDType0Font
import org.apache.pdfbox.pdmodel.graphics.color.{PDColor, PDICCBased}
import scala.util.Using

final case class PacketOptions(stamp: String = "", placement: String = "top", numbered: Boolean = false, start: Int = 1, style: String = "page", prefix: String = "")

/** One document pipeline and one final Kyo write. PDFBox authors only the small
  * embedded-font artwork; source pages stay in Kyo's object graph throughout.
  */
object PacketBuild:
  import PdfValue.*
  private def checked[A](value: => A): A < (Abort[PdfError] & Sync) =
    Abort.catching[Exception](error => PdfError.InvalidPdf(Option(error.getMessage).getOrElse("Could not build the packet")))(Sync.defer(value))

  def run(body: PdfDocument, order: Vector[Int], head: Option[PdfDocument], tail: Option[PdfDocument], options: PacketOptions): Vector[Byte] < (Abort[PdfError] & Sync) =
    checked {
      require(options.stamp.length <= 80 && options.prefix.length <= 24 &&
        (options.stamp + options.prefix).forall(c => !Character.isISOControl(c)), "Use a single-line stamp up to 80 characters and prefix up to 24 characters")
      require(Set("top", "center")(options.placement) && Set("page", "number", "bates")(options.style), "Choose a supported stamp placement and number format")
      require(options.start >= 1 && options.start <= 999999, "Start numbering from 1 to 999999")
    }.map { _ => PdfPages.select(body, order).map { selected =>
      PdfAssembly.concatenate(head.toVector ++ Vector(selected) ++ tail.toVector, head.size).map { assembled =>
        finish(assembled, options).map(PdfWriter.write)
      }
    }}

  /** Artwork interpreter shared by fixed packets and data-defined recipes. */
  def finish(assembled: PdfDocument, options: PacketOptions): PdfDocument < (Abort[PdfError] & Sync) =
    PdfPages.pageRefs(assembled).map { pages =>
      if pages.length > 64 || assembled.objects.length > 8000 then Abort.fail(PdfError.RetentionExceeded("A packet may contain at most 64 pages and 8000 objects"))
      else if options.stamp.isEmpty && !options.numbered then assembled
      else checked(artwork(assembled, pages, options)).map { bytes =>
        ByteLimit.fromBytes(4 * 1024 * 1024).map { limit => PdfDocument.decode(bytes, limit).map { art =>
          PdfAssembly.overlay(assembled, art)
        }}
      }
    }

  private def artwork(document: PdfDocument, pages: Vector[ObjectRef], options: PacketOptions): scala.Array[Byte] =
    def resolve(value: PdfValue, depth: Int = 0): PdfValue = value match
      case Ref(ref) =>
        require(depth < 32, "Over-deep page geometry")
        resolve(document.byRef(ref).value, depth + 1)
      case other => other
    def number(value: PdfValue): Float = resolve(value) match
      case Number(value) => value.toFloat
      case _ => throw IllegalArgumentException("Invalid page geometry")
    Using.resource(new PDDocument()) { pdf =>
      val font = Using.resource(getClass.getResourceAsStream("/paper-machine/chivo.ttf"))(PDType0Font.load(pdf, _, true))
      val colorSpace = new PDICCBased(pdf)
      colorSpace.getPDStream.getCOSObject.setInt(COSName.N, 3)
      Using.resource(colorSpace.getPDStream.createOutputStream(COSName.FLATE_DECODE))(_.write(ICC_Profile.getInstance(ColorSpace.CS_sRGB).getData))
      val ink = new PDColor(scala.Array(0.08f, 0.18f, 0.24f), colorSpace)
      val red = new PDColor(scala.Array(0.65f, 0.14f, 0.10f), colorSpace)
      pages.zipWithIndex.foreach { (ref, index) =>
        val page = document.byRef(ref).dictionary.get
        def rectangle(value: PdfValue): Vector[Float] = resolve(value) match
          case Array(values) if values.length == 4 => values.map(number)
          case _ => throw IllegalArgumentException("Invalid page crop box")
        val media = rectangle(page.get("MediaBox").get)
        val crop = page.get("CropBox").fold(media)(rectangle)
        val bounds = Vector(math.max(media(0), crop(0)), math.max(media(1), crop(1)), math.min(media(2), crop(2)), math.min(media(3), crop(3)))
        val rotate = page.get("Rotate").map(number).getOrElse(0f)
        require(rotate.isFinite && rotate % 90 == 0, "Page rotation must be a multiple of 90")
        val sideways = ((rotate.toInt % 360) + 360) % 180 == 90
        val width = if sideways then bounds(3) - bounds(1) else bounds(2) - bounds(0)
        val height = if sideways then bounds(2) - bounds(0) else bounds(3) - bounds(1)
        val unit = page.get("UserUnit").map(number).getOrElse(1f)
        require(width.isFinite && height.isFinite && width > 0 && height > 0 && width <= 100000 && height <= 100000 && unit.isFinite && unit >= 0.01 && unit <= 75000, "Page dimensions are outside stamping bounds")
        val target = new PDPage(new PDRectangle(width, height))
        pdf.addPage(target)
        Using.resource(new PDPageContentStream(pdf, target)) { stream =>
          val margin = math.min(18f / unit, math.min(width, height) * 0.08f)
          def label(text: String, size: Float, top: Boolean, centered: Boolean, color: PDColor): Unit =
            val glyphWidth = font.getStringWidth(text) / 1000f
            val fitted = math.min(size / unit, (width - margin * 2) / math.max(1f, glyphWidth))
            val actualWidth = glyphWidth * fitted
            val x = if centered then (width - actualWidth) / 2 else width - margin - actualWidth
            val y = if top then height - margin - fitted else if centered && color == red then height / 2 else margin
            stream.beginText()
            stream.setFont(font, fitted)
            stream.setNonStrokingColor(color)
            stream.newLineAtOffset(x, y)
            stream.showText(text)
            stream.endText()
          if options.stamp.nonEmpty then label(options.stamp, if options.placement == "center" then 36f else 18f, options.placement == "top", options.placement == "center", red)
          if options.numbered then
            val current = options.start + index
            val text = options.prefix + (options.style match
              case "bates" => f"$current%06d"
              case "number" => current.toString
              case _ => s"Page $current of ${options.start + pages.length - 1}")
            label(text, 10f, false, true, ink)
        }
      }
      val output = new ByteArrayOutputStream()
      pdf.save(output)
      require(output.size <= 4 * 1024 * 1024, "Stamp artwork exceeds 4 MiB")
      output.toByteArray
    }
