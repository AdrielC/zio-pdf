package com.tybera.kyopdf

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.ISO_8859_1
import java.util.zip.DeflaterOutputStream
import kyo.*

/** Kyo port of zio-pdf's renderer-neutral `/Thumb` image construction.
  * A real renderer can supply DeviceGray pixels; the default is deterministic.
  */
object PdfThumbnail:
  type PixelSource = (Long, Int, Int) => Either[String, Array[Byte]]

  final case class Options(width: Int = 64, height: Int = 64) derives Schema

  final case class ImageObject(info: ThumbnailInfo, bytes: Array[Byte])

  def imageObject(
      objectNumber: Long,
      pageNumber: Long,
      options: Options = Options(),
      pixels: Option[PixelSource] = None
  ): ImageObject < (Abort[PdfError] & Sync) =
    val width = math.max(1, options.width)
    val height = math.max(1, options.height)
    val source = pixels.fold[Either[String, Array[Byte]]](Right(patternPixels(pageNumber, width, height)))(_(pageNumber, width, height))
    source match
      case Left(error) => Abort.fail(PdfError.ThumbnailFailed(error))
      case Right(raw) if raw.length < width * height =>
        Abort.fail(PdfError.ThumbnailFailed(s"Expected ${width * height} thumbnail bytes, got ${raw.length}"))
      case Right(raw) =>
        Abort.catching[Throwable](error => PdfError.ThumbnailFailed(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))) {
          Sync.defer {
            val compressed = compress(raw.take(width * height))
            val header =
              s"$objectNumber 0 obj\n<< /Type /XObject /Subtype /Image /Width $width /Height $height " +
                s"/ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /FlateDecode /Length ${compressed.length} >>\nstream\n"
            val footer = "\nendstream\nendobj\n"
            val output = header.getBytes(ISO_8859_1) ++ compressed ++ footer.getBytes(ISO_8859_1)
            ImageObject(ThumbnailInfo(objectNumber, pageNumber, width, height, compressed.length), output)
          }
        }

  private def compress(bytes: Array[Byte]): Array[Byte] =
    val output = new ByteArrayOutputStream()
    val deflater = new DeflaterOutputStream(output)
    try deflater.write(bytes)
    finally deflater.close()
    output.toByteArray

  private def patternPixels(pageNumber: Long, width: Int, height: Int): Array[Byte] =
    val border = math.max(1, math.min(width, height) / 8)
    Array.tabulate(width * height) { index =>
      val x = index % width
      val y = index / width
      if x < border || y < border || x >= width - border || y >= height - border then 0xff.toByte
      else (0xd0 - math.floorMod(pageNumber, 8).toInt * 8).toByte
    }
