/*
 * JVM-only PDF page rasterization for tests (Apache PDFBox).
 *
 * Not part of the published artifact — inject [[PdfThumbnail.PixelSource]] at call sites.
 */

package zio.pdf

import java.awt.{Color, RenderingHints}
import java.awt.image.{BufferedImage, DataBufferByte, Raster}

import javax.imageio.ImageIO

object PdfRenderer {

  /** Build a [[PdfThumbnail.PixelSource]] backed by PDFBox for a fixed PDF byte array. */
  def pixelSource(pdfBytes: Array[Byte]): PdfThumbnail.PixelSource =
    (pageNumber, width, height) =>
      renderPageGrayscale(pdfBytes, pageNumber.toInt, width, height)

  /** Render a page to DeviceGray bytes suitable for a PDF `/Thumb` image XObject. */
  def renderPageGrayscale(
    pdfBytes: Array[Byte],
    pageIndex: Int,
    width: Int,
    height: Int
  ): Either[String, Array[Byte]] =
    try
      val document = org.apache.pdfbox.Loader.loadPDF(pdfBytes)
      try
        if pageIndex < 0 || pageIndex >= document.getNumberOfPages then
          Left(s"page index $pageIndex out of range (pages=${document.getNumberOfPages})")
        else
          val renderer = new org.apache.pdfbox.rendering.PDFRenderer(document)
          val page     = document.getPage(pageIndex)
          val media    = page.getMediaBox
          val scale    = math.min(width / media.getWidth, height / media.getHeight)
          val rendered = renderer.renderImage(pageIndex, scale, org.apache.pdfbox.rendering.ImageType.GRAY)
          val fitted   = fit(rendered, width, height)
          Right(extractGray(fitted, width, height))
      finally
        document.close()
    catch {
      case ex: Throwable => Left(s"PdfRenderer: ${ex.getMessage}")
    }

  /** Encode rendered grayscale pixels as a Flate-compressed PDF image XObject. */
  def thumbObject(number: Long, pdfBytes: Array[Byte], pageIndex: Int, width: Int, height: Int): Either[String, IndirectObj] =
    renderPageGrayscale(pdfBytes, pageIndex, width, height).flatMap { pixels =>
      PdfThumbnail.grayImageObject(number, width, height, pixels)
    }

  private def fit(source: BufferedImage, width: Int, height: Int): BufferedImage =
    if source.getWidth == width && source.getHeight == height then source
    else
      val target = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
      val graphics = target.createGraphics()
      graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR)
      graphics.setColor(Color.WHITE)
      graphics.fillRect(0, 0, width, height)
      val offsetX = (width - source.getWidth) / 2
      val offsetY = (height - source.getHeight) / 2
      graphics.drawImage(source, offsetX, offsetY, null)
      graphics.dispose()
      target

  private def extractGray(image: BufferedImage, width: Int, height: Int): Array[Byte] = {
    val gray = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val graphics = gray.createGraphics()
    graphics.drawImage(image, 0, 0, null)
    graphics.dispose()
    val raster: Raster = gray.getRaster
    val data           = raster.getDataBuffer.asInstanceOf[DataBufferByte].getData
    if data.length >= width * height then data.take(width * height).toArray
    else
      val out = new Array[Byte](width * height)
      System.arraycopy(data, 0, out, 0, math.min(data.length, out.length))
      out
  }

  /** Best-effort PNG bytes for debugging rendered thumbnails outside PDF viewers. */
  def renderPagePng(pdfBytes: Array[Byte], pageIndex: Int, width: Int, height: Int): Either[String, Array[Byte]] =
    renderPageGrayscale(pdfBytes, pageIndex, width, height).flatMap { gray =>
      try
        val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
        image.getRaster.setDataElements(0, 0, width, height, gray)
        val out = new java.io.ByteArrayOutputStream
        ImageIO.write(image, "png", out)
        Right(out.toByteArray)
      catch {
        case ex: Throwable => Left(ex.getMessage)
      }
    }
}
