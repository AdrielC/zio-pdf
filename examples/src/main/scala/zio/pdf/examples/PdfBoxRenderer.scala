/*
 * JVM-only PDF page rasterization for runnable examples (Apache PDFBox).
 *
 * Not published — inject [[PdfThumbnail.PixelSource]] at the application edge.
 */

package zio.pdf.examples

import java.awt.{Color, RenderingHints}
import java.awt.image.{BufferedImage, DataBufferByte, Raster}

import zio.pdf.PdfThumbnail

object PdfBoxRenderer {

  /** Build a [[PdfThumbnail.PixelSource]] backed by PDFBox for fixed PDF bytes. */
  def pixelSource(pdfBytes: Array[Byte]): PdfThumbnail.PixelSource =
    (pageNumber, width, height) =>
      renderPageGrayscale(pdfBytes, pageNumber.toInt, width, height)

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
          Right(extractGray(fit(rendered, width, height), width, height))
      finally
        document.close()
    catch {
      case ex: Throwable => Left(s"PdfBoxRenderer: ${ex.getMessage}")
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
}
