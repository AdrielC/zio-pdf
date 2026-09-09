package zio.pdf

/** IANA media type helpers without the zio-blocks-mediatype Native artifact. */
object PdfMime:

  val mimeType: String = "application/pdf"

  val contentTypeHeader: String = mimeType
