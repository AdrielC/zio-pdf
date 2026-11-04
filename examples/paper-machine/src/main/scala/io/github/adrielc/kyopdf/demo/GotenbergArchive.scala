package io.github.adrielc.kyopdf.demo

import java.net.{HttpURLConnection, URI}
import java.nio.charset.StandardCharsets.US_ASCII
import java.util.UUID
import scala.util.Using

/** Operator-configured local conversion. Recipes carry no network addresses. */
private[demo] object GotenbergArchive:
  private val MaxBytes = 12 * 1024 * 1024

  def convert(bytes: Vector[Byte]): Vector[Byte] =
    val port = sys.env.get("KYO_PDF_GOTENBERG_PORT").flatMap(_.toIntOption).filter(p => p > 0 && p <= 65535)
      .getOrElse(throw IllegalStateException("Set KYO_PDF_GOTENBERG_PORT to the local Gotenberg port"))
    require(bytes.length <= MaxBytes, "PDF/A-3b conversion accepts at most 12 MiB")
    val boundary = UUID.randomUUID().toString
    val prefix = (s"--$boundary\r\nContent-Disposition: form-data; name=\"pdfa\"\r\n\r\nPDF/A-3b\r\n" +
      s"--$boundary\r\nContent-Disposition: form-data; name=\"files\"; filename=\"packet.pdf\"\r\n" +
      "Content-Type: application/pdf\r\n\r\n").getBytes(US_ASCII)
    val suffix = s"\r\n--$boundary--\r\n".getBytes(US_ASCII)
    val connection = URI.create(s"http://127.0.0.1:$port/forms/pdfengines/convert").toURL.openConnection().asInstanceOf[HttpURLConnection]
    try
      connection.setRequestMethod("POST")
      connection.setInstanceFollowRedirects(false)
      connection.setConnectTimeout(3000)
      connection.setReadTimeout(90000)
      connection.setDoOutput(true)
      connection.setRequestProperty("Content-Type", s"multipart/form-data; boundary=$boundary")
      connection.setFixedLengthStreamingMode(prefix.length + bytes.length + suffix.length)
      Using.resource(connection.getOutputStream) { out =>
        out.write(prefix)
        out.write(bytes.toArray)
        out.write(suffix)
      }
      val status = connection.getResponseCode
      require(status == 200, s"Gotenberg PDF/A-3b conversion returned HTTP $status")
      val converted = Using.resource(connection.getInputStream)(_.readNBytes(MaxBytes + 1))
      require(converted.length <= MaxBytes, "Converted PDF exceeds 12 MiB")
      require(converted.startsWith("%PDF-".getBytes(US_ASCII)), "Gotenberg did not return a PDF")
      converted.toVector
    finally connection.disconnect()
