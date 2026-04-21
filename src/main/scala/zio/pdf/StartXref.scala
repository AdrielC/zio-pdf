/*
 * Port of fs2.pdf.StartXref to Scala 3 + scodec 2.3.
 */

package zio.pdf

import zio.pdf.codec.{Text, Whitespace}
import _root_.scodec.Codec
import _root_.scodec.codecs.{bitsRemaining, optional}

/**
 * The trailing `startxref / offset / %%EOF` triple that closes a PDF.
 * For PDFs that compress their xref into a stream, this is what
 * appears at the end of the file in lieu of a textual `xref` block.
 */
final case class StartXref(offset: Long)

object StartXref {

  import Text.{ascii, str}
  import Whitespace.skipWs as nlWs

  /** `%%EOF` followed by optional trailing whitespace / EOF. */
  val eof: Codec[Unit] =
    str("%%EOF") <~ optional(bitsRemaining, nlWs).unit(Some(()))

  given codec: Codec[StartXref] =
    (str("startxref") ~> nlWs ~> ascii.long.withContext("startxref offset") <~ nlWs <~ eof)
      .withContext("startxref")
      .xmap(StartXref(_), _.offset)
}
