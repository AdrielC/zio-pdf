/*
 * Fail-closed encryption detection for write workflows.
 *
 * The library does not decrypt. Encrypted filings are reported structurally
 * and rejected before merge, append, linearize, flatten, or transform rewrite.
 */

package zio.pdf

import zio.Chunk

object PdfCrypto {

  final case class Encrypted(reference: Option[Prim.Ref])
      extends Exception(
        reference.fold("encrypted PDFs cannot be rewritten") { ref =>
          s"encrypted PDFs cannot be rewritten (Encrypt ${ref.number} ${ref.generation} R)"
        }
      )

  def encryption(trailer: Trailer): Option[PdfInspection.Encryption] =
    trailer.data.data.get("Encrypt").map {
      case reference: Prim.Ref => PdfInspection.Encryption(Some(reference))
      case _                   => PdfInspection.Encryption(None)
    }

  def encryption(decoded: Chunk[Decoded]): Option[PdfInspection.Encryption] =
    decoded.foldLeft(Option.empty[PdfInspection.Encryption]) {
      case (found, Decoded.Meta(_, Some(trailer), _)) => found.orElse(encryption(trailer))
      case (found, _)                                 => found
    }

  def requireUnencrypted(trailer: Trailer): Either[Encrypted, Unit] =
    encryption(trailer).fold(Right(()))(found => Left(Encrypted(found.reference)))

  def requireUnencrypted(decoded: Chunk[Decoded]): Either[Encrypted, Unit] =
    encryption(decoded).fold(Right(()))(found => Left(Encrypted(found.reference)))
}
