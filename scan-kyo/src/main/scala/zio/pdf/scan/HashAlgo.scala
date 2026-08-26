/*
 * Hash algorithm enum -- a closed set so a `ScanPrim.Hash(algo)` is fully
 * serialisable as a tag plus a few configuration bytes.
 *
 * Every advertised algorithm maps to a mandatory JCA digest. The enum never
 * substitutes one digest for another because doing so would corrupt content
 * addresses while appearing to succeed.
 */

package zio.pdf.scan

import java.security.MessageDigest

enum HashAlgo(val name: String) {
  case Sha256          extends HashAlgo("SHA-256")
  case Sha512          extends HashAlgo("SHA-512")
  case Sha1            extends HashAlgo("SHA-1")
  case Md5             extends HashAlgo("MD5")
  /** Construct a fresh digest for an algorithm required by the JCA runtime. */
  def newDigest(): MessageDigest =
    MessageDigest.getInstance(name)
}

/** Failure carried by `BombGuard` when the byte budget is exceeded. */
final case class BombError(seen: Long, limit: Long) {
  override def toString: String =
    s"BombError(seen=$seen, limit=$limit)"
}
