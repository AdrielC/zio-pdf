/*
 * Hash algorithm enum -- a closed set so a `ScanPrim.Hash(algo)` is fully
 * serialisable as a tag plus a few configuration bytes.
 *
 * `Blake3` is included because Graviton uses it for content addressing;
 * `MessageDigest` is the JVM standard so the runtime falls back to that
 * for the SHA family.
 */

package zio.pdf.scan

import java.security.MessageDigest

enum HashAlgo(val name: String) {
  case Sha256          extends HashAlgo("SHA-256")
  case Sha512          extends HashAlgo("SHA-512")
  case Sha1            extends HashAlgo("SHA-1")
  case Md5             extends HashAlgo("MD5")
  case Blake3          extends HashAlgo("BLAKE3")

  /** Construct a fresh `MessageDigest` instance for this algorithm. Falls
    * back to SHA-256 for `Blake3` because the JDK doesn't ship a Blake3
    * provider; production code would register a Bouncy Castle provider or
    * use a dedicated JNI binding -- here we keep the build dependency-light
    * and treat that as a TODO marker rather than blocking the design. */
  def newDigest(): MessageDigest =
    try MessageDigest.getInstance(name)
    catch {
      case _: java.security.NoSuchAlgorithmException =>
        // Fall back so the scan algebra remains usable in environments
        // where the requested provider is not installed.
        MessageDigest.getInstance("SHA-256")
    }
}

/** Failure carried by `BombGuard` when the byte budget is exceeded. */
final case class BombError(seen: Long, limit: Long) {
  override def toString: String =
    s"BombError(seen=$seen, limit=$limit)"
}
