package zio.graviton.scan

import zio.blocks.schema.Schema

/** JVM-backed digest algorithms for [[ScanPrim.Hash]]. */
enum HashAlgo derives Schema:
  case Sha256
  /** Placeholder name: uses SHA-256 until a Blake3 provider is wired in. */
  case Blake3

  def digestAlgorithm: String =
    this match
      case Sha256 => "SHA-256"
      case Blake3 => "SHA-256"
