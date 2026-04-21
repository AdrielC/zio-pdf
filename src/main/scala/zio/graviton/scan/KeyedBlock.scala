package zio.graviton.scan

import zio.blocks.schema.Schema

/** Minimal keyed block description for ingest pipeline typing / schema smoke. */
final case class KeyedBlock(
  bytes: Array[Byte],
  key: Long
) derives Schema
