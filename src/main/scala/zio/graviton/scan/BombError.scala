package zio.graviton.scan

import zio.blocks.schema.Schema

final case class BombError(seen: Long, limit: Long) derives Schema
