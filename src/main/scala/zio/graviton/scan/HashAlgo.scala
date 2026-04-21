package zio.graviton.scan

import zio.blocks.schema.Schema

enum HashAlgo derives Schema:
  case Sha256, Blake3
