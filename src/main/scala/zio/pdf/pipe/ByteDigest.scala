package zio.pdf.pipe

import zio.pdf.pipe.FusedDecode.{Cfg, Slice}

object ByteDigest {

  def digestSlice(slice: Slice, cfg: Cfg = Cfg()): Array[Byte] =
    ByteFeed.digestBatched(slice, cfg.batchSize)
}
