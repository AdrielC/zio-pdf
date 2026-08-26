package zio.pdf.cdc

/** Benchmark-module FastCDC primitives kept out of the published SDK. */
private[pdf] object FastCdcSupport {

  private val Gear: Array[Long] = {
    val rng = new java.util.Random(0x9e3779b97f4a7c15L)
    Array.fill(256)(rng.nextLong())
  }

  def cutOffset(buffer: Array[Byte], cfg: FastCdc.Config): Int = {
    val n = buffer.length
    if n <= cfg.minSize then n
    else {
      val maxScan = math.min(n, cfg.maxSize)
      val avgScan = math.min(cfg.avgSize, maxScan)
      var i       = cfg.minSize
      var hash    = 0L
      var cut     = 0

      while i < avgScan && cut == 0 do {
        hash = (hash << 1) + Gear(buffer(i) & 0xff)
        if (hash & cfg.maskSmall) == 0L then cut = i + 1
        i += 1
      }
      while i < maxScan && cut == 0 do {
        hash = (hash << 1) + Gear(buffer(i) & 0xff)
        if (hash & cfg.maskLarge) == 0L then cut = i + 1
        i += 1
      }
      if cut == 0 then maxScan else cut
    }
  }

  def drain(
    buffer: Array[Byte],
    flushTail: Boolean,
    cfg: FastCdc.Config
  ): (Array[Array[Byte]], Array[Byte]) = {
    val out = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
    var buf = buffer
    while buf.length >= cfg.maxSize || (flushTail && buf.nonEmpty) do {
      val window =
        if buf.length >= cfg.maxSize then java.util.Arrays.copyOfRange(buf, 0, cfg.maxSize)
        else buf
      val cut = cutOffset(window, cfg)
      out += java.util.Arrays.copyOfRange(buf, 0, cut)
      buf = java.util.Arrays.copyOfRange(buf, cut, buf.length)
    }
    (out.toArray, buf)
  }

  def append(prefix: Array[Byte], bytes: Array[Byte], from: Int, until: Int): Array[Byte] = {
    val merged = new Array[Byte](prefix.length + until - from)
    System.arraycopy(prefix, 0, merged, 0, prefix.length)
    System.arraycopy(bytes, from, merged, prefix.length, until - from)
    merged
  }
}
