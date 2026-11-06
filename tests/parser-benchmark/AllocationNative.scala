package io.github.adrielc.kyopdf

object Allocation:
  // No JVM management API is available on Native; report elapsed time only.
  def bytes(): Option[Long] = None
