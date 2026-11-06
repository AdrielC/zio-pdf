package io.github.adrielc.kyopdf

object Allocation:
  private val bean = java.lang.management.ManagementFactory.getThreadMXBean match
    case value: com.sun.management.ThreadMXBean if value.isThreadAllocatedMemorySupported =>
      value.setThreadAllocatedMemoryEnabled(true)
      Some(value)
    case _ => None
  def bytes(): Option[Long] = bean.map(_.getThreadAllocatedBytes(Thread.currentThread().threadId()))
