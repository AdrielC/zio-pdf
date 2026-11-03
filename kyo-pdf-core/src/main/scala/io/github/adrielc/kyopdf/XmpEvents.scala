package io.github.adrielc.kyopdf

private[kyopdf] trait XmpEvents:
  private var count = 0
  final def event(): Unit =
    count += 1
    require(count <= 50000, "XMP event limit exceeded")

  def start(namespace: String, local: String, attributes: => Iterator[(String, String, String)]): Unit
  def characters(value: => String): Unit
  def end(): Unit
