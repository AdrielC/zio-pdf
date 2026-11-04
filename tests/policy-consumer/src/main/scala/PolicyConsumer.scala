import io.github.adrielc.kyopdf.zio.PdfZIO

object PolicyConsumer:
  def main(args: Array[String]): Unit =
    val (document, output) = PolicyConsumerChecks.run()
    val adapted = zio.Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe.run(PdfZIO.write(document)).getOrThrowFiberFailure()
    }
    assert(output == adapted)
    println("Published Policy API compiled SchemaExpr, round-tripped and ran a conditional pipeline, and linked the ZIO adapter.")
