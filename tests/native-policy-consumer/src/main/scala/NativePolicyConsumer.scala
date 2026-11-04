import java.nio.file.{Files, Path}

object NativePolicyConsumer:
  def main(args: Array[String]): Unit =
    val (_, output) = PolicyConsumerChecks.run()
    require(args.length == 1, "Supply the output PDF path")
    Files.write(Path.of(args(0)), output.toArray)
    println("Native Policy API compiled SchemaExpr, round-tripped and ran a conditional pipeline, and wrote the checked PDF.")
