import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.github.adrielc.scan.{ObservationSyntax, Scan}
import io.github.adrielc.scan.fs2.ScanFs2.*

object ScanConsumerChecks:
  private final case class Bytes(value: Vector[Byte])
  private final case class Count(value: Int)
  def run(): Unit =
    import ObservationSyntax.*
    val blocks = List(Bytes(Vector(1, 2)), Bytes(Vector(3)))
    val observer = Scan.fold[Int, Bytes](0)((count, block) => count + block.value.size).map(Count(_))
    val result = Stream.emits(blocks).covary[IO].through(observer.withInput.toPipe[IO]).compile.toVector.unsafeRunSync()
    assert(result == Vector(blocks(0), blocks(1), Count(3)))
    assert(result.collect { case block: Bytes => block }.zip(blocks).forall((a, b) => a eq b))
    val contextual = Scan.stateful1[Int, Int, Int](0)((sum, input) => (sum + input, sum + input)).first[String]
    val expected = Vector((2, "a"), (5, "b"))
    assert(Stream.emits(List((2, "a"), (3, "b"))).covary[IO].through(contextual.toPipe[IO])
      .compile.toVector.unsafeRunSync() == expected)
    println("Published scan/FS2 adapter preserved byte identity, observations and contextual state.")
