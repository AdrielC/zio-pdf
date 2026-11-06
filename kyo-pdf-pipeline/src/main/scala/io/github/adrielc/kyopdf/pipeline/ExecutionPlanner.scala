package io.github.adrielc.kyopdf.pipeline

import java.io.{InputStream, ByteArrayInputStream, OutputStream}
import java.nio.file.{Files, Path}
import kyo.*
import StepRegistry.*
import PdfPipeline.Op

/** Resource decisions are host policy, never recipe-supplied filesystem paths. */
object ExecutionPlanner:
  final val MaxMemoryBytes = 1024 * 1024
  final val MaxSpoolBytes = 1024L * 1024 * 1024
  final val MaxChunkBytes = 64 * 1024
  final val MaxItems = 1024
  final val MaxChunks = 65536
  final case class Storage(memoryBytes: Int = MaxMemoryBytes, spoolBytes: Long = MaxSpoolBytes)
  enum Strategy:
    case BoundedMemory, DiskSpool
  final case class Stage(node: String, strategy: Strategy, reason: String)
  final case class Plan(stages: Vector[Stage], fusedScans: Vector[Vector[String]])

  /** Only total, pure byte probes fuse. Rewrites, validation, and external work
    * retain their ordering. A group shares a source and one bounded Scan runner.
    */
  private[pipeline] def scanGroups(plan: PdfPipeline.Checked): Map[String, Vector[(String, Call)]] =
    val nodes = plan.order.collect { case PdfPipeline.Node(id, Op.Registered(call), inputs)
        if call.inputs == Vector(PdfPipeline.Kind.File) && plan.registry.find(call.operation, call.version).exists(_.probe(call.settings).nonEmpty) =>
      (id, call, inputs.head)
    }
    nodes.groupBy(_._3).valuesIterator.flatMap { grouped =>
      val group = grouped.map((id, call, _) => id -> call)
      group.map((id, _) => id -> group)
    }.toMap

  def explain(plan: PdfPipeline.Checked, inputBytes: Long, storage: Storage = Storage()): Plan =
    val stages = plan.order.collect { case PdfPipeline.Node(id, Op.Registered(call), _) =>
      val entry = plan.registry.find(call.operation, call.version).get
      val strategy = if inputBytes <= storage.memoryBytes then Strategy.BoundedMemory else Strategy.DiskSpool
      Stage(id, strategy, s"${entry.access}; final outputs remain provisional until every demanded step succeeds")
    }
    Plan(stages, scanGroups(plan).values.map(_.map(_._1)).toVector.distinct)

  /** A staged source is valid only inside withStaged. No recipe can name its path.
    * Source chunks are bounded before copying; disk quota is checked before write.
    */
  sealed abstract class Staged(val size: Long, val strategy: Strategy):
    private var live = true
    protected def openSource: InputStream
    private[pipeline] def close(): Unit = { live = false }
    final def withInput[A, E: ConcreteTag, S](use: InputStream => A < (Abort[E] & S)): A < (S & Abort[E] & Abort[Error] & Sync) = Sync.defer {
      if !live then Abort.fail(Error(ErrorCode.InvalidInput, "Staged source has left its scope"))
      else io(openSource).map { stream =>
        SyncResource.ensure[E](Abort.catching[java.io.IOException](Sync.defer(stream.close())))(use(stream))
      }
    }
  private final class Memory(private var bytes: Array[Byte], length: Int) extends Staged(length.toLong, Strategy.BoundedMemory):
    protected def openSource = new ByteArrayInputStream(bytes, 0, length)
    override private[pipeline] def close(): Unit =
      super.close()
      bytes = Array.emptyByteArray
  private final class Spool(path: Path, length: Long) extends Staged(length, Strategy.DiskSpool):
    protected def openSource = Files.newInputStream(path)
  private final class Workspace(val path: Path):
    var output: Option[OutputStream] = None
    def close(): Unit =
      output.foreach(_.close())
      Files.deleteIfExists(path)
      ()

  private def io[A](body: => A): A < (Abort[Error] & Sync) =
    Abort.run[java.io.IOException](Abort.catching[java.io.IOException](Sync.defer(body))).map {
      case Result.Success(value) => value
      case _ => Abort.fail(Error(ErrorCode.Backend, "Staging I/O failed"))
    }

  def withStaged[A, E: ConcreteTag, S](chunks: Iterator[Array[Byte]], storage: Storage = Storage())(use: Staged => A < (Abort[E] & S)): A < (S & Abort[E] & Abort[Error] & Sync) =
    if storage.memoryBytes < 0 || storage.memoryBytes > MaxMemoryBytes || storage.spoolBytes < storage.memoryBytes || storage.spoolBytes > MaxSpoolBytes then
      Abort.fail(Error(ErrorCode.InvalidInput, "Invalid staging budget"))
    else io(new Workspace(Files.createTempFile("kyo-stage-", ".bin"))).map { workspace =>
      SyncResource.ensure[Error | E](Abort.catching[java.io.IOException](Sync.defer(workspace.close()))) {
        var total = 0L
        var chunksSeen = 0
        // Fixed capacity: a growing buffer can overshoot the advertised cap.
        var memory = new Array[Byte](storage.memoryBytes)
        var memorySize = 0
        def consume(): Staged < (Abort[Error] & Sync) =
          if !chunks.hasNext then io {
            workspace.output match
              case None =>
                val owned = memory
                memory = Array.emptyByteArray
                new Memory(owned, memorySize)
              case Some(stream) => stream.close(); workspace.output = None; new Spool(workspace.path, total)
          }
          else if chunksSeen >= MaxChunks then Abort.fail(Error(ErrorCode.Limit, "Source chunk budget exceeded"))
          else
            val chunk = chunks.next()
            chunksSeen += 1
            if chunk.length > MaxChunkBytes then Abort.fail(Error(ErrorCode.Limit, "Source chunks must be at most 64 KiB"))
            else if chunk.length.toLong > storage.spoolBytes - total then Abort.fail(Error(ErrorCode.Limit, "Spool quota exceeded"))
            else io {
              if workspace.output.isEmpty && total + chunk.length > storage.memoryBytes then
                val stream = Files.newOutputStream(workspace.path)
                workspace.output = Some(stream)
                stream.write(memory, 0, memorySize)
                memory = Array.emptyByteArray
                memorySize = 0
              workspace.output match
                case Some(stream) => stream.write(chunk)
                case None =>
                  java.lang.System.arraycopy(chunk, 0, memory, memorySize, chunk.length)
                  memorySize += chunk.length
              total += chunk.length
            }.map(_ => consume())
        consume().map(stage => SyncResource.ensure[E](Sync.defer(stage.close()))(use(stage)))
      }
    }

  final case class Item[A](index: Int, result: Either[Error, A])
  /** Sequential bounded traversal. Recoverable errors are retained as flags;
    * hard errors stop before pulling the next source item. The caller supplies
    * a size measure for its schema, capped at 1 MiB total retained results.
    */
  def mapBounded[A, B](items: Iterator[A], maximum: Int, recoverAndFlag: Boolean = false)(
      measure: B => Long)(step: A => B < (Abort[Error] & Sync)): Vector[Item[B]] < (Abort[Error] & Sync) =
    if maximum < 0 || maximum > MaxItems then Abort.fail(Error(ErrorCode.InvalidInput, "Invalid collection bound"))
    else
      def loop(index: Int, bytes: Long, results: Vector[Item[B]]): Vector[Item[B]] < (Abort[Error] & Sync) =
        if !items.hasNext then results
        else if index == maximum then Abort.fail(Error(ErrorCode.Limit, "Collection bound exceeded"))
        else Abort.run[Error](step(items.next())).map {
          case Result.Success(value) =>
            val size = measure(value)
            if size < 0 || size > MaxMemoryBytes - bytes then Abort.fail(Error(ErrorCode.Limit, "Collection result byte budget exceeded"))
            else loop(index + 1, bytes + size, results :+ Item(index, Right(value)))
          case Result.Failure(error) if recoverAndFlag && error.recoverable && Set(ErrorCode.Unavailable, ErrorCode.Backend)(error.code) =>
            val bounded = error.copy(message = error.message.take(300))
            val size = bounded.message.length.toLong * 2
            if size > MaxMemoryBytes - bytes then Abort.fail(Error(ErrorCode.Limit, "Collection flag byte budget exceeded"))
            else loop(index + 1, bytes + size, results :+ Item(index, Left(bounded)))
          case Result.Failure(error) => Abort.fail(error)
          case Result.Panic(_) => Abort.fail(Error(ErrorCode.Backend, "Collection step defect"))
        }
      loop(0, 0, Vector.empty)
