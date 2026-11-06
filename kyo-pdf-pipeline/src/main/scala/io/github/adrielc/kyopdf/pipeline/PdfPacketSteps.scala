package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import java.io.ByteArrayOutputStream
import kyo.*
import zio.blocks.schema.{Schema as BlocksSchema}
import StepRegistry.*

/** Ordinary registered arrows. Tool outputs remain staged/fail-closed, while
  * PdfPacket's pull API and stream adapters also offer direct live delivery.
  */
object PdfPacketSteps:
  final case class Options(title: String = "Combined documents", titles: Vector[String] = Vector.empty)
  private val parameters = new DataSchema("pdf.packet.settings", 1, BlocksSchema.derived[Options])
  private def step[A](id: String, in: Port[A], count: Int)(files: A => Vector[PdfFile]): Step[A, PdfFile, Options] =
    new Step(id, 1, in, Port.file, parameters, Vector(Options()), StepRegistry.Access.RandomAccess, Set(Capability.Rewrite), p =>
      Option.when(!PdfPacket.validLabel(p.title))(PdfPipeline.Issue("settings.title", PdfPipeline.Code.Setting,
        "Use a nonempty cover title up to 120 WinAnsi characters")).toVector ++
      Option.when(p.titles.nonEmpty && p.titles.size != count)(PdfPipeline.Issue("settings.titles", PdfPipeline.Code.Setting,
        s"Supply exactly $count document titles or omit titles")).toVector ++
      p.titles.zipWithIndex.collect { case (title, i) if !PdfPacket.validLabel(title) =>
        PdfPipeline.Issue(s"settings.titles[$i]", PdfPipeline.Code.Setting, "Use a nonempty document title up to 120 WinAnsi characters")
      }, description = s"Combine $count Files in input order with a generated cover, clickable contents and bookmarks. titles names the inputs in the same order. Each source is consumed once. Output is exposed only after completion; source forms require flattening and source tags/outlines are replaced.")(
      (input, p, _) => combine(files(input), p))
  val two = step("pdf.packet-2", Port.pair(Port.file, Port.file), 2)(value => Vector(value._1, value._2))
  val three = step("pdf.packet-3", Port.pair(Port.pair(Port.file, Port.file), Port.file), 3)(value => Vector(value._1._1, value._1._2, value._2))
  val four = step("pdf.packet-4", Port.pair(Port.pair(Port.pair(Port.file, Port.file), Port.file), Port.file), 4)(
    value => Vector(value._1._1._1, value._1._1._2, value._1._2, value._2))

  private def decode(file: PdfFile): PdfDocument < (Abort[PdfError] & Sync) =
    ByteLimit.fromBytes(PdfFile.MaxBytes).map { limit =>
      val reader = PdfRead.open(limit)
      SyncResource.ensure[PdfError](Sync.defer(reader.close())) {
        Sync.defer {
          var position = 0
          var failure: Option[PdfError] = None
          while position < file.bytes.length && failure.isEmpty do
            val start = position
            val size = math.min(PdfPacket.ChunkBytes, file.bytes.length - position)
            reader.offer(size)(i => file.bytes(start + i)) match
              case Left(error) => failure = Some(error)
              case Right(_) => position += size
          Abort.get(failure.toLeft(())).map(_ => Abort.get(reader.decode(RetentionLimits(maxPayloadBytes = PdfFile.MaxBytes))))
        }
      }
    }
  private def combine(files: Vector[PdfFile], options: Options): PdfFile < (Abort[Error] & Sync) =
    if files.exists(_.size > PdfFile.MaxBytes) then Abort.fail(Error(ErrorCode.Limit, "Packet inputs are at most 64 MiB each"))
    else
      val work = PdfPacket.open(PdfPacket.Settings(options.title), PdfPacket.Bounds(outputBytes = PdfFile.MaxBytes, memoryBytes = 256L * 1024 * 1024)).map { session =>
        SyncResource.ensure[PdfError](Sync.defer(session.close())) {
          val out = new ByteArrayOutputStream(PdfPacket.ChunkBytes)
          def consume(pull: session.Pull): Unit < (Abort[PdfError] & Sync) = pull.read().map {
            case None => ()
            case Some(block) => Sync.defer(out.write(block.array, 0, block.size)).map(_ => consume(pull))
          }
          def loop(index: Int): Vector[Byte] < (Abort[PdfError] & Sync) =
            if index == files.size then session.finish().map(pull => consume(pull).map(_ => out.toByteArray.toVector))
            else decode(files(index)).map { doc =>
              val title = options.titles.lift(index).getOrElse(s"Document ${index + 1}")
              session.append(title, doc).map(pull => consume(pull).map(_ => loop(index + 1)))
            }
          loop(0)
        }
      }
      Abort.run[PdfError](work).map {
        case Result.Success(bytes) => PdfFile.checked(bytes).fold(_ => Abort.fail(Error(ErrorCode.Verification, "Invalid packet output")), identity)
        case Result.Failure(error: PdfError.TooLarge) => Abort.fail(Error(ErrorCode.Limit, error.message))
        case Result.Failure(error: PdfError.RetentionExceeded) => Abort.fail(Error(ErrorCode.Limit, error.message))
        case Result.Failure(error) => Abort.fail(Error(ErrorCode.InvalidInput, error.message))
        case Result.Panic(_) => Abort.fail(Error(ErrorCode.Backend, "Packet stitching failed"))
      }
