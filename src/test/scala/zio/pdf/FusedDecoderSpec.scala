package zio.pdf

import zio.*
import zio.prelude.Identity
import zio.test.*

object FusedDecoderSpec extends ZIOSpecDefault {

  private def sameEvent(left: Decoded, right: Decoded): Boolean =
    (left, right) match {
      case (a: Decoded.ContentObj, b: Decoded.ContentObj) =>
        a.obj == b.obj && a.rawStream == b.rawStream
      case (a, b) => a == b
    }

  private def sameTimeline(left: Chunk[Decoded], right: Chunk[Decoded]): Boolean =
    left.size == right.size && left.zip(right).forall(sameEvent)

  private def load(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val input = getClass.getResourceAsStream(s"/$name")
      require(input != null, s"$name missing")
      val bytes = input.readAllBytes()
      input.close()
      bytes
    }

  private def finish(result: FusedDecoder.Result): Either[Throwable, Chunk[Decoded]] =
    FusedDecoder
      .run(result.next, FusedDecoder.finish(enableDiagnostics = false))
      .map(tail => result.emitted ++ tail.emitted)

  def spec: Spec[Any, Throwable] = suite("FusedDecoder")(
    test("fresh cursors do not share a duplicate filter") {
      val left  = FusedDecoder.initial
      val right = FusedDecoder.initial
      assertTrue(left != right, left.parser.dupFilter ne right.parser.dupFilter)
    },
    test("ZPure chunk programs match a one-chunk decode") {
      for {
        bytes <- load("test-image.pdf")
        config = StreamingDecode.Config.default
        split  = bytes.length / 2
        direct = FusedDecoder
                   .run(FusedDecoder.initial, FusedDecoder.feed(Chunk.fromArray(bytes), config))
                   .flatMap(finish)
        chunked = FusedDecoder
                    .run(FusedDecoder.initial, FusedDecoder.feed(Chunk.fromArray(bytes.take(split)), config))
                    .flatMap { first =>
                      FusedDecoder
                        .run(first.next, FusedDecoder.feed(Chunk.fromArray(bytes.drop(split)), config))
                        .map(second => FusedDecoder.Result(second.next, first.emitted ++ second.emitted))
                    }
                    .flatMap(finish)
        matches = (direct, chunked) match {
                    case (Right(left), Right(right)) => sameTimeline(left, right)
                    case _                            => false
                  }
      } yield assertTrue(
        direct.isRight,
        chunked.isRight,
        matches
      )
    },
    test("checkpoint resume is isolated from later mutation of the live cursor") {
      for {
        bytes <- load("test-image.pdf")
        config = StreamingDecode.Config.default
        split  = bytes.length / 2
        prefix = Chunk.fromArray(bytes.take(split))
        suffix = Chunk.fromArray(bytes.drop(split))
        first  = FusedDecoder.run(FusedDecoder.initial, FusedDecoder.feed(prefix, config))
        resumed = first.flatMap { result =>
                    val saved = FusedDecoder.checkpoint(result.next, split.toLong, config)
                    for {
                      liveTail <- FusedDecoder.run(result.next, FusedDecoder.feed(suffix, config)).flatMap(finish)
                      restored <- FusedDecoder.restore(saved, config)
                      mismatch  = FusedDecoder.restore(saved, StreamingDecode.Config(config.inlineMaxBytes + 1L)).isLeft
                      resumedTail <- FusedDecoder
                                       .run(restored, FusedDecoder.feed(suffix, config))
                                       .flatMap(finish)
                    } yield (saved, liveTail, resumedTail, mismatch)
                  }
      } yield assertTrue(
        resumed.exists { case (saved, liveTail, resumedTail, mismatch) =>
          saved.nextByteOffset == split.toLong && mismatch && sameTimeline(liveTail, resumedTail)
        }
      )
    },
    test("ordered segments form a monoid and advance from a checkpoint") {
      for {
        bytes <- load("test-image.pdf")
        config = StreamingDecode.Config.default
        plan   = FusedDecoder.plan(config)
        first  = bytes.length / 3
        second = first * 2
        a      = plan.fromChunk(Chunk.fromArray(bytes.take(first)))
        b      = plan.fromChunk(Chunk.fromArray(bytes.slice(first, second)))
        c      = plan.fromChunk(Chunk.fromArray(bytes.drop(second)))
        empty  = Identity[plan.Segment].identity
        initial = FusedDecoder.checkpoint(FusedDecoder.initial, 0L, config)
        left   = plan.advance(initial, (a ++ b) ++ c)
        right  = plan.advance(initial, a ++ (b ++ c))
        withIdentity = plan.advance(initial, empty ++ a ++ b ++ c)
        associative = (left, right) match {
                        case (Right((leftCheckpoint, leftOut)), Right((rightCheckpoint, rightOut))) =>
                          leftCheckpoint.nextByteOffset == rightCheckpoint.nextByteOffset && sameTimeline(leftOut, rightOut)
                        case _ => false
                      }
        identityLaw = (left, withIdentity) match {
                        case (Right((_, leftOut)), Right((_, identityOut))) => sameTimeline(leftOut, identityOut)
                        case _                                               => false
                      }
      } yield assertTrue(
        left.isRight,
        right.isRight,
        withIdentity.isRight,
        associative,
        identityLaw
      )
    }
  )
}
