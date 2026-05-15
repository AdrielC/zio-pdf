/*
 * kyo-zio: lift `ZStream` into Kyo `Stream` via [[kyo.ZStreams.get]], then collect with `Stream.run`.
 * `ZStreams.get` leaves `Async` in the row (ZIO pull); interpret it with [[kyo.ZIOs.run]] inside ZIO tests.
 */

package zio.pdf

import kyo.{Abort, Result as KResult, *}
import kyo.{ZIOs, ZStreams}
import zio.stream.ZStream
import zio.test.*

object KyoZioStreamsSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Any] =
    suite("kyo-zio ZStreams.get")(
      test("lifts ZStream into Kyo Stream and collects the same elements") {
        val zs        = ZStream.fromIterable(Seq(1, 2, 3, 4, 5))
        val kyoStream = ZStreams.get(zs)
        for
          r <- ZIOs.run(Abort.run(kyoStream.run))
        yield assertTrue(r == KResult.succeed(Chunk(1, 2, 3, 4, 5)))
      },
      test("propagates typed failure as Abort") {
        val zs        = ZStream.fail("boom")
        val kyoStream = ZStreams.get(zs)
        for
          r <- ZIOs.run(Abort.run(kyoStream.run))
        yield assertTrue(r == KResult.fail("boom"))
      }
    )
}
