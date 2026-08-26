package zio.pdf

import _root_.scodec.bits.ByteVector
import zio.test.*

object TiffSpec extends ZIOSpecDefault {
  def spec: Spec[Any, Nothing] = suite("Tiff")(
    test("rejects an image wrapper above its typed materialization bound before allocating") {
      val limit = ByteLimit.fromBytes(8L).toOption.get
      Tiff
        .image(Tiff(1, 1, 4, 1))(ByteVector.empty, limit)
        .either
        .map {
          case Left(Tiff.MaterializationLimitExceeded(`limit`, observed)) =>
            assertTrue(observed == Tiff.headerSize.toLong)
          case _ => assertTrue(false)
        }
    }
  )
}
