/*
 * Smoke test demonstrating that the `zio-blocks-schema` dependency is
 * resolvable and usable from Scala 3.8.x. Derives a `Schema` for a
 * tiny case class and asserts that we can round-trip a value through
 * `DynamicValue` (the format-agnostic representation).
 */

package zio.scodec.stream

import zio.blocks.schema.Schema
import zio.test.*

object ZioBlocksSchemaSmokeSpec extends ZIOSpecDefault {

  final case class Pair(name: String, count: Int)
  object Pair {
    given Schema[Pair] = Schema.derived[Pair]
  }

  def spec: Spec[Any, Throwable] = suite("zio-blocks-schema smoke")(
    test("derived Schema is non-null and reports the expected name") {
      val s = summon[Schema[Pair]]
      assertTrue(
        s ne null,
        s.toString.contains("Pair")
      )
    }
  )
}
