package zio.pdf

import zio.*
import zio.stream.ZStream
import zio.test.*

object RewriteSpec extends ZIOSpecDefault {

  private val trailer = Trailer(BigDecimal(12), Prim.dict("Root" -> Prim.Ref(1L, 0)), Some(Prim.Ref(1L, 0)))

  private def obj(number: Long): Part[Trailer] =
    Part.Obj(IndirectObj.nostream(number, Prim.dict("Type" -> Prim.Name("Example"))))

  private val collect: Rewrite.Collect[Int, Int] = state => value => {
    val nextTrailer = if value == 2 then Some(trailer) else state.trailer
    Right(Rewrite.Emission(Chunk(obj(value.toLong)), state.copy(state = state.state + value, trailer = nextTrailer)))
  }

  def spec: Spec[Any, Throwable] = suite("Rewrite")(
    test("full rewrite emits multiple final parts after its trailer") {
      val update: Rewrite.Update[Int] = _ =>
        Right(Chunk(obj(10L), obj(11L)))

      ZStream(1, 2).via(Rewrite.parts(0)(collect)(update)).runCollect.map { parts =>
        assertTrue(
          parts == Chunk(
            obj(1L),
            obj(2L),
            Part.Meta(trailer): Part[Trailer],
            obj(10L),
            obj(11L)
          )
        )
      }
    },
    test("forState reuses collection semantics without encoding a PDF") {
      ZStream(1, 2).via(Rewrite.forState(0)(collect)).runCollect.map { states =>
        assertTrue(states == Chunk(3))
      }
    },
    test("simpleParts retains the ZPure state, log, and failure contract") {
      ZStream(1, 2)
        .via(
          Rewrite.simpleParts[Int, Int](0) { state => value =>
            val nextTrailer = if value == 2 then Some(trailer) else state.trailer
            (List(obj(value.toLong)), state.copy(state = state.state + value, trailer = nextTrailer))
          } { completed =>
            obj(completed.state.toLong + 10L)
          }
        )
        .runCollect
        .map { parts =>
          assertTrue(
            parts == Chunk(
              obj(1L),
              obj(2L),
              Part.Meta(trailer): Part[Trailer],
              obj(13L)
            )
          )
        }
    }
  )
}
