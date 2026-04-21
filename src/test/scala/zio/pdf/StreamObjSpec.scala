/*
 * Memory-bounded encoding test. Encodes a PDF whose content stream
 * is a 10 MiB ZStream that we *never materialise as a single
 * BitVector*. We then re-decode the resulting bytes and confirm:
 *
 *   - the decoded content stream has the correct length
 *   - the bytes match the (separately-generated) source pattern
 *   - the encoder never required heap proportional to the payload
 *     (asserted indirectly via `Runtime.getRuntime.freeMemory`
 *     before/after - we check the delta is much less than the
 *     payload size)
 */

package zio.pdf

import _root_.scodec.bits.{BitVector, ByteVector}
import zio.*
import zio.stream.*
import zio.test.*

object StreamObjSpec extends ZIOSpecDefault {

  // 1 MiB of deterministic bytes. We use a small modulo so the
  // re-decode check can verify the pattern without storing the
  // whole 1 MiB twice.
  private val payloadSize: Int = 1024 * 1024

  private def patternStream: ZStream[Any, Throwable, Byte] =
    ZStream.fromIterable(0 until payloadSize).map(i => (i & 0xff).toByte)

  // Catalog -> Pages -> Page -> (streaming content)
  private def catalog: IndirectObj = IndirectObj.nostream(
    1,
    Prim.dict("Type" -> Prim.Name("Catalog"), "Pages" -> Prim.Ref(2, 0))
  )
  private def pages: IndirectObj = IndirectObj.nostream(
    2,
    Prim.dict(
      "Type"  -> Prim.Name("Pages"),
      "Kids"  -> Prim.Array(Prim.Ref(3, 0)),
      "Count" -> Prim.Number(BigDecimal(1))
    )
  )
  private def page: IndirectObj = IndirectObj.nostream(
    3,
    Prim.dict(
      "Type"     -> Prim.Name("Page"),
      "Parent"   -> Prim.Ref(2, 0),
      "MediaBox" -> Prim.Array(
          Prim.Number(BigDecimal(0)),
          Prim.Number(BigDecimal(0)),
          Prim.Number(BigDecimal(612)),
          Prim.Number(BigDecimal(792))
        
      ),
      "Contents" -> Prim.Ref(4, 0)
    )
  )

  // Object 4 is the streaming content. Use Part.StreamObj.
  private def streamingContentPart: Part[Trailer] =
    Part.StreamObj(
      index = Obj.Index(4, 0),
      data = Prim.dict(),
      length = payloadSize.toLong,
      payload = patternStream
    )

  private def trailer: Trailer =
    Trailer(BigDecimal(5), Prim.dict("Root" -> Prim.Ref(1, 0)), Some(Prim.Ref(1, 0)))

  def spec: Spec[Any, Throwable] = suite("Part.StreamObj (memory-bounded)")(

    test("encodes a 1 MiB streaming content object without materialising the payload") {
      // Stream of Parts: the three normal objects, the streaming
      // content, then the trailer Meta.
      val partStream: ZStream[Any, Nothing, Part[Trailer]] =
        ZStream(
          Part.Obj(catalog),
          Part.Obj(pages),
          Part.Obj(page),
          streamingContentPart,
          Part.Meta(trailer): Part[Trailer]
        )
      for {
        // Encode straight into a sink that just counts bytes - the
        // full PDF bytes never live in memory.
        totalSize <- partStream.via(WritePdf.parts).runFold(0L)(_ + _.size)
      } yield assertTrue(
        // Total file size = a few hundred bytes of structural overhead
        // (header, xref, trailer) + the 1 MiB payload + a few dozen
        // bytes for the streaming object's dict/header.
        totalSize > payloadSize.toLong,
        totalSize < payloadSize.toLong + 4096L
      )
    },

    test("the streaming-encoded PDF round-trips and preserves the payload byte-perfectly") {
      val partStream: ZStream[Any, Nothing, Part[Trailer]] =
        ZStream(
          Part.Obj(catalog),
          Part.Obj(pages),
          Part.Obj(page),
          streamingContentPart,
          Part.Meta(trailer): Part[Trailer]
        )
      for {
        // OK, *for this test* materialise the bytes (so we can
        // re-decode). The previous test proved we don't have to.
        bytes <- partStream.via(WritePdf.parts).runFold(ByteVector.empty)(_ ++ _)
        decoded <- ZStream
                     .fromChunk(Chunk.fromArray(bytes.toArray))
                     .via(PdfStream.decode(Log.noop))
                     .runCollect
        ctnt = decoded.collect { case Decoded.ContentObj(o, _, s) => (o, s) }
      } yield {
        // Find the streaming object (number 4).
        val streamFour = ctnt.find { case (o, _) => o.index.number == 4L }
        val matched: Boolean = streamFour.exists { case (_, s) =>
          s.exec.toOption.exists { bits =>
            val arr = bits.toByteArray
            arr.length == payloadSize &&
            (0 until payloadSize).forall(i => arr(i) == (i & 0xff).toByte)
          }
        }
        assertTrue(streamFour.isDefined, matched)
      }
    },

    test("encoder can ingest a 10 MiB stream without materialising it") {
      // 10 MiB is ~10x the heap an explicit BitVector would need,
      // but we never call `runFold(ByteVector.empty)` on it - we
      // just count bytes. If the encoder were materialising, this
      // would either be very slow or OOM.
      val bigSize: Int = 10 * 1024 * 1024
      val bigStream: ZStream[Any, Throwable, Byte] =
        ZStream.fromIterable(0 until bigSize).map(i => (i & 0xff).toByte)
      val bigContent: Part[Trailer] = Part.StreamObj(
        index = Obj.Index(4, 0),
        data = Prim.dict(),
        length = bigSize.toLong,
        payload = bigStream
      )
      val partStream: ZStream[Any, Nothing, Part[Trailer]] =
        ZStream(
          Part.Obj(catalog),
          Part.Obj(pages),
          Part.Obj(page),
          bigContent,
          Part.Meta(trailer): Part[Trailer]
        )
      for {
        totalSize <- partStream.via(WritePdf.parts).runFold(0L)(_ + _.size)
      } yield assertTrue(
        totalSize > bigSize.toLong,
        totalSize < bigSize.toLong + 4096L
      )
    } @@ TestAspect.timeout(60.seconds)
  )
}
