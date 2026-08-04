package zio.pdf.pipe

import zio.*
import zio.pdf.{Decoded, PdfEngine, PdfHyperdrive, StreamingDecode}
import zio.test.*

object PipeSpec extends ZIOSpecDefault {

  private def loadFixture(name: String): ZIO[Any, Throwable, Array[Byte]] =
    ZIO.attemptBlocking {
      val is = getClass.getResourceAsStream(s"/$name")
      require(is != null, s"$name missing from test resources")
      val buf = is.readAllBytes()
      is.close()
      buf
    }

  private def withTempPdf[R](name: String)(use: java.nio.file.Path => ZIO[R, Throwable, TestResult]) =
    for {
      bytes  <- loadFixture(name)
      path   <- ZIO.attemptBlocking {
                  val p = java.nio.file.Files.createTempFile("pipe-", ".pdf")
                  java.nio.file.Files.write(p, bytes)
                  p
                }
      result <- use(path).ensuring(ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path)).ignore)
    } yield result

  def spec: Spec[Any, Throwable] = suite("Pipe")(
    suite("composition laws")(
      test(">>> associates") {
        val f      = Pipe[Int, Int](_ + 1)
        val g      = Pipe[Int, Int](_ * 2)
        val h      = Pipe[Int, Int](_ - 3)
        val inputs = (0 until 32).toList
        val lhs    = Pipe.comp { (x: Int) => h.run(g.run(f.run(x))) }
        val rhs    = (f >>> g) >>> h
        assertTrue(inputs.map(lhs.run) == inputs.map(rhs.run))
      },
      test("par distributes independently") {
        val double = Pipe[Int, Int](_ * 2)
        val plus   = Pipe[Int, Int](_ + 10)
        val fused  = Pipe.par(double, plus)
        val inputs = (0 until 16).toList
        assertTrue(inputs.map(i => fused.run((i, i))) == inputs.map(i => (i * 2, i + 10)))
      },
      test("fanOut matches independent runs") {
        val f      = Pipe[Int, Int](_ + 1)
        val g      = Pipe[Int, String](i => s"$i")
        val inputs = (0 until 8).toList
        assertTrue(inputs.map(Pipe.fanOut(f, g).run) == inputs.map(i => (f.run(i), g.run(i))))
      },
      test("<> and &&& are fanOut") {
        val f = Pipe[Int, Int](_ + 1)
        val g = Pipe[Int, String](i => s"$i")
        assertTrue((f <> g).run(4) == (f &&& g).run(4), (f <> g).run(4) == Pipe.fanOut(f, g).run(4))
      },
      test("stagedDecodeAndDigest matches fused digest on fixture") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          slice  = FusedDecode.Slice(bytes, 0, bytes.length)
          staged = IngestPipeline.stagedDecodeAndDigest().run(slice)
          fused  = IngestPipeline.fusedDecodeAndDigest(slice, FusedDecode.Cfg())
        } yield assertTrue(staged.decoded == fused.decoded, staged.digest.sameElements(fused.digest))
      }
    ),
    suite("fused decode parity")(
      test("FusedDecode.decodeBytes matches PdfHyperdrive.decodeSync on xref-stream.pdf") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          fused  = FusedDecode.decodeBytes(bytes)
          hyper  = PdfHyperdrive.decodeSync(bytes)
        } yield assertTrue(fused == hyper)
      },
      test("streaming slice matches decodeStreamingSync") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          cfg    = FusedDecode.Cfg(config = StreamingDecode.Config.default)
          slice  = FusedDecode.Slice(bytes, 0, bytes.length)
          fused  = FusedDecode.decodeStreamingSlice(slice, cfg)
          hyper  = PdfHyperdrive.decodeStreamingSync(bytes)
        } yield assertTrue(fused == hyper)
      },
      test("FusedElements matches elementsStagedSync on xref-stream.pdf") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          fused  = FusedElements.decodeBytes(bytes)
          staged = PdfHyperdrive.elementsStagedSync(bytes)
        } yield assertTrue(fused == staged)
      },
      test("IngestPipeline fused digest matches ByteDigest on fixture") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          slice  = FusedDecode.Slice(bytes, 0, bytes.length)
          cfg    = FusedDecode.Cfg()
          ingest = IngestPipeline.fusedDecodeAndDigest(slice, cfg)
          direct = ByteDigest.digestSlice(slice)
        } yield assertTrue(ingest.decoded == PdfHyperdrive.decodeSync(bytes), ingest.digest.sameElements(direct))
      },
      test("HyperFuse elements matches elementsFusedSync") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          slice  = FusedDecode.Slice(bytes, 0, bytes.length)
          cfg    = FusedDecode.Cfg()
          fused  = FusedElements.decodeSlice(slice, cfg)
          hyper  = PdfHyperdrive.elementsFusedSync(bytes)
        } yield assertTrue(fused == hyper)
      },
      test("decodeSliceSink matches decodeSlice on xref-stream.pdf") {
        for {
          bytes   <- loadFixture("xref-stream.pdf")
          slice    = FusedDecode.Slice(bytes, 0, bytes.length)
          cfg      = FusedDecode.Cfg()
          builder  = Chunk.newBuilder[Decoded]
          count    = FusedDecode.decodeSliceSink(slice, cfg)(d => builder += d)
          fused    = builder.result()
          direct   = FusedDecode.decodeSlice(slice, cfg)
        } yield assertTrue(count == direct.size.toLong, fused == direct)
      },
      test("decodeFromPathSink matches decodeSync via temp file") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            bytes   <- loadFixture("xref-stream.pdf")
            builder  = Chunk.newBuilder[Decoded]
            count    = PdfHyperdrive.decodeFromPathSink(path)(d => builder += d)
            sunk     = builder.result()
            direct   = PdfHyperdrive.decodeSync(bytes)
          } yield assertTrue(count == direct.size.toLong, sunk == direct)
        }
      },
      test("PdfEngine.sink matches PdfEngine.decode on xref-stream.pdf") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            builder <- ZIO.succeed(Chunk.newBuilder[Decoded])
            count   <- PdfEngine.sink(path)(d => builder += d)
            sunk     = builder.result()
            decoded <- PdfEngine.decode(path)
          } yield assertTrue(count == decoded.size.toLong, sunk == decoded)
        }.provide(PdfEngine.live)
      },
      test("PdfEngine.stream matches PdfEngine.decode on xref-stream.pdf") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            streamed <- PdfEngine.stream(path).runCollect
            decoded  <- PdfEngine.decode(path)
          } yield assertTrue(streamed == decoded)
        }.provide(PdfEngine.live)
      },
      test("fusedDecodeAndDigestSink matches fusedDecodeAndDigest on fixture") {
        for {
          bytes   <- loadFixture("xref-stream.pdf")
          slice    = FusedDecode.Slice(bytes, 0, bytes.length)
          cfg      = FusedDecode.Cfg()
          builder  = Chunk.newBuilder[Decoded]
          sunk     = IngestPipeline.fusedDecodeAndDigestSink(slice, cfg)(d => builder += d)
          direct   = IngestPipeline.fusedDecodeAndDigest(slice, cfg)
          digest   = ByteDigest.digestSlice(slice, cfg)
        } yield assertTrue(
          sunk.count == direct.decoded.size.toLong,
          builder.result() == direct.decoded,
          sunk.digest.sameElements(direct.digest),
          sunk.digest.sameElements(digest)
        )
      },
      test("decodeAndDigestFromPathSink matches PdfEngine.decodeAndDigest") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            builder           <- ZIO.succeed(Chunk.newBuilder[Decoded])
            (count, digArr)    = PdfHyperdrive.decodeAndDigestFromPathSink(path)(d => builder += d)
            sunk               = builder.result()
            (direct, digChunk) <- PdfEngine.decodeAndDigest(path)
          } yield assertTrue(
            count == direct.size.toLong,
            sunk == direct,
            digChunk.toArray.sameElements(digArr)
          )
        }.provide(PdfEngine.live)
      },
      test("PdfEngine.digest matches decodeAndDigest digest on fixture") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            bytes      <- loadFixture("xref-stream.pdf")
            digestOnly <- PdfEngine.digest(path)
            (_, fused) <- PdfEngine.decodeAndDigest(path)
            syncDigest  = PdfHyperdrive.digestSync(bytes)
          } yield assertTrue(
            digestOnly == fused,
            digestOnly.toArray.sameElements(syncDigest)
          )
        }.provide(PdfEngine.live)
      },
      test("PdfEngine.validate matches ValidatePdf.fromDecoded(stream)") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            eng    <- ZIO.service[PdfEngine]
            engine <- eng.validate(path)
            direct <- zio.pdf.ValidatePdf.fromDecoded(eng.stream(path))
          } yield assertTrue(engine == direct)
        }.provide(PdfEngine.live)
      },
      test("PdfEngine.stream.runCollect matches PdfEngine.decode") {
        withTempPdf("xref-stream.pdf") { path =>
          for {
            streamed <- PdfEngine.stream(path).runCollect
            decoded  <- PdfEngine.decode(path)
          } yield assertTrue(streamed == decoded)
        }.provide(PdfEngine.live)
      }
    )
  )
}
