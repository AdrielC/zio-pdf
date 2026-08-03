package zio.pdf.pipe

import zio.*
import zio.pdf.{Decoded, PdfHyperdrive, PdfStream, StreamingDecode}
import zio.pdf.io.PdfIO
import zio.stream.ZStream
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

  def spec: Spec[Any, Throwable] = suite("Pipe")(
    suite("composition laws")(
      test(">>> associates") {
        val f = Pipe[Int, Int](_ + 1)
        val g = Pipe[Int, Int](_ * 2)
        val h = Pipe[Int, Int](_ - 3)
        val inputs = (0 until 32).toList
        val lhs = Pipe.comp { (x: Int) => h.run(g.run(f.run(x))) }
        val rhs = (f >>> g) >>> h
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
        val f = Pipe[Int, Int](_ + 1)
        val g = Pipe[Int, String](i => s"$i")
        val inputs = (0 until 8).toList
        assertTrue(inputs.map(Pipe.fanOut(f, g).run) == inputs.map(i => (f.run(i), g.run(i))))
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
          bytes <- loadFixture("xref-stream.pdf")
          slice  = FusedDecode.Slice(bytes, 0, bytes.length)
          cfg    = FusedDecode.Cfg()
          builder = Chunk.newBuilder[Decoded]
          count   = FusedDecode.decodeSliceSink(slice, cfg)(d => builder += d)
          fused   = builder.result()
          direct  = FusedDecode.decodeSlice(slice, cfg)
        } yield assertTrue(count == direct.size.toLong, fused == direct)
      },
      test("decodeFromPathSink matches decodeSync via temp file") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("sink-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          builder = Chunk.newBuilder[Decoded]
          count   = PdfHyperdrive.decodeFromPathSink(path)(d => builder += d)
          sunk    = builder.result()
          direct  = PdfHyperdrive.decodeSync(bytes)
          _       <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(count == direct.size.toLong, sunk == direct)
      },
      test("warpStreaming matches warp on xref-stream.pdf") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("warp-stream-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          collected <- Ref.make(List.empty[Decoded])
          count     <- PdfIO.warpStreaming(path)(d => collected.update(_ :+ d))
          sunk      <- collected.get.map(Chunk.fromIterable(_))
          warp      <- PdfIO.warp(path)
          _         <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(count == warp.size.toLong, sunk == warp)
      },
      test("warpStream matches warp on xref-stream.pdf") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("warp-zstream-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          streamed <- PdfIO.warpStream(path).runCollect
          warp     <- PdfIO.warp(path)
          _        <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(streamed == warp)
      },
      test("fusedDecodeAndDigestSink matches fusedDecodeAndDigest on fixture") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          slice  = FusedDecode.Slice(bytes, 0, bytes.length)
          cfg    = FusedDecode.Cfg()
          builder = Chunk.newBuilder[Decoded]
          sunk    = IngestPipeline.fusedDecodeAndDigestSink(slice, cfg)(d => builder += d)
          direct  = IngestPipeline.fusedDecodeAndDigest(slice, cfg)
          digest  = ByteDigest.digestSlice(slice, cfg)
        } yield assertTrue(
          sunk.count == direct.decoded.size.toLong,
          builder.result() == direct.decoded,
          sunk.digest.sameElements(direct.digest),
          sunk.digest.sameElements(digest)
        )
      },
      test("decodeAndDigestStreaming matches decodeAndDigest on xref-stream.pdf") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("digest-stream-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          collected <- Ref.make(List.empty[Decoded])
          (count, digest) <- PdfIO.decodeAndDigestStreaming(path)(d => collected.update(_ :+ d))
          sunk          <- collected.get.map(Chunk.fromIterable(_))
          (direct, dig) <- PdfIO.decodeAndDigest(path)
          _             <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(count == direct.size.toLong, sunk == direct, digest.sameElements(dig))
      },
      test("digest matches decodeAndDigest digest on fixture") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("digest-only-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          digestOnly <- PdfIO.digest(path)
          (_, fused) <- PdfIO.decodeAndDigest(path)
          syncDigest  = PdfHyperdrive.digestSync(bytes)
          _          <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(digestOnly.sameElements(fused), digestOnly.sameElements(syncDigest))
      },
      test("validateHyperdrive matches validate on xref-stream.pdf") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("validate-hd-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          hyper   <- PdfIO.validateHyperdrive(path)
          default <- PdfIO.validate(path)
          _       <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(hyper == default)
      },
      test("decodeStream matches warp for files above hyperdrive threshold") {
        for {
          bytes <- loadFixture("xref-stream.pdf")
          path  <- ZIO.attemptBlocking {
            val p = java.nio.file.Files.createTempFile("decode-stream-", ".pdf")
            java.nio.file.Files.write(p, bytes)
            p
          }
          // Force stream path by setting threshold below file size
          decoded <- PdfIO.decodeDecoded(path, hyperdriveThreshold = 0L)
          warp    <- PdfIO.warp(path)
          _       <- ZIO.attemptBlocking(java.nio.file.Files.deleteIfExists(path))
        } yield assertTrue(decoded == warp)
      }
    )
  )
}
