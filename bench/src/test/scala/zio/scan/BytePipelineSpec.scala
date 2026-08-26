package zio.pdf.bench.scan

import zio.*
import zio.test.*

object BytePipelineSpec extends ZIOSpecDefault {

  def spec: Spec[Any, Nothing] = suite("BytePipeline")(
    test("scanBenchFused matches hand-coded reference") {
      val bytes = Array.tabulate[Byte](4096)(i => (i & 0xff).toByte)
      val fused = BytePipeline.scanBenchFused.run(bytes)
      val ref   = BytePipeline.scanBenchFusedHandCoded(bytes)
      assertTrue(fused == ref)
    },
    test("InlineByteScan.run fuses the same workload at compile time") {
      val bytes = Array.tabulate[Byte](4096)(i => (i & 0xff).toByte)
      val fused = InlineByteScan.map(_ + 1).map(_ ^ 0x55).map(_ - 1).run(bytes)
      val ref   = BytePipeline.scanBenchFusedHandCoded(bytes)
      assertTrue(fused == ref)
    },
    test("InlineByteScan.run matches pipeline.run") {
      val bytes     = Array.tabulate[Byte](4096)(i => (i & 0xff).toByte)
      val inlineRun = InlineByteScan.map(_ + 1).map(_ ^ 0x55).map(_ - 1).run(bytes)
      val pipeline  = InlineByteScan.map(_ + 1).map(_ ^ 0x55).map(_ - 1).pipeline.run(bytes)
      assertTrue(inlineRun == pipeline)
    },
    test("runtime BytePipeline.map matches inline when stages are identical") {
      val bytes = Array.tabulate[Byte](4096)(i => (i & 0xff).toByte)
      val rt    = BytePipeline.empty.map(_ + 1).map(_ ^ 0x55).map(_ - 1).run(bytes)
      val fused = InlineByteScan.map(_ + 1).map(_ ^ 0x55).map(_ - 1).pipeline.run(bytes)
      assertTrue(rt == fused)
    },
    test("BlocksPureByteScan.runBatched matches fused pipeline") {
      val bytes   = Array.tabulate[Byte](100_000)(i => (i & 0xff).toByte)
      val direct  = BytePipeline.scanBenchFused.run(bytes)
      val batched = BlocksPureByteScan.runBatched(BytePipeline.scanBenchFused, bytes, batchSize = 8192)
      assertTrue(direct == batched)
    },
    test("BlocksPureByteScan.countBytes counts every byte") {
      val bytes = Array.fill[Byte](50_000)(42)
      assertTrue(BlocksPureByteScan.countBytes(bytes) == 50_000L)
    }
  )
}
