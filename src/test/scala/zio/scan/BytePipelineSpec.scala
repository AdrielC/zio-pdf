package zio.scan

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
    test("ZPureByteScan.runFast matches fused pipeline") {
      val bytes = Array.tabulate[Byte](100_000)(i => (i & 0xff).toByte)
      val direct = BytePipeline.scanBenchFused.run(bytes)
      val fast   = ZPureByteScan.runFast(BytePipeline.scanBenchFused, bytes)
      assertTrue(direct == fast)
    },
    test("ZPureByteScan.runBatched matches fused pipeline") {
      val bytes   = Array.tabulate[Byte](100_000)(i => (i & 0xff).toByte)
      val direct  = BytePipeline.scanBenchFused.run(bytes)
      val batched = ZPureByteScan.runBatched(BytePipeline.scanBenchFused, bytes, batchSize = 8192)
      assertTrue(direct == batched)
    },
    test("ZPureByteScan.countBytes counts every byte") {
      val bytes = Array.fill[Byte](50_000)(42)
      assertTrue(ZPureByteScan.countBytes(bytes) == 50_000L)
    }
  )
}
