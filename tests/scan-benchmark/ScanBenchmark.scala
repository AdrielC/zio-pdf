package io.github.adrielc.scan

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import _root_.fs2.{Chunk, Stream, Scan as FsScan}
import io.github.adrielc.scan.fs2.ScanFs2.*
import zio.test.*

/** Paired comparison of equivalent bounded synthetic workloads; not a CI speed gate. */
object ScanBenchmark extends ZIOSpecDefault:
  private var sink = 0L
  private val inputs = Vector.tabulate(32768)(_ % 256)
  private val chunk = Chunk.from(inputs)
  private val sum = Scan.statefulLong[Int, Long](0L)((state, value, emit) => { val next = state + value; emit(next); next })
  private val fsSum = FsScan.stateful1[Long, Int, Long](0L)((state, value) => (state + value, state + value))
  private def ours[I, O](scan: Scan[I, O], data: Vector[I], hash: O => Long): () => Long =
    val compiled = scan.compile().toOption.get
    () =>
      var total = 0L
      val run = compiled.open(value => total += hash(value))
      val fed = run.feed(emit => data.foreach(emit(_)))
      val done = run.finish()
      scala.Predef.assert(fed.isRight && done.isRight)
      total
  private def theirs[S, I, O](scan: FsScan[S, I, O], data: Vector[I], hash: O => Long): () => Long = () =>
    var state = scan.initial
    var total = 0L
    data.foreach { value =>
      val (next, outputs) = scan.transform(state, value)
      state = next
      outputs.foreach(output => total += hash(output))
    }
    scan.onComplete(state).foreach(output => total += hash(output))
    total
  private def compare(name: String, countAllocation: Boolean = true)(reference: () => Long, candidate: () => Long): Unit =
    scala.Predef.assert(reference() == candidate())
    (0 until 15).foreach { _ => sink ^= reference(); sink ^= candidate() }
    def measure(f: () => Long): (Long, Long) =
      val allocated = if countAllocation then Allocation.bytes() else None
      val start = System.nanoTime()
      sink ^= f()
      val elapsed = System.nanoTime() - start
      (elapsed, Allocation.bytes().zip(allocated).map((after, before) => after - before).getOrElse(-1L))
    val samples = (0 until 21).map { index =>
      if index % 2 == 0 then
        val old = measure(reference)
        val fresh = measure(candidate)
        (old, fresh)
      else
        val fresh = measure(candidate)
        val old = measure(reference)
        (old, fresh)
    }
    def median(values: Seq[Long]): Long = values.sorted.apply(values.size / 2)
    println(s"SCAN_BENCH {\"case\":\"$name\",\"inputs\":${inputs.size},\"fs2Ns\":${median(samples.map(_._1._1))},\"oursNs\":${median(samples.map(_._2._1))},\"fs2Allocated\":${median(samples.map(_._1._2))},\"oursAllocated\":${median(samples.map(_._2._2))}}")

  def spec = suite("FS2 paired scan benchmark")(
    test("equivalent kernels and stream adapters") {
      compare("running-sum-kernel")(theirs(fsSum, inputs, identity[Long]), ours(sum, inputs, identity[Long]))
      val chain = sum.map(_ + 1).map(_ * 2).map(_ - 3)
      val fsChain = fsSum.map(_ + 1).map(_ * 2).map(_ - 3)
      compare("mapped-sum-kernel")(theirs(fsChain, inputs, identity[Long]), ours(chain, inputs, identity[Long]))
      val context = inputs.map(value => (value, "context"))
      compare("contextual-first-kernel")(theirs(fsSum.first[String], context, _._1), ours(sum.first[String], context, _._1))
      val selected = inputs.map(value => if value % 2 == 0 then Left(value) else Right(value))
      compare("choose-kernel")(theirs(fsSum.choose(fsSum), selected, _.fold(identity[Long], identity[Long])),
        ours(sum.choose(sum), selected, _.fold(identity[Long], identity[Long])))
      val source = Stream.chunk(chunk).covary[IO]
      val fsStream = source.through(fsChain.toPipe[IO]).compile.fold(0L)(_ + _)
      val ourStream = source.through(chain.toPipe[IO]).compile.fold(0L)(_ + _)
      // IO may run on another thread; current-thread allocations are not its total.
      compare("chunked-source-pipe", countAllocation = false)(() => fsStream.unsafeRunSync(), () => ourStream.unsafeRunSync())
      val elementwise = source.through(chain.toPipeElementwise[IO]).compile.fold(0L)(_ + _)
      compare("elementwise-source-pipe", countAllocation = false)(() => fsStream.unsafeRunSync(), () => elementwise.unsafeRunSync())
      assertTrue(sink >= Long.MinValue)
    }
  )
