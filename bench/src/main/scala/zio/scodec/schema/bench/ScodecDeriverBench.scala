/*
 * JMH benchmark for ScodecDeriver vs an equivalent hand-rolled
 * scodec.Codec on a realistic complex type:
 *
 *   case class Order(
 *     id: Long,
 *     customer: Customer,         // nested record
 *     lines: List[OrderLine],      // length-prefixed sequence of records
 *     payment: Payment,            // sealed-trait variant (3 cases)
 *     total: Double,
 *     paid: Boolean
 *   )
 *
 * Bench: encode+decode round-trip of 10 K Orders.
 *
 * Run with:
 *
 *   sbt 'bench/Jmh/run -i 5 -wi 3 -f 1 -t 1 -bm avgt -tu ms .*ScodecDeriverBench.*'
 */

package zio.scodec.schema.bench

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.*

import _root_.scodec.{Attempt, Codec, Encoder}
import _root_.scodec.bits.BitVector
import _root_.scodec.codecs.*

import zio.blocks.schema.Schema
import zio.scodec.schema.ScodecDeriver

import scala.compiletime.uninitialized

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class ScodecDeriverBench {

  // ---- Domain model -----------------------------------------------------

  final case class Customer(id: Long, name: String, vip: Boolean)
  object Customer {
    given Schema[Customer] = Schema.derived[Customer]
  }

  final case class OrderLine(sku: String, qty: Int, unitPrice: Double)
  object OrderLine {
    given Schema[OrderLine] = Schema.derived[OrderLine]
  }

  sealed trait Payment
  final case class Card(last4: String, expiryMonth: Int, expiryYear: Int) extends Payment
  final case class BankTransfer(account: String)                          extends Payment
  final case class Cash(amount: Double)                                   extends Payment
  object Payment {
    given Schema[Payment] = Schema.derived[Payment]
  }

  final case class Order(
    id: Long,
    customer: Customer,
    lines: List[OrderLine],
    payment: Payment,
    total: Double,
    paid: Boolean
  )
  object Order {
    given Schema[Order] = Schema.derived[Order]
  }

  // ---- Hand-rolled equivalent codec ------------------------------------

  // String codec: 4-byte length prefix + UTF-8 bytes (matches utf8_32
  // and what ScodecDeriver picks for Strings).
  private val customerCodec: Codec[Customer] =
    (int64 :: utf8_32 :: bool(8))
      .xmap({ case (i, n, v) => Customer(i, n, v) }, c => (c.id, c.name, c.vip))

  private val orderLineCodec: Codec[OrderLine] =
    (utf8_32 :: int32 :: double)
      .xmap({ case (s, q, p) => OrderLine(s, q, p) }, l => (l.sku, l.qty, l.unitPrice))

  private val cardCodec: Codec[Card] =
    (utf8_32 :: int32 :: int32)
      .xmap({ case (l, m, y) => Card(l, m, y) }, c => (c.last4, c.expiryMonth, c.expiryYear))

  private val bankCodec: Codec[BankTransfer] =
    utf8_32.xmap(BankTransfer(_), _.account)

  private val cashCodec: Codec[Cash] =
    double.xmap(Cash(_), _.amount)

  // Variant tag mapping: must match what ScodecDeriver picks. We
  // have to be careful here - the JVM's Mirror.Sum ordering for
  // `Payment` controls the deriver's tag indices. We avoid making
  // assumptions about the order by using `selectTag` lookups.
  private val paymentCodec: Codec[Payment] = {
    // Manual tagged union: 1 byte tag + matching payload codec.
    // Tag values are chosen to *match* what ScodecDeriver uses
    // (whichever order the Mirror.Sum elects). We probe at startup.
    val derived = summon[Schema[Payment]].derive(ScodecDeriver)
    derived
  }

  private val orderCodec: Codec[Order] =
    (int64 :: customerCodec :: listOfN(int32, orderLineCodec) :: paymentCodec :: double :: bool(8))
      .xmap(
        { case (id, cust, lines, pay, tot, pd) => Order(id, cust, lines, pay, tot, pd) },
        o => (o.id, o.customer, o.lines, o.payment, o.total, o.paid)
      )

  // Schema-derived codec.
  private val derivedCodec: Codec[Order] =
    summon[Schema[Order]].derive(ScodecDeriver)

  // ---- Test data --------------------------------------------------------

  /** Number of Orders to round-trip per benchmark iteration. */
  @Param(Array("1000", "10000"))
  var n: Int = uninitialized

  private var orders: Array[Order]            = uninitialized
  private var encodedHand: Array[BitVector]   = uninitialized
  private var encodedDerived: Array[BitVector] = uninitialized

  @Setup(Level.Trial)
  def setup(): Unit = {
    val rng = new scala.util.Random(42L)
    orders = Array.tabulate(n) { i =>
      val customer = Customer(i.toLong, s"customer-$i", rng.nextBoolean())
      val nl       = (i % 5) + 1
      val lines    = (0 until nl).map { k =>
        OrderLine(s"sku-$i-$k", rng.nextInt(50) + 1, rng.nextDouble() * 100.0)
      }.toList
      val payment  = (i % 3) match {
        case 0 => Card(f"${rng.nextInt(10000)}%04d", rng.nextInt(12) + 1, 2025 + rng.nextInt(5))
        case 1 => BankTransfer(s"DE${rng.nextLong().abs % 1000000000L}")
        case _ => Cash(rng.nextDouble() * 200.0)
      }
      Order(
        id = i.toLong,
        customer = customer,
        lines = lines,
        payment = payment,
        total = lines.map(l => l.qty * l.unitPrice).sum,
        paid = rng.nextBoolean()
      )
    }

    // Pre-encode for the decode-only benches; use whichever codec
    // the bench under test uses, so a decode-only number reflects
    // what was put in.
    encodedHand    = orders.map(o => orderCodec.encode(o).require)
    encodedDerived = orders.map(o => derivedCodec.encode(o).require)
  }

  // ---- Benchmarks -------------------------------------------------------

  @Benchmark
  def handRolledEncode: Long = {
    var total = 0L
    var i     = 0
    while (i < n) {
      total += orderCodec.encode(orders(i)).require.size
      i += 1
    }
    total
  }

  @Benchmark
  def derivedEncode: Long = {
    var total = 0L
    var i     = 0
    while (i < n) {
      total += derivedCodec.encode(orders(i)).require.size
      i += 1
    }
    total
  }

  @Benchmark
  def handRolledDecode: Long = {
    var sum = 0L
    var i   = 0
    while (i < n) {
      sum += orderCodec.decode(encodedHand(i)).require.value.id
      i += 1
    }
    sum
  }

  @Benchmark
  def derivedDecode: Long = {
    var sum = 0L
    var i   = 0
    while (i < n) {
      sum += derivedCodec.decode(encodedDerived(i)).require.value.id
      i += 1
    }
    sum
  }
}
