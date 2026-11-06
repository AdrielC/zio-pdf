package io.github.adrielc.bytescan

import java.util.IdentityHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable.ArrayBuffer
import io.github.adrielc.scan.{Scan, Span}

/** Portable byte scanning. This package has no PDF or effect-library dependency.
  * Sessions and their views are confined to one driver; definitions are reusable.
  */
object SharedScan:
  final val MaxBlockBytes = 64 * 1024
  final val MaxRetainedBytes = 8 * 1024 * 1024
  final val MaxSegments = 4096
  final val MaxScans = 64
  final val MaxPins = 1024

  enum LimitField:
    case RetainedBytes, Segments, Scans, Pins
  final case class LimitIssue(field: LimitField, observed: Long, minimum: Long, maximum: Long)

  final class Limits private (
      val retainedBytes: Long, val segments: Int, val scans: Int, val pins: Int)
  object Limits:
    def checked(retainedBytes: Long = 1024 * 1024, segments: Int = 256,
        scans: Int = 32, pins: Int = 128): Either[Vector[LimitIssue], Limits] =
      val issues = Vector(
        (LimitField.RetainedBytes, retainedBytes, 1L, MaxRetainedBytes.toLong),
        (LimitField.Segments, segments.toLong, 1L, MaxSegments.toLong),
        (LimitField.Scans, scans.toLong, 1L, MaxScans.toLong),
        (LimitField.Pins, pins.toLong, 0L, MaxPins.toLong)
      ).collect { case (field, value, min, max) if value < min || value > max => LimitIssue(field, value, min, max) }
      if issues.nonEmpty then Left(issues) else Right(new Limits(retainedBytes, segments, scans, pins))
    val default: Limits = new Limits(1024 * 1024, 256, 32, 128)

  enum BlockIssue:
    case MissingBytes, MissingReader
    case Length(observed: Int, maximum: Int)
    case BackingBytes(observed: Long, maximum: Long)
    case BackingSmallerThanView(length: Int, backing: Long)

  /** Primitive index/read signature avoids boxing Int offsets in byte hot paths. */
  @java.lang.FunctionalInterface
  trait ByteReader:
    def apply(index: Int): Byte

  /** A bounded block. Shared readers never receive its mutable backing storage.
    * Borrowing is an explicit producer contract; copying is the safe array ingress.
    */
  final class Block private (val length: Int, val retainedBytes: Long, reader: ByteReader):
    private val references = new AtomicInteger(0)
    def activeReferences: Int = references.get()
    def isRetained: Boolean = activeReferences != 0
    private[SharedScan] def at(index: Int): Byte = reader(index)
    /** Trusted parser access to immutable owned ingress blocks. */
    private[adrielc] def parserByte(index: Int): Byte = reader(index)
    private[SharedScan] def acquire(): Unit = { val _ = references.incrementAndGet(); () }
    private[SharedScan] def release(): Unit = { val _ = references.decrementAndGet(); () }

  object Block:
    private[SharedScan] def owned(bytes: Array[Byte]): Block = new Block(bytes.length, bytes.length.toLong, bytes.apply)
    private def validate(length: Int, backing: Long): Either[Vector[BlockIssue], Unit] =
      val issues = Vector.newBuilder[BlockIssue]
      if length <= 0 || length > MaxBlockBytes then issues += BlockIssue.Length(length, MaxBlockBytes)
      if backing <= 0 || backing > MaxRetainedBytes then issues += BlockIssue.BackingBytes(backing, MaxRetainedBytes)
      if length > 0 && backing > 0 && backing < length then issues += BlockIssue.BackingSmallerThanView(length, backing)
      val found = issues.result()
      if found.nonEmpty then Left(found) else Right(())

    /** Checks the bound before allocating the owned array. */
    def copyOf(length: Int)(read: ByteReader): Either[Vector[BlockIssue], Block] =
      validate(length, length.toLong).flatMap { _ =>
        if read == null then Left(Vector(BlockIssue.MissingReader))
        else
          val owned = new Array[Byte](length)
          var index = 0
          while index < length do
            owned(index) = read(index)
            index += 1
          Right(Block.owned(owned))
      }
    def copyOf(bytes: Array[Byte]): Either[Vector[BlockIssue], Block] =
      if bytes == null then Left(Vector(BlockIssue.MissingBytes)) else copyOf(bytes.length)(bytes.apply)

    /** No copy. The reader must remain immutable and alive while isRetained is true.
      * backingBytes must cover the retained backing storage, including a larger
      * array retained by a slice. It is byte-storage accounting, not total heap.
      */
    def borrowImmutable(length: Int, backingBytes: Long)(read: ByteReader): Either[Vector[BlockIssue], Block] =
      validate(length, backingBytes).flatMap { _ =>
        if read == null then Left(Vector(BlockIssue.MissingReader))
        else Right(new Block(length, backingBytes, read))
      }
    /** The producer may refill the array only after every owning session releases it. */
    def unsafeBorrowArray(bytes: Array[Byte]): Either[Vector[BlockIssue], Block] =
      if bytes == null then Left(Vector(BlockIssue.MissingBytes))
      else borrowImmutable(bytes.length, bytes.length.toLong)(bytes.apply)

  enum AccessIssue:
    case Expired
    case OutsideRange(from: Long, until: Long, availableFrom: Long, availableUntil: Long)
    case PinLimit(maximum: Int)
    case DetachLimit(observed: Long, maximum: Int)

  enum ProtocolIssue:
    case CursorMovedBackwards(previous: Long, next: Long)
    case CursorOutsideInput(cursor: Long, end: Long)
    case InvalidRetention(previous: Long, next: Long, cursor: Long)
    case InvalidDemand(minimumBytes: Int)
    case NoProgress

  enum Failure[+E]:
    case Stage(index: Int, error: E)
    case Protocol(index: Int, issue: ProtocolIssue)
    case UnexpectedEnd(index: Int, cursor: Long, available: Long, minimumBytes: Int)
    case Backpressure(retainedBytes: Long, incomingBytes: Long, maximumBytes: Long,
        segments: Int, maximumSegments: Int)
    case TooManyScans(observed: Int, maximum: Int)
    case InvalidInitialOffset(value: Long)
    case OffsetOverflow(offset: Long, length: Int)
    case Closed, AlreadyCompleted, ReentrantDriver

  enum Step[+E, +S, +A]:
    /** position is consumed input; retainFrom is the earliest byte still needed.
      * The next call waits for minimumBytes after position, or for EOF.
      */
    case Await(state: S, position: Long, retainFrom: Long, minimumBytes: Int = 1)
    case Done(value: A)
    case Failed(error: E)

  /** Validated reader position, independent of the storage range. */
  final case class Cursor(retainFrom: Long, position: Long, minimumBytes: Int)

  enum PipelineIssue[+E]:
    case Decode(error: E)
    case Run(error: Scan.RunIssue)

  private enum Transition:
    case Await(position: Long, retainFrom: Long, minimumBytes: Int)
    case Done(value: Any)
    case Failed(error: Any)
  private trait Machine:
    def step(window: Window, endOfInput: Boolean): Transition
    def close(): Unit = ()
  private final class Stage(val create: () => Machine)
  private final class Evaluation(stages: Vector[Stage], values: Vector[Any]):
    private val cache = new IdentityHashMap[Probe[?, ?], Any]()
    def stage(stage: Stage): Any = values(stages.indexWhere(_ eq stage))
    def value[A](scan: Probe[?, A]): A =
      if cache.containsKey(scan) then cache.get(scan).asInstanceOf[A]
      else
        val result = scan.assemble(this)
        val _ = cache.put(scan, result)
        result

  /** map shares a stage's result; zip shares input and deduplicates common stages
    * by definition identity. Each session evaluates the initial-state expression
    * again; mutable seeds must be allocated inside that expression.
    */
  final class Probe[+E, +A] private (
      private[SharedScan] val stages: Vector[Stage], private[SharedScan] val assemble: Evaluation => A):
    def map[B](f: A => B): Probe[E, B] = new Probe[E, B](stages, values => f(values.value(this)))
    def zip[E2 >: E, B](that: Probe[E2, B]): Probe[E2, (A, B)] =
      val combined = stages ++ that.stages.filterNot(stage => stages.exists(_ eq stage))
      new Probe[E2, (A, B)](combined, values => (values.value(this), values.value(that)))

  object Probe:
    /** Decode borrowed windows into a generic scan, folding outputs as they arrive.
      * Parser, transducer and accumulator state are fresh for every session.
      */
    def decode[E, S, I, O, A](initial: => S, pipeline: Scan.Compiled[I, O, ?], result: => A)(
        read: (S, Window, Boolean, Scan.Emit[I]) => Step[E, S, Unit]
    )(fold: (A, O) => A): Probe[PipelineIssue[E], A] =
      val stage = new Stage(() => new Machine:
        private var state = initial
        private var accumulated = result
        private val runner = pipeline.open(value => accumulated = fold(accumulated, value))
        def step(window: Window, endOfInput: Boolean): Transition =
          var next: Option[Step[E, S, Unit]] = None
          runner.feed(emit => next = Some(read(state, window, endOfInput, emit))) match
            case Left(issue) => Transition.Failed(PipelineIssue.Run(issue))
            case Right(_) => next.get match
              case Step.Await(next, position, retainFrom, minimum) =>
                state = next
                Transition.Await(position, retainFrom, minimum)
              case Step.Failed(issue) => Transition.Failed(PipelineIssue.Decode(issue))
              case Step.Done(_) => runner.finish() match
                case Left(issue) => Transition.Failed(PipelineIssue.Run(issue))
                case Right(_) => Transition.Done(accumulated)
        override def close(): Unit = runner.close()
      )
      new Probe(Vector(stage), values => values.stage(stage).asInstanceOf[A])

    /** Feed bytes into an ordinary scan without copying each scanner's input. */
    def bytes[O, A](pipeline: Scan.Compiled[Byte, O, ?], initial: => A)(fold: (A, O) => A): Probe[PipelineIssue[Nothing], A] =
      decode[Nothing, Unit, Byte, O, A]((), pipeline, initial) { (_, window, eof, emit) =>
        window.foldTrusted(window.position, window.end)(())((_, byte) => emit(byte))
        if eof then Step.Done(()) else Step.Await((), window.end, window.end)
      }(fold)

    def stateful[E, S, A](initial: => S)(advance: (S, Window, Boolean) => Step[E, S, A]): Probe[E, A] =
      val stage = new Stage(() => new Machine:
        private var state = initial
        def step(window: Window, endOfInput: Boolean): Transition = advance(state, window, endOfInput) match
          case Step.Await(next, position, retainFrom, minimum) =>
            state = next
            Transition.Await(position, retainFrom, minimum)
          case Step.Done(value) => Transition.Done(value)
          case Step.Failed(error) => Transition.Failed(error)
      )
      // The erased vector is private to the sealed plan constructors. Its one
      // entry is produced only by this stage's Step.Done[A].
      new Probe[E, A](Vector(stage), values => values.stage(stage).asInstanceOf[A])

    def succeed[A](value: => A): Probe[Nothing, A] = new Probe(Vector.empty, _ => value)
    def foldBytes[S](initial: => S)(fold: (S, Byte) => S): Probe[Nothing, S] =
      stateful[Nothing, S, S](initial) { (state, window, eof) =>
        val next = window.foldTrusted(window.position, window.end)(state)(fold)
        if eof then Step.Done(next) else Step.Await(next, window.end, window.end)
      }
    val count: Probe[Nothing, Long] = stateful[Nothing, Long, Long](0L) { (state, window, eof) =>
      val next = state + (window.end - window.position)
      if eof then Step.Done(next) else Step.Await(next, window.end, window.end)
    }

  final case class Stats(acceptedBytes: Long, retainedBytes: Long, peakRetainedBytes: Long,
      retainedSegments: Int, peakSegments: Int, activePins: Int, acceptedBlocks: Long, releasedBlocks: Long)

  private final case class Segment(start: Long, end: Long, block: Block)
  private final class Store(val limits: Limits, initialOffset: Long):
    val segments = ArrayBuffer.empty[Segment]
    private val roots = new IdentityHashMap[Block, java.lang.Integer]()
    val pins = ArrayBuffer.empty[Lease]
    var end = initialOffset
    var frontier = initialOffset
    var closed = false
    private var readers = 0
    private var liveBytes = 0L
    private var peakBytes = 0L
    private var peakSegments = 0
    private var acceptedBytes = 0L
    private var acceptedBlocks = 0L
    private var releasedBlocks = 0L

    def stats: Stats = Stats(acceptedBytes, liveBytes, peakBytes, segments.length, peakSegments,
      pins.length, acceptedBlocks, releasedBlocks)
    def incomingCost(block: Block): Long = if roots.containsKey(block) then 0L else block.retainedBytes
    def append(block: Block): Unit =
      val previous = Option(roots.get(block)).fold(0)(_.intValue)
      if previous == 0 then liveBytes += block.retainedBytes
      val _ = roots.put(block, previous + 1)
      block.acquire()
      segments += Segment(end, end + block.length.toLong, block)
      end += block.length.toLong
      acceptedBytes += block.length.toLong
      acceptedBlocks += 1
      peakBytes = math.max(peakBytes, liveBytes)
      peakSegments = math.max(peakSegments, segments.length)

    private def release(segment: Segment): Unit =
      val count = roots.get(segment.block).intValue
      if count == 1 then
        val _ = roots.remove(segment.block)
        liveBytes -= segment.block.retainedBytes
      else
        val _ = roots.put(segment.block, count - 1)
      segment.block.release()
      releasedBlocks += 1

    def reclaim(): Unit =
      if readers == 0 then
        segments.filterInPlace { segment =>
          val keep = !closed && (segment.end > frontier ||
            pins.exists(pin => pin.start < segment.end && pin.end > segment.start))
          if !keep then release(segment)
          keep
        }

    /** Pure memory borrow guard. Even reentrant close cannot release a backing
      * array while a scanner/read callback is still inspecting it.
      */
    def borrowed[A](read: => A): A =
      readers += 1
      try read
      finally
        readers -= 1
        reclaim()

    def byteAt(position: Long): Byte =
      val segment = segments.find(segment => position >= segment.start && position < segment.end).get
      segment.block.at((position - segment.start).toInt)
    def fold[S](from: Long, until: Long)(initial: S)(f: (S, Byte) => S): S =
      var state = initial
      segments.foreach { segment =>
        var position = math.max(from, segment.start)
        val end = math.min(until, segment.end)
        while position < end do
          state = f(state, segment.block.at((position - segment.start).toInt))
          position += 1
      }
      state
    def pin(from: Long, until: Long): Either[AccessIssue, Lease] =
      if pins.length >= limits.pins then Left(AccessIssue.PinLimit(limits.pins))
      else
        val lease = new Lease(this, from, until)
        pins += lease
        Right(lease)
    def unpin(lease: Lease): Unit =
      val index = pins.indexWhere(_ eq lease)
      if index >= 0 then { val _ = pins.remove(index); () }
      reclaim()
    def close(): Unit =
      closed = true
      pins.foreach(_.invalidate())
      pins.clear()
      reclaim()

  sealed abstract class View private[SharedScan] (
      private[SharedScan] val store: Store, val start: Long, val end: Long):
    protected def active: Boolean
    def isValid: Boolean = active && !store.closed
    /** The owner is the guarded view, never its mutable backing storage. */
    lazy val span: Span[View] = Span.checked(this, start, end).toOption.get
    private def validate(from: Long, until: Long): Either[AccessIssue, Unit] =
      if !isValid then Left(AccessIssue.Expired)
      else if from < start || until < from || until > end then Left(AccessIssue.OutsideRange(from, until, start, end))
      else Right(())
    def read(position: Long): Either[AccessIssue, Byte] =
      if position == Long.MaxValue then Left(AccessIssue.OutsideRange(position, position, start, end))
      else validate(position, position + 1).flatMap { _ =>
        store.borrowed {
          val value = store.byteAt(position)
          if isValid then Right(value) else Left(AccessIssue.Expired)
        }
      }
    def fold[S](from: Long, until: Long)(initial: S)(f: (S, Byte) => S): Either[AccessIssue, S] =
      validate(from, until).flatMap { _ =>
        store.borrowed {
          val value = store.fold(from, until)(initial)(f)
          if isValid then Right(value) else Left(AccessIssue.Expired)
        }
      }
    /** Detach only a named, bounded owned block; checks precede allocation. */
    def detach(from: Long, until: Long): Either[AccessIssue, Block] =
      validate(from, until).flatMap { _ =>
        val length = until - from
        if length < 1 || length > MaxBlockBytes then Left(AccessIssue.DetachLimit(length, MaxBlockBytes))
        else store.borrowed {
          val array = new Array[Byte](length.toInt)
          var index = 0
          store.fold(from, until)(()) { (_, byte) => array(index) = byte; index += 1; () }
          if isValid then Right(Block.owned(array)) else Left(AccessIssue.Expired)
        }
      }

  final class Window private[SharedScan] (owner: Store, val cursor: Cursor, until: Long)
      extends View(owner, cursor.retainFrom, until):
    def position: Long = cursor.position
    private var valid = true
    protected def active: Boolean = valid
    private[SharedScan] def invalidate(): Unit = valid = false
    private[SharedScan] def foldTrusted[S](from: Long, until: Long)(initial: S)(f: (S, Byte) => S): S =
      store.fold(from, until)(initial)(f)
    def pin(from: Long, until: Long): Either[AccessIssue, Lease] =
      if !isValid then Left(AccessIssue.Expired)
      else if from < start || until <= from || until > end then Left(AccessIssue.OutsideRange(from, until, start, end))
      else store.pin(from, until)

  final class Lease private[SharedScan] (owner: Store, from: Long, until: Long)
      extends View(owner, from, until) with AutoCloseable:
    private var valid = true
    protected def active: Boolean = valid
    private[SharedScan] def invalidate(): Unit = valid = false
    /** Independent lifetime over the same retained byte range, without copying. */
    def duplicate(): Either[AccessIssue, Lease] =
      if isValid then store.pin(start, end) else Left(AccessIssue.Expired)
    def close(): Unit =
      if valid then
        valid = false
        store.unpin(this)

  private final class Slot(val machine: Machine, initialOffset: Long):
    var position = initialOffset
    var retainFrom = initialOffset
    var minimum = 1
    var first = true
    var done = false
    var value: Any = ()

  final class Session[E, A] private (
      plan: Probe[E, A], val limits: Limits, initialOffset: Long) extends AutoCloseable:
    private val store = new Store(limits, initialOffset)
    private var slots = plan.stages.map(stage => new Slot(stage.create(), initialOffset))
    private var terminal: Option[Either[Failure[E], A]] = None
    private var ended = false
    private var driving = false
    def stats: Stats = store.stats
    def result: Option[Either[Failure[E], A]] = terminal
    def isClosed: Boolean = store.closed

    private def fail(error: Failure[E]): Unit =
      terminal = Some(Left(error))
      slots.foreach(_.machine.close())
      slots = Vector.empty
      store.close()
    private def protocol(slot: Slot, position: Long, retain: Long, minimum: Int): Option[ProtocolIssue] =
      if position < slot.position then Some(ProtocolIssue.CursorMovedBackwards(slot.position, position))
      else if position > store.end then Some(ProtocolIssue.CursorOutsideInput(position, store.end))
      else if retain < slot.retainFrom || retain > position then Some(ProtocolIssue.InvalidRetention(slot.retainFrom, retain, position))
      else if minimum < 1 || minimum > MaxRetainedBytes then Some(ProtocolIssue.InvalidDemand(minimum))
      else if position == slot.position && store.end - position >= minimum then Some(ProtocolIssue.NoProgress)
      else None

    private[SharedScan] def drive(): Unit =
      driving = true
      try
        var repeat = true
        while repeat && terminal.isEmpty && !store.closed do
          repeat = false
          slots.zipWithIndex.foreach { (slot, index) =>
            if terminal.isEmpty && !store.closed && !slot.done &&
                (slot.first || ended || store.end - slot.position >= slot.minimum) then
              slot.first = false
              val window = new Window(store, Cursor(slot.retainFrom, slot.position, slot.minimum), store.end)
              val next = store.borrowed {
                try slot.machine.step(window, ended)
                finally window.invalidate()
              }
              if !store.closed then next match
                case Transition.Failed(error) => fail(Failure.Stage(index, error.asInstanceOf[E]))
                case Transition.Done(value) => slot.done = true; slot.value = value; slot.machine.close()
                case Transition.Await(position, retain, minimum) =>
                  protocol(slot, position, retain, minimum) match
                    case Some(issue) => fail(Failure.Protocol(index, issue))
                    case None =>
                      slot.position = position
                      slot.retainFrom = retain
                      slot.minimum = minimum
                      if store.end - position >= minimum then repeat = true
                      else if ended then fail(Failure.UnexpectedEnd(index, position, store.end - position, minimum))
          }
          if terminal.isEmpty && !store.closed then
            if slots.forall(_.done) then
              val value = new Evaluation(plan.stages, slots.map(_.value)).value(plan)
              if !store.closed then
                terminal = Some(Right(value))
                slots = Vector.empty
                store.frontier = store.end
            else store.frontier = slots.filterNot(_.done).map(_.retainFrom).min
            store.reclaim()
      finally driving = false

    /** Capacity/offset rejection takes no ownership. Backpressure leaves the
      * session usable: release pins, then retry that same block. Failures from
      * a scanner, its cursor protocol or EOF terminate and release the session.
      */
    def offer(block: Block): Either[Failure[E], Option[A]] =
      if driving then Left(Failure.ReentrantDriver)
      else if store.closed then Left(Failure.Closed)
      else terminal match
        case Some(_) => Left(Failure.AlreadyCompleted)
        case None =>
          val cost = store.incomingCost(block)
          if block.length.toLong > Long.MaxValue - store.end then Left(Failure.OffsetOverflow(store.end, block.length))
          else if cost > limits.retainedBytes - stats.retainedBytes || stats.retainedSegments >= limits.segments then
            Left(Failure.Backpressure(stats.retainedBytes, cost, limits.retainedBytes, stats.retainedSegments, limits.segments))
          else
            store.append(block)
            drive()
            terminal match
              case Some(value) => value.map(Some.apply)
              case None if store.closed => Left(Failure.Closed)
              case None => Right(None)

    def finish(): Either[Failure[E], A] =
      if driving then Left(Failure.ReentrantDriver)
      else if store.closed then terminal match
        case Some(Left(error)) => Left(error)
        case _ => Left(Failure.Closed)
      else
        ended = true
        drive()
        terminal.getOrElse(Left(Failure.Closed))
    def close(): Unit =
      slots.foreach(_.machine.close())
      slots = Vector.empty
      store.close()

  object Session:
    def open[E, A](scan: Probe[E, A], limits: Limits = Limits.default,
        initialOffset: Long = 0): Either[Failure[E], Session[E, A]] =
      if initialOffset < 0 then Left(Failure.InvalidInitialOffset(initialOffset))
      else if scan.stages.length > limits.scans then Left(Failure.TooManyScans(scan.stages.length, limits.scans))
      else
        val session = new Session(scan, limits, initialOffset)
        session.drive()
        session.result match
          case Some(Left(error)) => session.close(); Left(error)
          case _ => Right(session)
