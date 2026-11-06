package io.github.adrielc.scan

import scala.collection.mutable.ArrayBuffer
import zio.blocks.schema.binding.{RegisterOffset, Registers}

/** A pure, reusable, incremental transformation. Each input can emit zero or
  * more outputs; completion can emit a final tail. Definitions own no run state.
  */
sealed abstract class Scan[-I, +O] private (private val node: Scan.Node):
  self =>
  import Scan.*
  type State
  /** A fresh seed on every call; no mutable state is cached in the definition. */
  def initial: State

  def andThen[P](next: Scan[O, P]): Aux[(State, next.State), I, P] =
    make(Pipe(node, next.node, pair), () => (self.initial, next.initial))
  def map[P](f: O => P): Aux[State, I, P] =
    make(Pipe(node, Scan.map(f).node, keepLeft), () => self.initial)
  def contramap[J](f: J => I): Aux[State, J, O] =
    make(Pipe(Scan.map(f).node, node, keepRight), () => self.initial)

  /** Both branches see the same input reference. Outputs are tagged, not paired:
    * unequal emission counts never drop values or require an unbounded queue.
    */
  def broadcast[J <: I, P](other: Scan[J, P]): Aux[(State, other.State), J, Either[O, P]] =
    make(Fork(node, other.node, choose = false, pair), () => (self.initial, other.initial))

  /** Each input selects exactly one branch; both branches finish at source EOF. */
  def choose[J, P](other: Scan[J, P]): Aux[(State, other.State), Either[I, J], Either[O, P]] =
    make(Fork(node, other.node, choose = true, pair), () => (self.initial, other.initial))

  def dimap[J, P](input: J => I)(output: O => P): Aux[State, J, P] = contramap(input).map(output)
  def choice[J, P >: O](other: Scan[J, P]): Aux[(State, other.State), Either[I, J], P] =
    choose(other).map(_.fold(value => value, value => value))

  /** Contextual adapters pair outputs with the input that produced them. Like
    * FS2, they discard completion outputs, which have no originating input.
    */
  def lens[J, P](get: J => I, set: (J, O) => P): Aux[State, J, P] =
    semilens(input => Right(get(input)), set)
  def semilens[J, P](extract: J => Either[P, I], inject: (J, O) => P): Aux[State, J, P] =
    make(Context(node, input => extract(input.asInstanceOf[J]),
      (input, output) => inject(input.asInstanceOf[J], output.asInstanceOf[O])), () => self.initial)
  def semipass[J, P >: O](extract: J => Either[P, I]): Aux[State, J, P] = semilens(extract, (_, output) => output)
  def first[J]: Aux[State, (I, J), (O, J)] = lens(_._1, (input, output) => (output, input._2))
  def second[J]: Aux[State, (J, I), (J, O)] = lens(_._2, (input, output) => (input._1, output))
  def left[J]: Aux[State, Either[I, J], Either[O, J]] =
    semilens(_.fold(input => Right(input), pass => Left(Right(pass))), (_, output) => Left(output))
  def right[J]: Aux[State, Either[J, I], Either[J, O]] =
    semilens(_.fold(pass => Left(Left(pass)), input => Right(input)), (_, output) => Right(output))

  def imapState[S](to: State => S)(from: S => State): Aux[S, I, O] =
    make(StateMap(node, value => to(value.asInstanceOf[State]), value => from(value.asInstanceOf[S])), () => to(self.initial))
  def withState(seed: => State): Aux[State, I, O] = make(Seed(node, () => seed), () => seed)

  /** Callback variants of FS2's chunk-returning stepping API. They never
    * collect arbitrary outputs into a second unbounded result buffer.
    */
  def transform(state: State, input: I)(emit: O => Unit): Either[Error, State] =
    transformAccumulate(state, Iterator.single(input))(emit)
  def transformAccumulate(state: State, input: IterableOnce[I])(emit: O => Unit): Either[Error, State] =
    compile().flatMap { plan =>
      val run = plan.openAt(state, emit)
      try run.feed { out =>
        val iterator = input.iterator
        while !run.isClosed && iterator.hasNext do out(iterator.next())
      }.flatMap(_ => run.state)
      finally run.close()
    }
  def onComplete(state: State)(emit: O => Unit): Either[Error, Unit] = compile().flatMap { plan =>
    val run = plan.openAt(state, emit)
    try run.finish().map(_ => ()) finally run.close()
  }
  def step(input: I)(emit: O => Unit): Either[Error, Aux[State, I, O]] =
    transform(initial, input)(emit).map(state => withState(state))

  def compile(limits: Limits = Limits.default): Either[CompileIssue, Compiled[I, O, State]] =
    Scan.compile(node, limits, new Budget, 0)

object Scan:
  type Aux[S, -I, +O] = Scan[I, O] { type State = S }
  type Stateless[-I, +O] = Aux[Unit, I, O]
  private def make[S, I, O](node: Node, seed: () => S): Aux[S, I, O] =
    val definition = node match
      case atom: Atom => atom.copy(seed = seed)
      case other => other
    new Scan[I, O](definition):
      type State = S
      def initial: S = initialState(definition).asInstanceOf[S]
  private val pair: (Any, Any) => Any = (left, right) => (left, right)
  private val keepLeft: (Any, Any) => Any = (left, _) => left
  private val keepRight: (Any, Any) => Any = (_, right) => right
  /** Valid only during the step/completion callback. Outputs are delivered by
    * the runner before push/finish returns. Retained borrowed values need leases.
    */
  trait Emit[-O]:
    def apply(value: O): Unit

  /** Primitive signatures avoid boxing the Long state at callback boundaries. */
  trait LongStep[-I, +O]:
    def apply(state: Long, input: I, emit: Emit[O]): Long
  trait LongFinish[+O]:
    def apply(state: Long, emit: Emit[O]): Unit
  trait LongFold[-I]:
    def apply(state: Long, input: I): Long

  final val MaxNodes = 16384
  final val MaxPending = 65536
  final class Limits private (val nodes: Int, val pending: Int)
  object Limits:
    val default: Limits = new Limits(4096, 1024)
    def checked(nodes: Int = 4096, pending: Int = 1024): Either[CompileIssue, Limits] =
      if nodes < 1 || nodes > MaxNodes || pending < 1 || pending > MaxPending then
        Left(CompileIssue.InvalidLimits(nodes, pending))
      else Right(new Limits(nodes, pending))

  type Error = CompileIssue | RunIssue
  final val MaxAdaptationDepth = 64
  enum CompileIssue:
    case InvalidLimits(nodes: Int, pending: Int)
    case TooManyNodes(maximum: Int)
    case AdaptationDepth(maximum: Int)
  enum RunIssue:
    case Closed, ReentrantDriver, EscapedEmitter
    case PendingLimit(maximum: Int)

  final case class Layout(stages: Int, longSlots: Int, referenceSlots: Int)

  def apply[S, I, O](initial: => S)(step: (S, I, Emit[O]) => S, finish: (S, Emit[O]) => Unit): Aux[S, I, O] =
    stateful(initial)(step, finish)

  def lift[I, O](f: I => O): Stateless[I, O] = map(f)
  def stateful1[S, I, O](initial: => S)(f: (S, I) => (S, O)): Aux[S, I, O] =
    stateful[S, I, O](initial)((state, input, emit) => { val (next, output) = f(state, input); emit(output); next })

  def identity[I]: Stateless[I, I] = map(value => value)
  inline def map[I, O](inline f: I => O): Stateless[I, O] = stateless1(input => f(input))
  /** Exactly one output; the executor can bypass an emission callback. */
  def stateless1[I, O](f: I => O): Stateless[I, O] =
    make(Atom(_ => Instruction(Action.Map(value => f(value.asInstanceOf[I])), (_, _) => ())), () => ())
  def filter[I](predicate: I => Boolean): Stateless[I, I] =
    make(Atom(_ => Instruction(Action.Filter(value => predicate(value.asInstanceOf[I])), (_, _) => ())), () => ())
  def stateless[I, O](step: (I, Emit[O]) => Unit): Stateless[I, O] =
    make(Atom(_ => Instruction(Action.Callback((_, input, emit) => step(input.asInstanceOf[I], emit)), (_, _) => ())), () => ())

  def stateful[S, I, O](initial: => S)(
      step: (S, I, Emit[O]) => S,
      finish: (S, Emit[O]) => Unit = (_: S, _: Emit[O]) => ()
  ): Aux[S, I, O] = make(Atom { layout =>
    val slot = layout.reference[S]()
    Instruction(
      Action.Callback((state, input, emit) => state.write(slot, step(state.read(slot), input.asInstanceOf[I], emit))),
      (state, emit) => finish(state.read(slot), emit),
      state => state.write(slot, initial),
      state => state.read(slot),
      restore = (state, value) => state.write(slot, value.asInstanceOf[S])
    )
  }, () => initial)

  def statefulLong[I, O](initial: => Long)(step: LongStep[I, O],
      finish: LongFinish[O] = new LongFinish[O] { def apply(state: Long, emit: Emit[O]): Unit = () }): Aux[Long, I, O] = make(Atom { layout =>
    val slot = layout.long()
    Instruction(
      Action.Callback((state, input, emit) => state.writeLong(slot, step(state.readLong(slot), input.asInstanceOf[I], emit))),
      (state, emit) => finish(state.readLong(slot), emit),
      state => state.writeLong(slot, initial),
      state => state.readLong(slot),
      restore = (state, value) => state.writeLong(slot, value.asInstanceOf[Long])
    )
  }, () => initial)

  /** Emits one accumulated result at EOF, including for an empty source. */
  def fold[S, I](initial: => S)(f: (S, I) => S): Aux[S, I, S] =
    stateful[S, I, S](initial)((state, input, _) => f(state, input), (state, emit) => emit(state))
  def foldLong[I](initial: => Long)(f: LongFold[I]): Aux[Long, I, Long] = make(Atom { layout =>
    val slot = layout.long()
    Instruction(
      Action.Consume((state, input) => state.writeLong(slot, f(state.readLong(slot), input.asInstanceOf[I]))),
      (state, emit) => emit(state.readLong(slot)),
      state => state.writeLong(slot, initial),
      state => state.readLong(slot),
      restore = (state, value) => state.writeLong(slot, value.asInstanceOf[Long])
    )
  }, () => initial)
  def count[I]: Aux[Long, I, Long] = foldLong[I](0L)((state, _) => state + 1L)

  def branch[I, O, P](predicate: I => Boolean)(yes: Scan[I, O], no: Scan[I, P])
      : Aux[(yes.State, no.State), I, Either[O, P]] =
    yes.choose(no).contramap[I](input => if predicate(input) then Left(input) else Right(input))

  private sealed trait Node
  private final case class Atom(build: Allocation => Instruction, seed: () => Any = () => ()) extends Node
  private final case class Pipe(first: Node, second: Node, combine: (Any, Any) => Any) extends Node
  private final case class Context(source: Node, extract: Any => Either[Any, Any], inject: (Any, Any) => Any) extends Node
  private final case class StateMap(source: Node, to: Any => Any, from: Any => Any) extends Node
  private final case class Seed(source: Node, initial: () => Any) extends Node
  private final case class Fork(left: Node, right: Node, choose: Boolean, combine: (Any, Any) => Any) extends Node

  private enum SeedTask:
    case Visit(node: Node)
    case Combine(f: (Any, Any) => Any)
    case Map(f: Any => Any)

  /** Initial state is also stack-safe, independently of compilation/execution. */
  private def initialState(root: Node): Any =
    var tasks = List(SeedTask.Visit(root))
    var values = List.empty[Any]
    while tasks.nonEmpty do
      val task = tasks.head
      tasks = tasks.tail
      task match
        case SeedTask.Visit(Atom(_, seed)) => values = seed() :: values
        case SeedTask.Visit(Pipe(left, right, combine)) =>
          tasks = SeedTask.Visit(left) :: SeedTask.Visit(right) :: SeedTask.Combine(combine) :: tasks
        case SeedTask.Visit(Fork(left, right, _, combine)) =>
          tasks = SeedTask.Visit(left) :: SeedTask.Visit(right) :: SeedTask.Combine(combine) :: tasks
        case SeedTask.Visit(Context(child, _, _)) => tasks = SeedTask.Visit(child) :: tasks
        case SeedTask.Visit(StateMap(child, to, _)) => tasks = SeedTask.Visit(child) :: SeedTask.Map(to) :: tasks
        case SeedTask.Visit(Seed(_, seed)) => values = seed() :: values
        case SeedTask.Map(f) => values = f(values.head) :: values.tail
        case SeedTask.Combine(f) => values = f(values.tail.head, values.head) :: values.drop(2)
    values.head
  // Phantom slot types prevent mixing a reference slot with another state type.
  // Both erase to the public Blocks RegisterOffset representation.
  private object Slots:
    opaque type ReferenceSlot[S] = RegisterOffset.RegisterOffset
    opaque type LongSlot = RegisterOffset.RegisterOffset
    def reference[S](index: Int): ReferenceSlot[S] = RegisterOffset(objects = index)
    def long(index: Int): LongSlot = RegisterOffset(longs = index)
    def read[S](registers: Registers, slot: ReferenceSlot[S]): S = registers.getObject(slot).asInstanceOf[S]
    def write[S](registers: Registers, slot: ReferenceSlot[S], value: S): Unit = registers.setObject(slot, value.asInstanceOf[AnyRef])
    def readLong(registers: Registers, slot: LongSlot): Long = registers.getLong(slot)
    def writeLong(registers: Registers, slot: LongSlot, value: Long): Unit = registers.setLong(slot, value)
  import Slots.{ReferenceSlot, LongSlot}
  private final class Allocation:
    var longs = 0
    var refs = 0
    def long(): LongSlot = { val offset = Slots.long(longs); longs += 1; offset }
    def reference[S](): ReferenceSlot[S] = { val offset = Slots.reference[S](refs); refs += 1; offset }
  private final class Memory(layout: Layout):
    private val registers = Registers(RegisterOffset(longs = layout.longSlots, objects = layout.referenceSlots))
    def read[S](slot: ReferenceSlot[S]): S = Slots.read(registers, slot)
    def write[S](slot: ReferenceSlot[S], value: S): Unit = Slots.write(registers, slot, value)
    def readLong(slot: LongSlot): Long = Slots.readLong(registers, slot)
    def writeLong(slot: LongSlot, value: Long): Unit = Slots.writeLong(registers, slot, value)
    def clear(): Unit = registers.clearObjects(RegisterOffset(objects = layout.referenceSlots))
  private enum Action:
    case Callback(step: (Memory, Any, Emit[Any]) => Unit)
    case Map(f: Any => Any)
    case Filter(predicate: Any => Boolean)
    case Consume(step: (Memory, Any) => Unit)
    case Nested(step: (Memory, Any, Emit[Any]) => Either[RunIssue, Unit])
    case Fork(left: Int, right: Int, choose: Boolean)
  private final case class Instruction(
      action: Action,
      finish: (Memory, Emit[Any]) => Unit,
      initialize: Memory => Unit = _ => (),
      snapshot: Memory => Any = _ => (),
      next: Int = -1,
      restore: (Memory, Any) => Unit = (_, _) => (),
      allocate: Memory => Unit = _ => (),
      release: Memory => Unit = _ => (),
      complete: Option[(Memory, Emit[Any]) => Either[RunIssue, Unit]] = None)

  private enum Shape:
    case Leaf(read: Memory => Any, write: (Memory, Any) => Unit)
    case Zip(left: Shape, right: Shape, combine: (Any, Any) => Any)

    /** Materialize logical state only when requested, with no recursive walk. */
    def read(memory: Memory): Any =
      var tasks = List[Either[(Any, Any) => Any, Shape]](Right(this))
      var values = List.empty[Any]
      while tasks.nonEmpty do
        val task = tasks.head
        tasks = tasks.tail
        task match
          case Right(Leaf(get, _)) => values = get(memory) :: values
          case Right(Zip(left, right, combine)) =>
            tasks = Right(left) :: Right(right) :: Left(combine) :: tasks
          case Left(combine) =>
            values = combine(values.tail.head, values.head) :: values.drop(2)
      values.head

    def write(memory: Memory, value: Any): Unit =
      var tasks = List((this, value))
      while tasks.nonEmpty do
        val (shape, state) = tasks.head
        tasks = tasks.tail
        shape match
          case Leaf(_, put) => put(memory, state)
          case Zip(left, right, combine) =>
            if combine eq keepLeft then tasks = (left, state) :: tasks
            else if combine eq keepRight then tasks = (right, state) :: tasks
            else
              val pair = state.asInstanceOf[(Any, Any)]
              tasks = (left, pair._1) :: (right, pair._2) :: tasks

  private final class Budget:
    var nodes = 0

  private enum Build:
    case Visit(node: Node, next: Int)
    case SeedState(initial: () => Any, codeStart: Int, overrideStart: Int)
    case Before(first: Node, combine: (Any, Any) => Any)
    case Combine(second: Shape, combine: (Any, Any) => Any)
    case RightBranch(right: Node, rightNext: Int, choose: Boolean, combine: (Any, Any) => Any)
    case Join(leftEntry: Int, leftState: Shape, choose: Boolean, combine: (Any, Any) => Any)

  /** Iterative compilation; every edge targets a lower numbered instruction.
    * This also gives an upstream-before-downstream EOF order without recursion.
    */
  private def compile[I, O, S](root: Node, limits: Limits, budget: Budget, depth: Int): Either[CompileIssue, Compiled[I, O, S]] =
    if depth > MaxAdaptationDepth then return Left(CompileIssue.AdaptationDepth(MaxAdaptationDepth))
    val allocation = new Allocation
    val code = ArrayBuffer.empty[Instruction]
    var tasks = List(Build.Visit(root, -1))
    var entry = -1
    val overrides = ArrayBuffer.empty[Memory => Unit]
    var shape: Shape = Shape.Leaf(_ => (), (_, _) => ())
    def append(instruction: Instruction): Int = { code += instruction; code.length - 1 }
    while tasks.nonEmpty do
      val task = tasks.head
      tasks = tasks.tail
      task match
        case Build.Visit(node, next) =>
          budget.nodes += 1
          if budget.nodes > limits.nodes then return Left(CompileIssue.TooManyNodes(limits.nodes))
          node match
            case Atom(build, _) =>
              val instruction = build(allocation).copy(next = next)
              entry = append(instruction)
              shape = Shape.Leaf(instruction.snapshot, instruction.restore)
            case Pipe(first, second, combine) => tasks = Build.Visit(second, next) :: Build.Before(first, combine) :: tasks
            case Fork(left, right, choose, combine) =>
              val leftNext = append(Instruction(Action.Map(value => Left(value)), (_, _) => (), next = next))
              val rightNext = append(Instruction(Action.Map(value => Right(value)), (_, _) => (), next = next))
              tasks = Build.Visit(right, rightNext) :: Build.RightBranch(left, leftNext, choose, combine) :: tasks
            case Seed(child, initial) => tasks = Build.Visit(child, next) :: Build.SeedState(initial, code.length, overrides.length) :: tasks
            case Context(child, extract, inject) =>
              compile[Any, Any, Any](child, limits, budget, depth + 1) match
                case Left(issue) => return Left(issue)
                case Right(inner) =>
                  val slot = allocation.reference[ContextRun]()
                  val instruction = Instruction(
                    Action.Nested((memory, input, emit) => memory.read(slot).push(input, emit)), (_, _) => (),
                    memory => memory.read(slot).start(), memory => memory.read(slot).state,
                    next = next,
                    restore = (memory, value) => memory.read(slot).restore(value),
                    allocate = memory => memory.write(slot, new ContextRun(inner, extract, inject)),
                    release = memory => memory.read(slot).close())
                  entry = append(instruction)
                  shape = Shape.Leaf(instruction.snapshot, instruction.restore)
            case StateMap(child, to, from) =>
              compile[Any, Any, Any](child, limits, budget, depth + 1) match
                case Left(issue) => return Left(issue)
                case Right(inner) =>
                  val slot = allocation.reference[MappedRun]()
                  val instruction = Instruction(
                    Action.Nested((memory, input, emit) => memory.read(slot).push(input, emit)), (_, _) => (),
                    memory => memory.read(slot).start(), memory => memory.read(slot).state,
                    next = next,
                    restore = (memory, value) => memory.read(slot).restore(value),
                    allocate = memory => memory.write(slot, new MappedRun(inner, to, from)),
                    release = memory => memory.read(slot).close(),
                    complete = Some((memory, emit) => memory.read(slot).finish(emit)))
                  entry = append(instruction)
                  shape = Shape.Leaf(instruction.snapshot, instruction.restore)
        case Build.SeedState(initial, codeStart, overrideStart) =>
          var index = codeStart
          while index < code.length do
            code(index) = code(index).copy(initialize = _ => ())
            index += 1
          val _ = overrides.dropRightInPlace(overrides.length - overrideStart)
          val target = shape
          overrides += (memory => target.write(memory, initial()))
        case Build.Before(first, combine) =>
          tasks = Build.Visit(first, entry) :: Build.Combine(shape, combine) :: tasks
        case Build.Combine(second, combine) => shape = Shape.Zip(shape, second, combine)
        case Build.RightBranch(right, next, choose, combine) =>
          tasks = Build.Visit(right, next) :: Build.Join(entry, shape, choose, combine) :: tasks
        case Build.Join(right, rightState, choose, combine) =>
          entry = append(Instruction(Action.Fork(entry, right, choose), (_, _) => ()))
          shape = Shape.Zip(shape, rightState, combine)
    Right(new Compiled(code.toVector, entry, Layout(code.length, allocation.longs, allocation.refs), shape, overrides.toVector, limits.pending))

  private final class ContextRun(plan: Compiled[Any, Any, Any], extract: Any => Either[Any, Any], inject: (Any, Any) => Any):
    private var input: Any = ()
    private var output: Emit[Any] = null
    private val run = plan.blank(value => output(inject(input, value)))
    def start(): Unit = run.initialize()
    def state: Any = run.state.toOption.get
    def restore(value: Any): Unit = run.restore(value)
    def push(value: Any, emit: Emit[Any]): Either[RunIssue, Unit] = extract(value) match
      case Left(pass) => emit(pass); Right(())
      case Right(scanned) =>
        input = value
        output = emit
        try run.push(scanned)
        finally
          input = ()
          output = null
    def close(): Unit = run.close()

  private final class MappedRun(plan: Compiled[Any, Any, Any], to: Any => Any, from: Any => Any):
    private var output: Emit[Any] = null
    private val run = plan.blank(value => output(value))
    private var mapped: Any = ()
    def start(): Unit = { run.initialize(); mapped = to(run.state.toOption.get) }
    def state: Any = mapped
    def restore(value: Any): Unit = { mapped = value; run.restore(from(value)) }
    def push(input: Any, emit: Emit[Any]): Either[RunIssue, Unit] =
      run.restore(from(mapped))
      output = emit
      try run.push(input).map { _ => mapped = to(run.state.toOption.get) }
      finally output = null
    def finish(emit: Emit[Any]): Either[RunIssue, Unit] =
      run.restore(from(mapped))
      output = emit
      // Completion has no mapped state transition in the FS2 contract.
      try run.finish().map(_ => ())
      finally output = null
    def close(): Unit = run.close()

  /** Immutable compiled plan. open allocates fresh, run-local state slots. */
  final class Compiled[-I, +O, S] private[Scan] (
      private val code: Vector[Instruction], private val entry: Int, val layout: Layout, private val shape: Shape, private val overrides: Vector[Memory => Unit], private val pending: Int):
    type State = S
    def open(emit: O => Unit): Runner[I, S] =
      val run = blank(emit)
      var initialized = false
      try
        run.initialize()
        initialized = true
        run
      finally if !initialized then run.close()
    def openAt(state: S, emit: O => Unit): Runner[I, S] =
      val run = blank(emit)
      var initialized = false
      try
        shape.write(run.memory, state)
        initialized = true
        run
      finally if !initialized then run.close()
    private[Scan] def blank(emit: O => Unit): Runner[I, S] =
      new Runner(code, entry, layout, shape, overrides, pending, value => emit(value.asInstanceOf[O]))

  /** Serial, synchronous driver. No source or result collection is performed.
    * Domain errors may be represented in O (for example Either[Error, Event]).
    * Callback exceptions are defects: they propagate after closing this run.
    */
  final class Runner[-I, S] private[Scan] (
      code: Vector[Instruction], entry: Int, layout: Layout, shape: Shape, overrides: Vector[Memory => Unit], pending: Int, output: Any => Unit) extends AutoCloseable:
    private[Scan] val memory = new Memory(layout)
    // A bounded work stack preserves depth-first composition without recursion.
    private val targets = new Array[Int](pending)
    private val values = new Array[Any](pending)
    private var queued = 0
    private var failure: Option[RunIssue] = None
    private var closed = false
    private var driving = false
    private var released = false
    code.foreach(_.allocate(memory))
    private[Scan] def initialize(): Unit =
      code.reverseIterator.foreach(_.initialize(memory))
      overrides.foreach(_(memory))
    private[Scan] def restore(value: S): Unit = shape.write(memory, value)

    type State = S
    /** Mutable user state remains mutable; this is a typed observation, not a clone. */
    def state: Either[RunIssue, S] =
      if driving then Left(RunIssue.ReentrantDriver)
      else if closed then Left(failure.getOrElse(RunIssue.Closed))
      else Right(shape.read(memory).asInstanceOf[S])
    def isClosed: Boolean = closed
    def close(): Unit =
      closed = true
      java.util.Arrays.fill(values.asInstanceOf[Array[AnyRef]], null)
      queued = 0
      // A callback can close the run reentrantly. Clear again after it returns.
      if !driving && !released then
        released = true
        code.foreach(_.release(memory))
        memory.clear()

    private def fail(issue: RunIssue): Unit =
      failure = Some(issue)
      close()
    private def enqueue(target: Int, value: Any): Unit =
      if !closed then
        if queued >= pending then fail(RunIssue.PendingLimit(pending))
        else
          targets(queued) = target
          values(queued) = value
          queued += 1

    private def callback[A](next: Int)(f: Emit[Any] => A): A =
      var active = true
      val base = queued
      val emit = new Emit[Any]:
        def apply(value: Any): Unit =
          if !active then fail(RunIssue.EscapedEmitter)
          else enqueue(next, value)
      try f(emit)
      finally
        active = false
        // Reverse this callback's burst so its first output is processed first,
        // completely through downstream, before the next output is stepped.
        var left = base
        var right = queued - 1
        while left < right do
          val target = targets(left)
          val value = values(left)
          targets(left) = targets(right)
          values(left) = values(right)
          targets(right) = target
          values(right) = value
          left += 1
          right -= 1

    private def drain(): Unit =
      while queued != 0 && !closed do
        queued -= 1
        var target = targets(queued)
        var value = values(queued)
        values(queued) = null
        var advancing = true
        while advancing && !closed do
          if target == -1 then
            output(value)
            advancing = false
          else
            val instruction = code(target)
            instruction.action match
              case Action.Map(f) => value = f(value); target = instruction.next
              case Action.Filter(predicate) =>
                if predicate(value) then target = instruction.next else advancing = false
              case Action.Fork(left, right, true) => value.asInstanceOf[Either[Any, Any]] match
                case Left(selected) => value = selected; target = left
                case Right(selected) => value = selected; target = right
              case Action.Fork(left, right, false) => enqueue(right, value); target = left
              case Action.Consume(step) => step(memory, value); advancing = false
              case Action.Callback(step) =>
                val input = value
                callback(instruction.next)(emit => step(memory, input, emit))
                advancing = false
              case Action.Nested(step) =>
                val input = value
                callback(instruction.next)(emit => step(memory, input, emit)) match
                  case Left(issue) => fail(issue)
                  case Right(_) => ()
                advancing = false

    private def drive(finish: Boolean)(body: => Unit): Either[RunIssue, Unit] =
      if driving then Left(RunIssue.ReentrantDriver)
      else if closed then Left(failure.getOrElse(RunIssue.Closed))
      else
        driving = true
        var completed = false
        try
          body
          val result = failure match
            case Some(issue) => Left(issue)
            case None if closed => Left(RunIssue.Closed)
            case None => Right(())
          completed = true
          result
        finally
          driving = false
          if !completed || finish || closed then close()

    def push(input: I): Either[RunIssue, Unit] = drive(finish = false) { enqueue(entry, input); drain() }

    /** Feed a bounded source batch without a result wrapper per element. Each
      * input is drained before the next is supplied; this does not collect it.
      */
    def feed(inputs: Emit[I] => Unit): Either[RunIssue, Unit] = drive(finish = false) {
      var active = true
      val emit = new Emit[I]:
        def apply(value: I): Unit =
          if !active then fail(RunIssue.EscapedEmitter)
          else if !closed then
            enqueue(entry, value)
            drain()
      try inputs(emit)
      finally active = false
    }

    def finish(): Either[RunIssue, S] =
      var result: Option[S] = None
      drive(finish = true) {
        var index = code.length - 1
        while index >= 0 && !closed do
          val instruction = code(index)
          instruction.complete match
            case None => callback(instruction.next)(emit => instruction.finish(memory, emit))
            case Some(complete) => callback(instruction.next)(emit => complete(memory, emit)) match
              case Left(issue) => fail(issue)
              case Right(_) => ()
          drain()
          index -= 1
        if !closed then result = Some(shape.read(memory).asInstanceOf[S])
      }.map(_ => result.get)
