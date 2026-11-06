package io.github.adrielc.kyopdf.pipeline

import io.github.adrielc.kyopdf.*
import io.github.adrielc.scan.Scan
import kyo.*
import zio.blocks.schema.{Schema as BSchema}
import zio.blocks.schema.json.{Json, JsonCodecDeriver}
import PdfPipeline.{Kind, Value, Code, Issue}

/** Versioned, typed operations. Recipes carry identifiers and data; executors
  * are supplied by the host and are never deserialized or loaded from a recipe.
  */
object StepRegistry:
  final val MaxSettingsChars = 64 * 1024
  enum Access derives Schema:
    case Stream, RandomAccess, Document
  enum Capability derives Schema:
    case Inspect, Rewrite, Render, Ocr, ExternalProcess, Verify
  type ErrorCode = OperationErrorCode
  val ErrorCode = OperationErrorCode
  type Error = OperationError
  val Error = OperationError
  enum OnError derives Schema:
    case Fail, RecoverAndFlag
  final case class Call(operation: String, version: Int, settings: String,
      inputs: Vector[Kind], output: Kind, onError: OnError = OnError.Fail) derives Schema
  final case class Flag(node: String, operation: String, code: ErrorCode, message: String)

  /** A schema is used for export, encoding, strict decoding, and examples. */
  final class DataSchema[A](val id: String, val version: Int, val schema: BSchema[A]):
    require(id.matches("[a-z][a-z0-9.-]{0,63}") && version >= 1)
    private lazy val codec = schema.derive(JsonCodecDeriver.withRejectExtraFields(true))
    val kind: Kind = Kind.Data(id, version)
    lazy val jsonSchema: Json = StepSchema.exported(schema, s"${id}_v$version")
    def encode(value: A): String = codec.encodeToString(value)
    def decode(json: String): Either[Error, A] =
      if json.length > MaxSettingsChars then Left(Error(ErrorCode.Limit, "Step data exceeds 64 KiB"))
      else ReportJson.parse(json).left.map(_ => Error(ErrorCode.InvalidInput, "Expected bounded object data")).flatMap { _ =>
        codec.decode(json).left.map(e => Error(ErrorCode.InvalidInput, e.toString.take(300)))
      }

  sealed abstract class Port[A](val kinds: Vector[Kind], val schemas: Vector[Json]):
    def read(values: Vector[Value]): Either[Error, A]
    def write(value: A): Either[Error, Vector[Value]]
  object Port:
    private def wrong = Left(Error(ErrorCode.InvalidInput, "Registered operation port mismatch"))
    val unit: Port[Unit] = new Port[Unit](Vector.empty, Vector.empty):
      def read(values: Vector[Value]) = if values.isEmpty then Right(()) else wrong
      def write(value: Unit) = Right(Vector.empty)
    val file: Port[PdfFile] = new Port[PdfFile](Vector(Kind.File), Vector(Json.Object("type" -> Json.String("string"), "format" -> Json.String("pdf-file-reference"), "contentMediaType" -> Json.String("application/pdf")))):
      def read(values: Vector[Value]) = values match
        case Vector(Value.File(file)) => Right(file)
        case _ => wrong
      def write(value: PdfFile) = Right(Vector(Value.File(value)))
    val document: Port[PdfDocument] = new Port[PdfDocument](Vector(Kind.Document), Vector(Json.Object("type" -> Json.String("string"), "format" -> Json.String("pdf-document-reference")))):
      def read(values: Vector[Value]) = values match
        case Vector(Value.Document(doc)) => Right(doc)
        case _ => wrong
      def write(value: PdfDocument) = Right(Vector(Value.Document(value)))
    def data[A](schema: DataSchema[A]): Port[A] = new Port[A](Vector(schema.kind), Vector(schema.jsonSchema)):
      def read(values: Vector[Value]) = values match
        case Vector(Value.Data(kind, json)) if kind == schema.kind => schema.decode(json.encoded)
        case _ => wrong
      def write(value: A) =
        val encoded = schema.encode(value)
        schema.decode(encoded).flatMap(_ => ReportJson.parse(encoded).left.map(_ => Error(ErrorCode.Limit, "Step data exceeds its bound")))
          .map(json => Vector(Value.Data(schema.kind, json)))
    def pair[A, B](a: Port[A], b: Port[B]): Port[(A, B)] = new Port[(A, B)](a.kinds ++ b.kinds, a.schemas ++ b.schemas):
      def read(values: Vector[Value]) = for
        left <- a.read(values.take(a.kinds.size))
        right <- b.read(values.drop(a.kinds.size))
      yield (left, right)
      def write(value: (A, B)) = for
        left <- a.write(value._1)
        right <- b.write(value._2)
      yield left ++ right

  final class Context(val services: PdfPipeline.Services, val limits: PdfPipeline.Limits):
    private[pipeline] val flags = scala.collection.mutable.ArrayBuffer.empty[Flag]
    private[pipeline] val scans = scala.collection.mutable.Map.empty[String, Value]
    private[pipeline] var groups = Map.empty[String, Vector[(String, Call)]]
    private[pipeline] var sourcePasses: Int = 0

  sealed trait Entry:
    def id: String
    def version: Int
    def inputs: Vector[Kind]
    def output: Kind
    def access: Access
    def capabilities: Set[Capability]
    def descriptor: Json
    def check(settings: String): Vector[Issue]
    private[pipeline] def execute(values: Vector[Value], settings: String, context: Context): Value < (Abort[Error] & Sync)
    private[pipeline] def probe(settings: String): Option[Scan[Byte, Value]]

  /** One output per node; products are represented by typed data schemas. */
  final class Step[A, B, P](val id: String, val version: Int, val in: Port[A], val out: Port[B],
      val parameters: DataSchema[P], val examples: Vector[P], val access: Access,
      val capabilities: Set[Capability], validate: P => Vector[Issue] = (_: P) => Vector.empty,
      scan: Option[P => Scan[Byte, B]] = None)(
      run: (A, P, Context) => B < (Abort[Error] & Sync)) extends Entry:
    require(out.kinds.length == 1 && in.kinds.length <= 4 && examples.nonEmpty)
    require(scan.isEmpty || (in.kinds == Vector(Kind.File) && access == Access.Stream &&
      capabilities == Set(Capability.Inspect) && out.kinds.head.isInstanceOf[Kind.Data]))
    val inputs = in.kinds
    val output = out.kinds.head
    def call(settings: P, onError: OnError = OnError.Fail): Call =
      Call(id, version, parameters.encode(settings), inputs, output, onError)
    def descriptor: Json = Json.Object(
      "operation" -> Json.String(id), "version" -> Json.Number(version),
      "parameters" -> parameters.jsonSchema,
      "inputs" -> Json.Array(in.schemas*), "output" -> out.schemas.head,
      "access" -> Json.String(access.toString),
      "capabilities" -> Json.Array(capabilities.toVector.sortBy(_.ordinal).map(c => Json.String(c.toString))*),
      "examples" -> Json.Array(examples.map(p => Json.parseUnsafe(parameters.encode(p)))*))
    def check(settings: String): Vector[Issue] = parameters.decode(settings) match
      case Left(error) => Vector(Issue("settings", Code.Setting, error.message))
      case Right(value) => validate(value)
    private def get[T](value: Either[Error, T]): T < Abort[Error] = value.fold(Abort.fail, identity)
    private[pipeline] def execute(values: Vector[Value], settings: String, context: Context): Value < (Abort[Error] & Sync) =
      get(in.read(values)).map { input => get(parameters.decode(settings)).map { params =>
        run(input, params, context).map(value => get(out.write(value)).map(_.head))
      }}
    private[pipeline] def probe(settings: String): Option[Scan[Byte, Value]] = scan.flatMap { make =>
      parameters.decode(settings).toOption.map(p => make(p).map(value => out.write(value).toOption.get.head))
    }

  final class Registry private[StepRegistry] (val entries: Vector[Entry]):
    private val indexed = entries.map(e => (e.id -> e.version) -> e).toMap
    def find(id: String, version: Int): Option[Entry] = indexed.get(id -> version)
    def exported: Json = StepSchema.hoist(Json.Object("operations" -> Json.Array(entries.map(_.descriptor)*)))
    def add(entry: Entry): Either[Error, Registry] =
      if entries.size >= 256 then Left(Error(ErrorCode.Limit, "Registry has at most 256 operations"))
      else if indexed.contains(entry.id -> entry.version) then Left(Error(ErrorCode.InvalidInput, "Duplicate operation/version"))
      else if !entry.id.matches("[a-z][a-z0-9.-]{0,63}") || entry.version < 1 then Left(Error(ErrorCode.InvalidInput, "Invalid operation identity"))
      else Right(new Registry(entries :+ entry))
    def check(call: Call): Vector[Issue] = find(call.operation, call.version) match
      case None => Vector(Issue("operation", Code.Setting, "Unknown registered operation/version"))
      case Some(entry) =>
        val ports = Option.when(call.inputs != entry.inputs || call.output != entry.output)(Issue("ports", Code.Type, "Ports differ from registered schemas")).toVector
        val recovery = Option.when(call.onError == OnError.RecoverAndFlag && (entry.inputs != Vector(entry.output) || entry.capabilities.exists(c => Set(Capability.Rewrite, Capability.Verify)(c))))(
          Issue("onError", Code.Setting, "Recovery requires a non-critical unary step with identical input/output types")).toVector
        ports ++ recovery ++ entry.check(call.settings)

    private[pipeline] def run(node: String, call: Call, values: Vector[Value], context: Context): Value < (Abort[PdfError] & Sync) =
      val entry = find(call.operation, call.version).get
      val work: Value < (Abort[Error] & Sync) = context.scans.remove(node) match
        case Some(value) => value
        case None if entry.probe(call.settings).nonEmpty =>
          val group = context.groups.getOrElse(node, Vector(node -> call))
          val probes = group.map { (id, c) => find(c.operation, c.version).get.probe(c.settings).get.map(id -> _) }
          val fused = probes.reduceLeft((a, b) => a.broadcast(b).map(_.fold(identity, identity)))
          val file = values.head.asInstanceOf[Value.File].value
          fused.compile() match
            case Left(_) => Abort.fail(Error(ErrorCode.Limit, "Fused scan exceeds execution limits"))
            case Right(plan) => Sync.defer {
              val runner = plan.open((pair: (String, Value)) => context.scans(pair._1) = pair._2)
              SyncResource.ensure(Sync.defer(runner.close())) {
                context.sourcePasses += 1
                val fed = runner.feed { emit =>
                  val source = file.bytes.iterator
                  while !runner.isClosed && source.hasNext do emit(source.next())
                }.flatMap(_ => runner.finish())
                if fed.isLeft then Abort.fail(Error(ErrorCode.Limit, "Fused scan stopped at a bound"))
                else context.scans.remove(node).fold[Value < Abort[Error]](Abort.fail(Error(ErrorCode.Backend, "Probe emitted no result")))(identity)
              }
            }
        case None => entry.execute(values, call.settings, context)
      Abort.run[Error](work).map {
        case Result.Success(value) => value
        case Result.Failure(error) if call.onError == OnError.RecoverAndFlag && error.recoverable &&
            Set(ErrorCode.Unavailable, ErrorCode.Backend)(error.code) =>
          context.flags += Flag(node, call.operation, error.code, error.message.take(300))
          values.head
        case Result.Failure(error) => Abort.fail(PdfError.OperationFailed(node, call.operation, error.copy(message = error.message.take(300))))
        case Result.Panic(error) => Abort.fail(PdfError.OperationFailed(node, call.operation, Error(ErrorCode.Backend, s"Step defect: ${error.getClass.getSimpleName}")))
      }

  def literal[A](id: String, schema: DataSchema[A], example: A): Step[Unit, A, A] =
    new Step(id, 1, Port.unit, Port.data(schema), schema, Vector(example), Access.Document,
      Set(Capability.Inspect))((_, value, _) => value)

  val empty: Registry = new Registry(Vector.empty)
  lazy val standard: Registry = PdfSteps.registry
