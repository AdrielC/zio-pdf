package volga.syntax.parsing

import scala.{PartialFunction as =\>}
import scala.quoted.{Quotes, Type}
import scala.annotation.threadUnsafe
import scala.annotation.tailrec
import volga.free.Nat.Vec

import volga.syntax.parsing.VError

import volga.syntax.parsing.Pos.{Mid, End, Tupling}
import volga.syntax.parsing.STerm
import volga.syntax.parsing.App
import volga.syntax.smc.SyApp
import volga.syntax.smc.V
import volga.free.Nat

final class MParsing[q <: Quotes & Singleton](using val q: q):
    import q.reflect.*
    import Pos.*
    val vars = Vars[q.type]()

    val SyAppRepr = TypeRepr.of[SyApp[?, ?]]

    private type PMid      = STerm[Var[q.type], Tree] & Pos.Mid
    private type PMidTup   = STerm[Var[q.type], Tree] & (Pos.Mid | Pos.Tupling)
    private type PEnd      = STerm[Var[q.type], Tree] & Pos.End
    private type PAnywhere = STerm[Var[q.type], Tree] & Pos.Mid & Pos.End
    private type PApp      = App[Var[q.type], Tree]

    type Parsed = (Vector[Var[q.type]], Vector[PMid], PEnd)

    @tailrec def parseBlock(block: Tree): Either[VError, Parsed] = block match
        case CFBlock(body)          => parseBlock(body)
        case Block(Nil, res: Block) => parseBlock(res)
        case InlineTerm(t)          => parseBlock(t)
        case Lambda(valDefs, body)  => parseWithVals(valDefs, body)
        case body                   => parseWithVals(Nil, body)

    private def parseWithVals(valDefs: List[ValDef], block: Tree): Either[VError, Parsed] =
        val vs = valDefs.toVector.map:
            case ValDef(name, tpe, rhs) => vars.varOf(name, tpe, rhs)

        block match
            case Block(mids, res) =>
                for
                    midTerms     <- VError.traverse(mids, asMidSTerm)(midTermErr)
                    detupledMids <- detuple(midTerms)
                    endTerm      <- VError.applyOr(res)(asEndTerm)(endTermErr(res))
                    (mids2, end2) = desugarProducers(detupledMids, endTerm)
                yield (vs, mids2, end2)
            case single           =>
                for term <- VError.applyOr(single)(asEndTerm)(endTermErr(single))
                yield
                    val (mids2, end2) = desugarProducers(Vector.empty, term)
                    (vs, mids2, end2)
        end match
    end parseWithVals

    /** 0-in wiring nodes (`node()`) — SyApp apply with no wire args. */
    private def isSourceApp(app: PApp): Boolean = app.args.isEmpty

    private var freshWireId = 0

    private def assignmentForSource(app: PApp): STerm.Assignment[Var[q.type], Tree] =
        freshWireId += 1
        val wire = vars.varOf(
            s"$$wire$$freshWireId",
            TypeTree.of[V[Nat.`1`]],
            Some(app.applied)
        )
        STerm.Assignment(Vector(wire), app)

    /** Turn standalone `node()` into `val $wire = node(); $wire` so stages track the producer. */
    private def desugarProducers(mids: Vector[PMid], last: PEnd): (Vector[PMid], PEnd) =
        val mids2 = mids.map:
            case app @ STerm.Application(app0) if isSourceApp(app0) => assignmentForSource(app0)
            case other                                            => other
        desugarLast(mids2, last)

    private def desugarLast(mids: Vector[PMid], last: PEnd): (Vector[PMid], PEnd) =
        last match
            case app @ STerm.Application(app0) if isSourceApp(app0) =>
                val assign = assignmentForSource(app0)
                (mids :+ assign, STerm.Result(assign.receivers))
            case other => (mids, other)

    val InlineTerm: Inlined =\> Term =
        case Inlined(_, _, t) => t

    private val CFBlock: Tree =\> Tree =
        case Lambda(List(p), t) if p.tpt.tpe <:< SyAppRepr => t

    object TupleAccess:
        def unapply(tree: Tree): Option[(String, Int)] =
            def index(sel: String): Option[Int] =
                if sel.startsWith("_") then OfInt.unapply(sel.drop(1)).map(_ - 1) else None
            tree match
                case Select(Ident(name), sel) =>
                    index(sel).map((name, _))
                case Select(TypeApply(Select(Ident(name), _), _), sel) =>
                    index(sel).map((name, _))
                case Select(Apply(TypeApply(Select(Ident(name), _), _), _), sel) =>
                    index(sel).map((name, _))
                case _ => None
    end TupleAccess

    val asMidSTerm: Tree =\> PMidTup =
        case asAnywhereTerm(t)                                                                  => t
        case ValDef(name, t, Some(asApplication(app)))                                          =>
            STerm.Assignment(Vector(vars.varOf(name, t, Some(app.applied))), app)
        case ValDef(name, _, Some(Match(Typed(asApplication(app), t @ TupleRepr(i)), List(_)))) =>
            STerm.Tupled(vars.varNamed(name), i, app)
        case ValDef(name, t, Some(TupleAccess(tname, i)))                                       =>
            STerm.Untupling(vars.varNamed(tname), vars.varOf(name, t), i)
        case ValDef(name, t, Some(Select(Ident(tname), s"_${OfInt(i)}")))                       =>
            STerm.Untupling(vars.varNamed(tname), vars.varOf(name, t), i - 1)
    end asMidSTerm

    val asEndTerm: Tree =\> PEnd =
        case asAnywhereTerm(t)                           => t
        case t if asApplication.isDefinedAt(t)           => STerm.Application(asApplication(t))
        case Typed(Ident(name), _)                       => STerm.Result(Vector(vars.varNamed(name)))
        case Ident(name)                                 => STerm.Result(Vector(vars.varNamed(name)))
        case Apply(TupleApp(()), ident.travector(names)) => STerm.Result(names.map(vars.varNamed))

    val asApplication: Tree =\> PApp =
        case Apply(Apply(_, List(t)), ident.travector(names)) =>
            App(t, names.map(vars.varNamed))

    val ident: Term =\> String =
        case Ident(name) => name

    val TupleApp: Term =\> Unit =
        case TypeApply(Select(Ident(s"Tuple$_"), "apply"), _) =>

    val asAnywhereTerm: Tree =\> PAnywhere =
        case t if asApplication.isDefinedAt(t) && asApplication(t).args.nonEmpty =>
            STerm.Application(asApplication(t))

    object TupleRepr:
        @threadUnsafe lazy val ConsS = TypeRepr.of[? *: ?].classSymbol
        @threadUnsafe lazy val NilS  = TypeRepr.of[EmptyTuple].classSymbol

        def unapply(x: TypeTree): Option[Int] =
            val t = x.tpe.dealias
            if t.isTupleN then Some(t.typeArgs.size)
            else arity(t, 0, x.pos)

        @tailrec private def arity(tr: TypeRepr, acc: Int, pos: Position): Option[Int] =
            tr.classSymbol match
                case ConsS =>
                    tr.typeArgs match
                        case List(_, tail) => arity(tail, acc + 1, pos)
                        case args          =>
                            report.error("unexpected error while matching tuples", pos)
                            None

                case NilS => Some(acc)
                case _    => None
    end TupleRepr

    val midTermErr = VError.atTree("error while parsing mid term")
    val endTermErr = VError.atTree("error while parsing end term")

    private def fullVector[A] = ({ case Some(a) => a }: Option[A] =\> A).travector

    case class TuplingState(app: Option[PApp] = None, bindings: Vector[Option[Var[q.type]]] = Vector.empty, arity: Int = 0):
        def addBinding(index: Int, binding: Var[q.type]) =
            val newBindings =
                if bindings.size > index then bindings.updated(index, Some(binding))
                else bindings ++ Vector.fill(index - bindings.size)(None) :+ Some(binding)
            copy(bindings = newBindings)

        def define(app: PApp, arity: Int) = copy(app = Some(app), arity = arity)

    val CompleteTuplingState: TuplingState =\> PMid =
        case TuplingState(Some(app), fullVector(xs), arity) if arity > 0 && xs.size == arity =>
            STerm.Assignment(xs, app)

    case class DetupleState(
        tuplings: Map[String, TuplingState] = Map.empty,
        result: Vector[PMid] = Vector.empty
    ):
        private def add(mid: PMid) = copy(result = result :+ mid)

        private def updateTupling(v: Var[q.type])(f: TuplingState => TuplingState) =
            f(tuplings.getOrElse(v.name, TuplingState())) match
                case CompleteTuplingState(mid) => add(mid).copy(tuplings = tuplings - v.name)
                case tupling                   => copy(tuplings = tuplings.updated(v.name, tupling))

        def push(cmd: STerm[Var[q.type], Tree] & (Pos.Mid | Pos.Tupling)): DetupleState = cmd match
            case STerm.Tupled(receiver, arity, application) => updateTupling(receiver)(_.define(application, arity))
            case STerm.Untupling(src, tgt, index)           => updateTupling(src)(_.addBinding(index, tgt))
            case term: Pos.Mid                              => add(term)

        def end: Either[VError, Vector[PMid]] =
            if tuplings.isEmpty then Right(result)
            else
                val errs = tuplings.values.collect:
                    case TuplingState(Some(App(tree, _)), _, _) =>
                        VError.atTree("incorrect tupling")(tree)

                Left(errs.reduce(_ ++ _))

    end DetupleState

    def detuple(commands: Iterable[PMidTup], acc: Vector[PMid] = Vector.empty)(using
        Quotes
    ): Either[VError, Vector[PMid]] =
        commands.foldLeft(DetupleState())(_.push(_)).end

end MParsing
