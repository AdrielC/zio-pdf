package zio.pdf.arrow

import volga.free.Nat

/** Boundary port labels for volga SMC `link` — arity from the graph, not the call site. */
object WiringBoundary {

  /** `Nat` arity of a free-prop diagram (match-type view). */
  type InArity[F] = F match {
    case volga.free.FreeProp[_, i, _] => i
  }

  type OutArity[F] = F match {
    case volga.free.FreeProp[_, _, j] => j
  }

  /** Build `Nat.Vec[I, String]` — explicit labels first, then `in` / `in1` / … defaults. */
  def vec[I: Nat](labels: String*): Nat.Vec[I, String] =
    Nat.Vec.tabulate[I, String](i => labels.lift(i).getOrElse(defaultFor(i)))

  private def defaultFor(i: Int): String =
    if i == 0 then "in" else s"in$i"
}
