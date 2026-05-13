/*
 * Flow-style typed scan builder.
 *
 * Modelled after `kyo.Flow`'s `Flow[In, Out, S]` shape: every
 * `.named[N, V](name, scan)` call refines the builder's `Out` type
 * parameter by intersection with `N ~ V`, so the compiler knows the
 * full record of named-leftover field types statically. At the end,
 * `run(inputs)` returns `Record[Out]` -- access fields by name via
 * `selectDynamic`, type-checked against `Out`.
 *
 * Example:
 *
 *   import zio.pdf.scan.{ScanFlow, Scan, HashAlgo}
 *
 *   val ingest =
 *     ScanFlow.over[Byte]
 *       .named("count",  Scan.countBytes)
 *       .named("digest", Scan.hash(HashAlgo.Sha256))
 *
 *   val rec = ingest.run((0 until 1024).map(_.toByte))
 *
 *   val count:  Seq[Byte] = rec.count    // type-checked against Out
 *   val digest: Seq[Byte] = rec.digest
 *
 * Semantics:
 *
 *   - Each labeled stage is a stateful scan over the *same input*
 *     whose final leftover becomes the named field's value.
 *   - The v0 implementation runs each labeled stage independently --
 *     `N` named stages mean `N` traversals of the input. Correct,
 *     readable, and easy to reason about. A v1 follow-up fuses every
 *     labeled stage into a single `Fanout` so the whole record is
 *     computed in one pass; that follow-up needs the register lane's
 *     native Fanout support (separate next-PR item).
 *
 * Why not extend `FreeScan` directly with a third type parameter?
 *
 *   - `FreeScan[I, O]` is the *AST* -- it stays small and unaware
 *     of names; this is what the runtime interprets.
 *   - `ScanFlow[I, Out]` is the *builder* -- it carries the names
 *     and a "produce one typed Record fragment per stage" closure
 *     per labeled stage, and the `Out` type in the type system. Same
 *     design split as Kyo's Flow vs internal AST nodes.
 *
 * Implementation detail (why we keep typed closures per stage):
 *
 *   Kyo's `Record` uses `Field(name, valueType)` as its internal
 *   map key for type-safety. Building a record fragment via the
 *   `String#~` extension at the call site of `.named` captures both
 *   the singleton `name.type` and the concrete `V` -- so the resulting
 *   `Record[name.type ~ V]` has the right key for `selectDynamic` to
 *   find. If we tried to assemble the map at `run` time with erased
 *   types, the lookup keys would mismatch.
 */

package zio.pdf.scan

import kyo.{Record, ~}

/** Flow-style typed scan builder over input type `I`.
  *
  * `Out` accumulates `N ~ V` intersection types as labeled stages are
  * added. Each stage carries a precomputed runner closure that, given
  * the inputs, produces a `Record[name ~ V]` fragment with the
  * correct internal key shape; `run` accumulates those fragments via
  * `Record#&`. */
final class ScanFlow[-I, +Out] @scala.annotation.publicInBinary private[scan] (
    private[scan] val stages: Vector[Iterable[Any] => Record[Any]]
) {

  /** Add a labeled stateful stage. The stage's scan is run over the
    * input stream; its final leftover becomes `record.<name>`.
    *
    * `inline` so that the singleton `name.type` and the concrete `V`
    * are visible to the body's `name ~ v` construction -- that is
    * what gives the resulting `Record` fragment the right internal
    * key shape for typed `selectDynamic` access. */
  /** Add a labeled stateful stage. The whole `Vector[V]` of final
    * leftover values becomes `record.<name>`. For stages that produce
    * one value (`Fold`, `Take`'s remainder, ...) call `.head` at the
    * use site; for stages that produce a byte vector (`Hash` ->
    * digest, `CountBytes` -> 8-byte BE encoding) the vector *is* the
    * value. */
  inline def named[N <: String & Singleton, V, I1 <: I](
      inline name: N,
      inline scan: FreeScan[I1, V]
  ): ScanFlow[I1, Out & (N ~ Vector[V])] = {
    val stage: Iterable[Any] => Record[Any] = { (inputs: Iterable[Any]) =>
      val (sig, _) = Scan.run[I1, V, Any](
        scan,
        inputs.asInstanceOf[Iterable[I1]]
      )
      val leftover: Vector[V] = sig.leftoverSeq.toVector
      // `name ~ leftover` runs at this call site with `name: N` and
      // `leftover: Vector[V]`, so the resulting Record fragment is
      // `Record[name.type ~ Vector[V]]` with the correct internal
      // `Field(name, Vector[V])` key.
      (name ~ leftover).asInstanceOf[Record[Any]]
    }
    new ScanFlow[I1, Out & (N ~ Vector[V])](
      stages.asInstanceOf[Vector[Iterable[Any] => Record[Any]]] :+ stage
    )
  }

  /** Run every labeled stage against `inputs` and assemble the
    * resulting fields into a `Record[Out]`. Each stage traverses the
    * input independently; v1 will fuse via `Fanout`. */
  def run(inputs: Iterable[I @scala.annotation.unchecked.uncheckedVariance]): Record[Out] = {
    var rec: Record[Any] = Record.empty
    var i = 0
    val seq = inputs.asInstanceOf[Iterable[Any]]
    while i < stages.length do {
      rec = rec & stages(i)(seq)
      i += 1
    }
    rec.asInstanceOf[Record[Out]]
  }
}

object ScanFlow {

  /** Start a flow that will consume values of type `I`. */
  def over[I]: ScanFlow[I, Any] = new ScanFlow[I, Any](Vector.empty)
}
