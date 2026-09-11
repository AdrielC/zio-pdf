package zio.pdf.arrow

import zio.test.*
import zio.pdf.pipe.Pipe
import volga.free.Nat
import volga.syntax.smc.V

/**
 * Cedar-shaped legal document DAG — arrows-paper operators (`?`, `&&&`, `|||`),
 * five fronds, attestation join, certification tail.
 *
 * Run: `testOnly zio.pdf.arrow.LegalDocumentCedarDemo`
 */
object LegalDocumentCedarDemo extends ZIOSpecDefault {

  private type Doc = Array[Byte]
  private type Frond5 = (Doc, (Doc, (Doc, (Doc, Doc))))

  private def pass: Pipe[Doc, Doc] = Pipe(identity)

  private def node(name: String, inPorts: Int = 1, outPorts: Int = 1): PipelineSpine.Spine[Doc, Doc] =
    PipelineSpine.node(name, inPorts, outPorts)(pass)

  private def chain(names: String*): PipelineSpine.Spine[Doc, Doc] =
    names.map(n => node(n)).reduceLeft(_ >>> _)

  private def mergeFronds: Pipe[Frond5, Doc] =
    Pipe { case (a, (_, (_, (_, _)))) => a }

  /** Intake trunk + privilege [[GraphSummary.TestOp]] gate (arrows conditional). */
  private def trunk: PipelineSpine.Spine[Doc, Doc] =
    chain(
      "vault-intake",
      "malware-sweep",
      "format-sniff",
      "decrypt-unseal",
      "ocr-rasterize",
      "layout-reconstruct",
      "text-normalize",
      GraphSummary.TestOp,
      "doc-classify"
    )

  private val structureFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "clause-segment",
    "heading-bind",
    "list-renumber",
    "section-index",
    "toc-align",
    "defined-term-scan",
    "term-graph",
    "structure-attest"
  )

  private val entityFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "party-extract",
    "role-infer",
    "signature-block",
    "duty-parse",
    "deadline-extract",
    "amount-normalize",
    "escalator-detect",
    "entity-attest"
  )

  private val complianceFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "privilege-detect",
    "work-product-tag",
    "pii-scan",
    "redaction-mask",
    "retention-class",
    "hold-notice",
    "compliance-attest"
  )

  private val citationFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "statute-resolve",
    "code-edition",
    "case-cite-parse",
    "reporter-normalize",
    "exhibit-detect",
    "amendment-chain",
    "citation-attest"
  )

  private val riskFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "indemnity-parse",
    "cap-floor-extract",
    "insurance-clause",
    "term-expiry",
    "arbitration-clause",
    "governing-law",
    "risk-attest"
  )

  /** Cedar canopy — [[GraphSummary.FanOp]] fans classify into five fronds. */
  private def canopy: PipelineSpine.Spine[Doc, Frond5] =
    structureFrond &&& (entityFrond &&& (complianceFrond &&& (citationFrond &&& riskFrond)))

  private def join: PipelineSpine.Spine[Frond5, Doc] =
    PipelineSpine.node(GraphSummary.MergeOp, 5, 1)(mergeFronds)

  private def tail: PipelineSpine.Spine[Doc, Doc] =
    chain("evidence-reconcile", "conflict-resolve", "audit-seal", "legal-export-bundle")

  /** Full cedar: trunk → ? → &&& canopy → ||| join → tail. */
  def legalCedarSpine: PipelineSpine.Spine[Doc, Doc] =
    trunk >>> canopy >>> join >>> tail

  def spec: Spec[Any, Any] = suite("LegalDocumentCedarDemo")(
    test("cedar spine: ? test, &&& fan-out, ||| join attests, export tail") {
      val formats = PipelineSpine.render("legal-doc-cedar", legalCedarSpine, "brief.pdf")
      val m       = formats.mermaid
      val edges   = formats.wiring._2

      println("\n===== LEGAL CEDAR (spine + arrow operators) =====\n")
      println(m)

      assertTrue(
        m.contains("?"),
        m.contains("&&&"),
        m.contains("|||"),
        edges.contains("doc-classify" -> GraphSummary.FanOp),
        edges.contains(GraphSummary.FanOp -> "clause-segment"),
        edges.contains("structure-attest" -> GraphSummary.MergeOp),
        edges.contains("entity-attest" -> GraphSummary.MergeOp),
        edges.contains("compliance-attest" -> GraphSummary.MergeOp),
        edges.contains("citation-attest" -> GraphSummary.MergeOp),
        edges.contains("risk-attest" -> GraphSummary.MergeOp),
        edges.contains(GraphSummary.MergeOp -> "evidence-reconcile"),
        edges.contains("audit-seal" -> "legal-export-bundle"),
        !m.contains("⟨in⟩")
      )
    },
    test("arrows wiring: of2 join chain to export (producer → consumer)") {
      type V1 = V[Nat.`1`]
      val formats = PipelineFlow.wiring("legal-join", "structure-attest", "entity-attest") {
        PipelineFlow.prop.of2 { (s: V1, e: V1) =>
          ArrowSyntax.node("structure-attest", 1, 0)(s)
          ArrowSyntax.node("entity-attest", 1, 0)(e)
          val merge = ArrowSyntax.node(GraphSummary.MergeOp, 0, 1)()
          ArrowSyntax.node("legal-export-bundle", 1, 0)(merge)
        }
      }
      println("\n===== LEGAL JOIN (produce → consume) =====\n")
      println(formats.mermaid)
      assertTrue(
        formats.wiring._2.contains("structure-attest" -> "structure-attest"),
        formats.wiring._2.contains("entity-attest" -> "entity-attest"),
        formats.wiring._2.exists(e => e._1 == GraphSummary.MergeOp && e._2 == "legal-export-bundle")
      )
    },
  )
}
