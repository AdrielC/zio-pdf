package zio.pdf.arrow

import zio.test.*
import zio.pdf.pipe.Pipe

/**
 * Cedar-shaped legal document processing DAG — narrow intake trunk, wide parallel
 * analysis canopy (five major fronds, each with sub-tiers), five certification tips.
 *
 * Run: `testOnly zio.pdf.arrow.LegalDocumentCedarDemo`
 */
object LegalDocumentCedarDemo extends ZIOSpecDefault {

  private type Doc = Array[Byte]
  private def pass: Pipe[Doc, Doc] = Pipe(identity)

  private def node(name: String): PipelineSpine.Spine[Doc, Doc] =
    PipelineSpine.node(name, 1, 1)(pass)

  private def chain(names: String*): PipelineSpine.Spine[Doc, Doc] =
    names.map(node).reduceLeft(_ >>> _)

  /** Intake trunk — tapers up into the canopy fork. */
  private def trunk: PipelineSpine.Spine[Doc, Doc] =
    chain(
      "vault-intake",
      "malware-sweep",
      "format-sniff",
      "decrypt-unseal",
      "ocr-rasterize",
      "layout-reconstruct",
      "text-normalize",
      "doc-classify"
    )

  /** Structure frond — geometry, clauses, definitions. */
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

  /** Entity frond — parties, duties, economics. */
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

  /** Compliance frond — privilege, PII, retention. */
  private val complianceFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "privilege-detect",
    "work-product-tag",
    "pii-scan",
    "redaction-mask",
    "retention-class",
    "hold-notice",
    "compliance-attest"
  )

  /** Citation frond — statutes, cases, exhibits. */
  private val citationFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "statute-resolve",
    "code-edition",
    "case-cite-parse",
    "reporter-normalize",
    "exhibit-detect",
    "amendment-chain",
    "citation-attest"
  )

  /** Risk frond — liability, termination, dispute. */
  private val riskFrond: PipelineSpine.Spine[Doc, Doc] = chain(
    "indemnity-parse",
    "cap-floor-extract",
    "insurance-clause",
    "term-expiry",
    "arbitration-clause",
    "governing-law",
    "risk-attest"
  )

  /** Cedar canopy — five parallel fronds from the classify fork. */
  private def canopy: PipelineSpine.Spine[Doc, (Doc, (Doc, (Doc, (Doc, Doc))))] =
    structureFrond &&& (entityFrond &&& (complianceFrond &&& (citationFrond &&& riskFrond)))

  /** Full cedar DAG. */
  def legalCedarSpine: PipelineSpine.Spine[Doc, (Doc, (Doc, (Doc, (Doc, Doc))))] =
    trunk >>> canopy

  def spec: Spec[Any, Any] = suite("LegalDocumentCedarDemo")(
    test("print cedar-shaped legal document processing DAG") {
      val formats = PipelineSpine.render("legal-doc-cedar", legalCedarSpine, "brief.pdf")
      val names   = ScanGraph.nodeNames(PipelineSpine.analyze(legalCedarSpine))
      val edges   = formats.wiring._2
      val m       = formats.mermaid

      println("\n===== LEGAL DOCUMENT CEDAR DAG =====")
      println(s"nodes=${names.size} edges=${edges.size}")
      println("\n" + m)

      assertTrue(
        names.size >= 44,
        edges.size >= 45,
        m.contains("brief.pdf --> vault-intake"),
        m.contains("doc-classify --> clause-segment"),
        m.contains("doc-classify --> party-extract"),
        m.contains("doc-classify --> privilege-detect"),
        m.contains("doc-classify --> statute-resolve"),
        m.contains("doc-classify --> indemnity-parse"),
        m.contains("structure-attest"),
        m.contains("risk-attest"),
        !m.contains("⟨in⟩")
      )
    }
  )
}
