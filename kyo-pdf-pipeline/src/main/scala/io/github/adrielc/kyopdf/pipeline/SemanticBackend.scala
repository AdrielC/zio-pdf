package io.github.adrielc.kyopdf.pipeline

import kyo.*
import StepRegistry.{Error, ErrorCode}
import PdfSteps.*

/** Rendering/OCR are explicit interpreter capabilities, on either platform. */
trait SemanticBackend:
  def locate(file: PdfFile, settings: Select): Selection < (Abort[Error] & Sync)
  def redact(file: PdfFile, plan: RedactionPlan, settings: Edit): SemanticBackend.Artifact < (Abort[Error] & Sync)
  def replace(file: PdfFile, settings: Replace): SemanticBackend.Artifact < (Abort[Error] & Sync)
  def ocr(file: PdfFile, settings: Ocr): SemanticBackend.Artifact < (Abort[Error] & Sync)
  def cleanup(file: PdfFile, settings: Edit): SemanticBackend.Artifact < (Abort[Error] & Sync)

object SemanticBackend:
  final case class Artifact(file: PdfFile, inputSha256: String, outputSha256: String, operation: String, verified: Boolean)
  val unavailable: SemanticBackend = new SemanticBackend:
    private def missing[A]: A < Abort[Error] = Abort.fail(Error(ErrorCode.Unavailable, "No semantic PDF backend configured"))
    def locate(file: PdfFile, settings: Select) = missing[Selection]
    def redact(file: PdfFile, plan: RedactionPlan, settings: Edit) = missing[Artifact]
    def replace(file: PdfFile, settings: Replace) = missing[Artifact]
    def ocr(file: PdfFile, settings: Ocr) = missing[Artifact]
    def cleanup(file: PdfFile, settings: Edit) = missing[Artifact]
