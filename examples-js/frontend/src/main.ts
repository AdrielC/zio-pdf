import "@fontsource-variable/jetbrains-mono";
import "@fontsource-variable/manrope";
import type { PDFDocumentLoadingTask, PDFDocumentProxy, PDFPageProxy, RenderTask } from "pdfjs-dist";
import PdfPreviewWorker from "pdfjs-dist/legacy/build/pdf.worker.min.mjs?worker";
import {
  ArrowRightLeft,
  Braces,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  FileSearch,
  FileUp,
  Minus,
  PanelTopOpen,
  Plus,
  Quote,
  RotateCcw,
  ScanLine,
  ScanSearch,
  Search,
  ShieldCheck,
  TextSearch,
  TriangleAlert,
  X,
  createIcons
} from "lucide";
import type { Analysis, Citation, FontResource, TextRecoveryRequest, TransformExecution, TransformPlan, WorkflowExecution } from "zio-pdf-demo";
import type { ScanWorkerMessage } from "./scan-protocol";
import "./styles.css";

const fileInput = document.querySelector<HTMLInputElement>("#file-input")!;
const dropZone = document.querySelector<HTMLLabelElement>("#drop-zone")!;
const workbench = document.querySelector<HTMLElement>("#workbench")!;
const analyzeButton = document.querySelector<HTMLButtonElement>("#analyze-button")!;
const analyzeButtonLabel = document.querySelector<HTMLElement>("#analyze-button-label")!;
const resetButton = document.querySelector<HTMLButtonElement>("#reset-button")!;
const fileFacts = document.querySelector<HTMLElement>("#file-facts")!;
const inputStatus = document.querySelector<HTMLElement>("#input-status")!;
const sourceStatus = document.querySelector<HTMLElement>("#source-status")!;
const dropTitle = document.querySelector<HTMLElement>("#drop-title")!;
const dropCopy = document.querySelector<HTMLElement>("#drop-copy")!;
const emptyState = document.querySelector<HTMLElement>("#empty-state")!;
const emptyStateCopy = document.querySelector<HTMLElement>("#empty-state-copy")!;
const report = document.querySelector<HTMLElement>("#report")!;
const reportTitle = document.querySelector<HTMLElement>("#report-title")!;
const reportState = document.querySelector<HTMLElement>("#report-state")!;
const errorState = document.querySelector<HTMLElement>("#error-state")!;
const errorMessage = document.querySelector<HTMLElement>("#error-message")!;
const observationList = document.querySelector<HTMLUListElement>("#observation-list")!;
const scanSteps = Array.from(document.querySelectorAll<HTMLElement>("[data-stage]"));
const textPreview = document.querySelector<HTMLElement>("#text-preview")!;
const textPreviewMeta = document.querySelector<HTMLElement>("#text-preview-meta")!;
const textPreviewValue = document.querySelector<HTMLElement>("#text-preview-value")!;
const ocrPanel = document.querySelector<HTMLElement>("#ocr-panel")!;
const ocrButton = document.querySelector<HTMLButtonElement>("#ocr-button")!;
const ocrStatus = document.querySelector<HTMLElement>("#ocr-status")!;
const ocrValue = document.querySelector<HTMLElement>("#ocr-value")!;
const previewStage = document.querySelector<HTMLElement>("#preview-stage")!;
const previewEmpty = document.querySelector<HTMLElement>("#preview-empty")!;
const previewHost = document.querySelector<HTMLElement>("#preview-host")!;
const previewError = document.querySelector<HTMLElement>("#preview-error")!;
const previewErrorMessage = document.querySelector<HTMLElement>("#preview-error-message")!;
const previewState = document.querySelector<HTMLElement>("#preview-state")!;
const previewPage = document.querySelector<HTMLElement>("#preview-page")!;
const previewEngine = document.querySelector<HTMLElement>("#preview-engine")!;
const previewCanvas = document.querySelector<HTMLCanvasElement>("#preview-canvas")!;
const previewScroll = document.querySelector<HTMLElement>("#preview-scroll")!;
const previewPrevious = document.querySelector<HTMLButtonElement>("#preview-previous")!;
const previewNext = document.querySelector<HTMLButtonElement>("#preview-next")!;
const previewPageControl = document.querySelector<HTMLElement>("#preview-page-control")!;
const previewZoomOut = document.querySelector<HTMLButtonElement>("#preview-zoom-out")!;
const previewZoomIn = document.querySelector<HTMLButtonElement>("#preview-zoom-in")!;
const previewZoomLabel = document.querySelector<HTMLElement>("#preview-zoom-label")!;
const citationLayer = document.querySelector<HTMLElement>("#citation-layer")!;
const citationPin = document.querySelector<HTMLButtonElement>("#citation-pin")!;
const citationPinLabel = document.querySelector<HTMLElement>("#citation-pin-label")!;
const citationCard = document.querySelector<HTMLElement>("#citation-card")!;
const citationPageLabel = document.querySelector<HTMLElement>("#citation-page-label")!;
const citationOrdinal = document.querySelector<HTMLElement>("#citation-ordinal")!;
const citationCount = document.querySelector<HTMLElement>("#citation-count")!;
const citationExcerpt = document.querySelector<HTMLElement>("#citation-excerpt")!;
const citationProvenance = document.querySelector<HTMLElement>("#citation-provenance")!;
const citationBounds = document.querySelector<HTMLElement>("#citation-bounds")!;
const citationClose = document.querySelector<HTMLButtonElement>("#citation-close")!;
const transformPlan = document.querySelector<HTMLElement>("#transform-plan")!;
const planBadge = document.querySelector<HTMLElement>("#plan-badge")!;
const planNodes = document.querySelector<HTMLOListElement>("#plan-nodes")!;
const planProfile = document.querySelector<HTMLElement>("#plan-profile")!;
const planScope = document.querySelector<HTMLElement>("#plan-scope")!;
const planContent = document.querySelector<HTMLElement>("#plan-content")!;
const planSource = document.querySelector<HTMLElement>("#plan-source")!;
const planRemap = document.querySelector<HTMLInputElement>("#plan-remap")!;
const planTokenize = document.querySelector<HTMLInputElement>("#plan-tokenize")!;
const planSourceFont = document.querySelector<HTMLSelectElement>("#plan-source-font")!;
const planTargetFont = document.querySelector<HTMLSelectElement>("#plan-target-font")!;
const planTokenizer = document.querySelector<HTMLSelectElement>("#plan-tokenizer")!;
const fontBindings = document.querySelector<HTMLElement>("#font-bindings")!;
const swapFontsButton = document.querySelector<HTMLButtonElement>("#swap-fonts")!;
const runTransformButton = document.querySelector<HTMLButtonElement>("#run-transform")!;
const transformDownload = document.querySelector<HTMLAnchorElement>("#transform-download")!;
const transformStatus = document.querySelector<HTMLElement>("#transform-status")!;
const workflowPlan = document.querySelector<HTMLElement>("#workflow-plan")!;
const workflowBadge = document.querySelector<HTMLElement>("#workflow-badge")!;
const workflowCopy = document.querySelector<HTMLElement>("#workflow-copy")!;
const workflowStatus = document.querySelector<HTMLElement>("#workflow-status")!;
const workflowDownload = document.querySelector<HTMLAnchorElement>("#workflow-download")!;
const runLinearizeButton = document.querySelector<HTMLButtonElement>("#run-linearize")!;
const runAppendButton = document.querySelector<HTMLButtonElement>("#run-append")!;
const runFlattenButton = document.querySelector<HTMLButtonElement>("#run-flatten")!;
const runWatermarkButton = document.querySelector<HTMLButtonElement>("#run-watermark")!;
const watermarkTextInput = document.querySelector<HTMLInputElement>("#watermark-text")!;
const watermarkDiagonalInput = document.querySelector<HTMLInputElement>("#watermark-diagonal")!;
const runMergeButton = document.querySelector<HTMLButtonElement>("#run-merge")!;
const runExtractButton = document.querySelector<HTMLButtonElement>("#run-extract")!;
const runRotateButton = document.querySelector<HTMLButtonElement>("#run-rotate")!;
const runSplitButton = document.querySelector<HTMLButtonElement>("#run-split")!;
const runThumbnailButton = document.querySelector<HTMLButtonElement>("#run-thumbnail")!;
const mergeFileInput = document.querySelector<HTMLInputElement>("#merge-file")!;
const pageFromInput = document.querySelector<HTMLInputElement>("#page-from")!;
const pageToInput = document.querySelector<HTMLInputElement>("#page-to")!;
const pageRotateSelect = document.querySelector<HTMLSelectElement>("#page-rotate")!;
const workflowDownloads = document.querySelector<HTMLElement>("#workflow-downloads")!;
const mappingRoute = document.querySelector<HTMLElement>("#mapping-route")!;
const mappingRouteCopy = document.querySelector<HTMLElement>("#mapping-route-copy")!;
const mappingRouteState = document.querySelector<HTMLElement>("#mapping-route-state")!;
const mappingSourceName = document.querySelector<HTMLElement>("#mapping-source-name")!;
const mappingSourceDetail = document.querySelector<HTMLElement>("#mapping-source-detail")!;
const mappingTargetName = document.querySelector<HTMLElement>("#mapping-target-name")!;
const mappingTargetDetail = document.querySelector<HTMLElement>("#mapping-target-detail")!;
const fontInventory = document.querySelector<HTMLElement>("#font-inventory")!;
const fontInventorySummary = document.querySelector<HTMLElement>("#font-inventory-summary")!;
const fontInventoryList = document.querySelector<HTMLUListElement>("#font-inventory-list")!;
const fontFilter = document.querySelector<HTMLInputElement>("#font-filter")!;
const scanProgressWrap = document.querySelector<HTMLElement>("#scan-progress-wrap")!;
const scanProgress = document.querySelector<HTMLProgressElement>("#scan-progress")!;
const scanProgressLabel = document.querySelector<HTMLElement>("#scan-progress-label")!;
const scanProgressDetail = document.querySelector<HTMLElement>("#scan-progress-detail")!;
const scanFlow = document.querySelector<HTMLElement>("#scan-flow")!;
const scanCommand = document.querySelector<HTMLElement>("#scan-command")!;

let selectedFile: File | undefined;
let previewGeneration = 0;
let currentPreviewPage = 1;
let previewTotalPages = 0;
let previewLoadingTask: PDFDocumentLoadingTask | undefined;
let previewDocument: PDFDocumentProxy | undefined;
let previewPageProxy: PDFPageProxy | undefined;
let previewRenderTask: RenderTask | undefined;
let previewWorker: Worker | undefined;
let previewZoom = 1;
let previewRanges: Array<readonly [number, number]> = [];
let activeScanWorker: Worker | undefined;
let activeScanCancel: (() => void) | undefined;
let activeTransformWorker: Worker | undefined;
let activeTransformCancel: (() => void) | undefined;
let scanGeneration = 0;
let transformGeneration = 0;
let ocrGeneration = 0;
let planGeneration = 0;
let discoveredFonts: FontResource[] = [];
let expandedFont = "";
let citations: Citation[] = [];
let textRecoveryRequests: TextRecoveryRequest[] = [];
let citationOpen = false;
let transformDownloadUrl = "";
let workflowDownloadUrl = "";
let lastInspectionEncrypted = false;
let lastInspectionHasForm = false;
let lastInspectionPages = 1;
let workflowExtraUrls: string[] = [];
let workflowGeneration = 0;
let activeWorkflowWorker: Worker | undefined;
let activeWorkflowCancel: (() => void) | undefined;

const PREVIEW_RANGE_BYTES = 256 * 1024;
const MAX_PREVIEW_RANGE_BYTES = 1024 * 1024;

type RunPhase = "idle" | "ready" | "bridge" | "evidence" | "complete" | "error";
type ObservationState = "positive" | "neutral" | "review";

const runMessages: Record<RunPhase, string> = {
  idle: "Select a PDF to preview.",
  ready: "Ready to inspect.",
  bridge: "Starting inspection…",
  evidence: "Inspecting document…",
  complete: "Inspection complete.",
  error: "Inspection failed."
};

const stageOrder = ["source", "decode", "evidence", "report"] as const;
const iconSet = {
  ArrowRightLeft,
  Braces,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  FileSearch,
  FileUp,
  Minus,
  PanelTopOpen,
  Plus,
  Quote,
  RotateCcw,
  ScanLine,
  ScanSearch,
  Search,
  ShieldCheck,
  TextSearch,
  TriangleAlert,
  X
};

function renderIcons(): void {
  createIcons({ icons: iconSet });
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB"];
  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function setText(selector: string, value: string): void {
  document.querySelector<HTMLElement>(selector)!.textContent = value;
}

function setRunPhase(phase: RunPhase): void {
  workbench.dataset.run = phase;
  scanSteps.forEach((step, index) => {
    if (phase === "error") step.dataset.state = index === 0 ? "complete" : "error";
    else if (phase === "complete") step.dataset.state = "complete";
    else if (phase === "evidence") step.dataset.state = index < 3 ? "active" : "idle";
    else if (phase === "bridge" || phase === "ready") step.dataset.state = index === 0 ? "ready" : "idle";
    else step.dataset.state = "idle";
  });
}

function setPreviewState(state: "idle" | "loading" | "ready" | "error", message: string): void {
  previewStage.dataset.state = state;
  previewState.dataset.state = state;
  previewEngine.textContent = message;
  if (state === "idle") previewPage.textContent = "No file";
  if (state === "loading") previewPage.textContent = "Loading preview";
  if (state === "error") previewPage.textContent = "Preview failed";
}

function setPreviewPage(page: number): void {
  currentPreviewPage = page;
  const label = previewTotalPages > 0 ? `Page ${page} / ${previewTotalPages}` : `Page ${page}`;
  previewPage.textContent = label;
  previewPageControl.textContent = label;
  previewPrevious.disabled = page <= 1;
  previewNext.disabled = previewTotalPages === 0 || page >= previewTotalPages;
}

function citationForPage(page: number): Citation | undefined {
  return citations.find((citation) => citation.page === page && citation.excerpt.length > 0);
}

function textRecoveryForPage(page: number): TextRecoveryRequest | undefined {
  return textRecoveryRequests.find((request) => request.page === page);
}

function renderTextRecoveryAction(): void {
  const request = textRecoveryForPage(currentPreviewPage);
  ocrPanel.hidden = !request || report.hidden;
  if (!request) return;
  ocrStatus.textContent = `No usable native text on page ${request.page} (object #${request.pageObjectNumber}). OCR is an explicit local recovery step for this page only.`;
}

function previewScrollContainer(): HTMLElement | undefined {
  return previewScroll;
}

function previewPageElement(_page: number): HTMLElement | undefined {
  return previewStage.dataset.state === "ready" ? previewCanvas : undefined;
}

function positionCitationOverlay(): void {
  const page = previewPageElement(currentPreviewPage);
  if (!page) return;

  const stage = previewStage.getBoundingClientRect();
  const bounds = page.getBoundingClientRect();
  const clamp = (value: number, minimum: number, maximum: number) => Math.max(minimum, Math.min(value, maximum));
  const left = clamp(bounds.right - stage.left - 42, 8, Math.max(8, stage.width - 42));
  const top = clamp(bounds.top - stage.top + 12, 8, Math.max(8, stage.height - 42));
  citationLayer.style.setProperty("--citation-x", `${Math.round(left)}px`);
  citationLayer.style.setProperty("--citation-y", `${Math.round(top)}px`);
}

function renderCitationOverlay(): void {
  const citation = citationForPage(currentPreviewPage);
  citationLayer.hidden = !citation || previewStage.dataset.state !== "ready";
  if (!citation) {
    citationCard.hidden = true;
    renderTextRecoveryAction();
    return;
  }

  const citationIndex = citations.indexOf(citation);
  citationPinLabel.textContent = `C${String(citation.page).padStart(2, "0")}`;
  citationPin.setAttribute("aria-label", `Open evidence for page ${citation.page}`);
  citationPin.setAttribute("aria-expanded", String(citationOpen));
  citationCard.hidden = !citationOpen;
  citationPageLabel.textContent = `Page ${citation.page}`;
  citationOrdinal.textContent = `C${String(citation.page).padStart(3, "0")}`;
  citationCount.textContent = `${citationIndex + 1} of ${citations.length} cited pages`;
  citationExcerpt.textContent = citation.excerpt;
  citationProvenance.textContent = `page object #${citation.pageObjectNumber} · content ${citation.contentObjectNumbers.map((number) => `#${number}`).join(", ") || "direct"}`;
  citationBounds.textContent = citation.truncated ? "bounded excerpt" : "complete page text";
  positionCitationOverlay();
  renderTextRecoveryAction();
}

function destroyPreview(): void {
  previewGeneration += 1;
  ocrGeneration += 1;
  currentPreviewPage = 1;
  previewTotalPages = 0;
  previewRenderTask?.cancel();
  previewRenderTask = undefined;
  previewPageProxy?.cleanup();
  previewPageProxy = undefined;
  if (previewLoadingTask) void previewLoadingTask.destroy().catch(() => undefined);
  previewLoadingTask = undefined;
  previewDocument = undefined;
  previewRanges = [];
  previewZoom = 1;
  delete previewStage.dataset.firstPageMs;
  previewCanvas.width = 0;
  previewCanvas.height = 0;
  previewCanvas.removeAttribute("style");
  previewZoomLabel.textContent = "Fit";
  previewPrevious.disabled = true;
  previewNext.disabled = true;
  previewZoomOut.disabled = true;
  previewZoomIn.disabled = true;
  previewHost.hidden = true;
  previewError.hidden = true;
  citationLayer.hidden = true;
  citationOpen = false;
}

function previewBytesRead(): number {
  const ranges = [...previewRanges].sort((left, right) => left[0] - right[0]);
  let total = 0;
  let cursor = -1;
  for (const [begin, end] of ranges) {
    const start = Math.max(begin, cursor);
    if (end > start) total += end - start;
    cursor = Math.max(cursor, end);
  }
  return total;
}

function recordPreviewRange(begin: number, end: number, total: number): void {
  previewRanges.push([begin, end]);
  const read = Math.min(total, previewBytesRead());
  previewEngine.textContent = `${formatBytes(read)} of ${formatBytes(total)} read`;
}

function setPreviewZoomLabel(): void {
  previewZoomLabel.textContent = previewZoom === 1 ? "Fit" : `${Math.round(previewZoom * 100)}%`;
}

async function renderPreviewPage(pageNumber: number, generation = previewGeneration): Promise<void> {
  if (!previewDocument || generation !== previewGeneration) return;

  const nextPage = Math.max(1, Math.min(pageNumber, previewDocument.numPages));
  previewRenderTask?.cancel();
  previewPageProxy?.cleanup();
  previewPageProxy = undefined;
  setPreviewPage(nextPage);
  setPreviewState("loading", `Rendering page ${nextPage}…`);

  try {
    const page = await previewDocument.getPage(nextPage);
    if (generation !== previewGeneration) return;
    previewPageProxy = page;
    const natural = page.getViewport({ scale: 1 });
    const availableWidth = Math.max(240, previewScroll.clientWidth - 48);
    const fitScale = Math.max(0.25, Math.min(2.5, availableWidth / natural.width));
    const viewport = page.getViewport({ scale: fitScale * previewZoom });
    const outputScale = Math.min(window.devicePixelRatio || 1, 2);
    const context = previewCanvas.getContext("2d", { alpha: false });
    if (!context) throw new Error("This browser could not create the PDF canvas.");

    previewCanvas.width = Math.floor(viewport.width * outputScale);
    previewCanvas.height = Math.floor(viewport.height * outputScale);
    previewCanvas.style.width = `${Math.floor(viewport.width)}px`;
    previewCanvas.style.height = `${Math.floor(viewport.height)}px`;
    previewCanvas.setAttribute("aria-label", `Rendered PDF page ${nextPage} of ${previewDocument.numPages}`);
    const transform = outputScale === 1 ? undefined : [outputScale, 0, 0, outputScale, 0, 0];
    previewRenderTask = page.render({ canvas: null, canvasContext: context, viewport, transform });
    await previewRenderTask.promise;
    if (generation !== previewGeneration) return;

    previewRenderTask = undefined;
    previewEmpty.hidden = true;
    previewHost.hidden = false;
    previewZoomOut.disabled = false;
    previewZoomIn.disabled = false;
    setPreviewState("ready", `${formatBytes(previewBytesRead())} of ${formatBytes(selectedFile?.size ?? 0)} read`);
    setPreviewPage(nextPage);
    renderCitationOverlay();
  } catch (error) {
    if (generation !== previewGeneration || (error instanceof Error && error.name === "RenderingCancelledException")) return;
    throw error;
  }
}

async function openPreview(file: File): Promise<void> {
  const startedAt = performance.now();
  const generation = ++previewGeneration;
  previewRenderTask?.cancel();
  if (previewLoadingTask) void previewLoadingTask.destroy().catch(() => undefined);
  previewLoadingTask = undefined;
  previewDocument = undefined;
  previewPageProxy = undefined;
  previewRanges = [];
  previewZoom = 1;
  setPreviewZoomLabel();
  previewEmpty.hidden = false;
  previewEmpty.querySelector("strong")!.textContent = "Loading preview…";
  previewEmpty.querySelector("span")!.textContent = `Reads are capped at ${formatBytes(MAX_PREVIEW_RANGE_BYTES)}.`;
  previewError.hidden = true;
  previewHost.hidden = true;
  setPreviewState("loading", "Opening PDF…");

  try {
    const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
    previewWorker ??= new PdfPreviewWorker();
    pdfjs.GlobalWorkerOptions.workerPort = previewWorker;
    class BlobRangeTransport extends pdfjs.PDFDataRangeTransport {
      private stopped = false;

      constructor() {
        super(file.size, null, false, file.name);
      }

      requestDataRange(begin: number, end: number): void {
        const length = end - begin;
        if (length <= 0 || length > MAX_PREVIEW_RANGE_BYTES) {
          this.onDataRange(begin, null);
          return;
        }
        void file.slice(begin, end).arrayBuffer().then(
          (buffer) => {
            if (this.stopped || generation !== previewGeneration) return;
            recordPreviewRange(begin, end, file.size);
            this.onDataRange(begin, new Uint8Array(buffer));
          },
          () => this.onDataRange(begin, null)
        );
      }

      abort(): void {
        this.stopped = true;
      }
    }

    const range = new BlobRangeTransport();
    previewLoadingTask = pdfjs.getDocument({
      range,
      rangeChunkSize: PREVIEW_RANGE_BYTES,
      disableStream: true,
      disableAutoFetch: true,
      stopAtErrors: false,
      isOffscreenCanvasSupported: true
    });
    const document = await previewLoadingTask.promise;
    if (generation !== previewGeneration) return;
    previewDocument = document;
    previewTotalPages = document.numPages;
    await renderPreviewPage(1, generation);
    if (generation === previewGeneration && previewStage.dataset.state === "ready") {
      const elapsedMs = Math.max(0, Math.round(performance.now() - startedAt));
      previewStage.dataset.firstPageMs = String(elapsedMs);
      previewEngine.textContent = `${previewEngine.textContent} · ${elapsedMs.toLocaleString()} ms`;
    }
  } catch (error) {
    if (generation !== previewGeneration) return;
    const message = error instanceof Error ? error.message : "The local previewer could not render this PDF.";
    previewHost.hidden = true;
    previewEmpty.hidden = true;
    previewError.hidden = false;
    previewErrorMessage.textContent = message;
    inputStatus.textContent = `${message} The worker-based evidence scan is still available.`;
    setPreviewState("error", "Preview failed");
  }
}

function resetScanUi(): void {
  delete analyzeButton.dataset.running;
  analyzeButtonLabel.textContent = "Run Evidence Scan";
  scanProgressWrap.hidden = true;
  scanProgress.value = 0;
  scanProgress.textContent = "0%";
  scanProgressLabel.textContent = "Streaming bytes";
  scanProgressDetail.textContent = "0%";
}

function stopActiveScan(): boolean {
  const wasRunning = activeScanWorker !== undefined;
  scanGeneration += 1;
  if (activeScanCancel) activeScanCancel();
  else activeScanWorker?.terminate();
  activeScanCancel = undefined;
  activeScanWorker = undefined;
  resetScanUi();
  return wasRunning;
}

function clearTransformDownload(): void {
  if (transformDownloadUrl) URL.revokeObjectURL(transformDownloadUrl);
  transformDownloadUrl = "";
  transformDownload.hidden = true;
  transformDownload.removeAttribute("href");
}

function clearWorkflowDownload(): void {
  if (workflowDownloadUrl) URL.revokeObjectURL(workflowDownloadUrl);
  workflowDownloadUrl = "";
  workflowDownload.hidden = true;
  workflowDownload.removeAttribute("href");
  for (const url of workflowExtraUrls) URL.revokeObjectURL(url);
  workflowExtraUrls = [];
  workflowDownloads.replaceChildren();
  workflowDownloads.hidden = true;
}

function pageRange(): { fromPage: number; toPage: number } {
  const fromPage = Math.max(1, Number.parseInt(pageFromInput.value, 10) || 1);
  const toPage = Math.max(fromPage, Number.parseInt(pageToInput.value, 10) || fromPage);
  return { fromPage, toPage };
}

function stopActiveWorkflow(): boolean {
  const wasRunning = activeWorkflowWorker !== undefined;
  workflowGeneration += 1;
  if (activeWorkflowCancel) activeWorkflowCancel();
  else activeWorkflowWorker?.terminate();
  activeWorkflowCancel = undefined;
  activeWorkflowWorker = undefined;
  return wasRunning;
}

function syncWorkflowControls(): void {
  const ready = selectedFile !== undefined && !lastInspectionEncrypted;
  runLinearizeButton.disabled = !ready;
  runAppendButton.disabled = !ready;
  runFlattenButton.disabled = !ready || !lastInspectionHasForm;
  runWatermarkButton.disabled = !ready || watermarkTextInput.value.trim() === "";
  runExtractButton.disabled = !ready;
  runRotateButton.disabled = !ready;
  runSplitButton.disabled = !ready || lastInspectionPages < 2;
  runThumbnailButton.disabled = !ready;
  runMergeButton.disabled = !ready || mergeFileInput.files?.[0] === undefined;
  workflowBadge.textContent = lastInspectionEncrypted ? "Encrypted" : ready ? "Ready" : "Run inspection";
  workflowCopy.textContent = lastInspectionEncrypted
    ? "This filing is encrypted. zio-pdf will not rewrite it."
    : "Encrypted PDFs cannot be processed. Flatten bakes forms; watermark stamps Helvetica text.";
}

type WorkflowKind = "linearize" | "append" | "flatten" | "merge" | "extract" | "rotate" | "split" | "watermark";

function workflowInWorker(
  kind: WorkflowKind,
  file: File,
  id: number,
  secondary?: File
): Promise<WorkflowExecution> {
  const worker = new Worker(new URL("./scan.worker.ts", import.meta.url), {
    type: "module",
    name: "zio-pdf-workflow"
  });
  activeWorkflowWorker = worker;

  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (): void => {
      if (activeWorkflowWorker === worker) activeWorkflowWorker = undefined;
      if (activeWorkflowCancel === cancel) activeWorkflowCancel = undefined;
      worker.terminate();
    };
    const cancel = (): void => {
      if (settled) return;
      settled = true;
      finish();
      reject(new DOMException("PDF workflow cancelled", "AbortError"));
    };
    activeWorkflowCancel = cancel;

    worker.onmessage = (event: MessageEvent<ScanWorkerMessage>) => {
      const message = event.data;
      if (message.id !== id || id !== workflowGeneration) return;
      if (message.kind === "workflow-complete") {
        settled = true;
        finish();
        resolve(message.execution);
      } else if (message.kind === "error") {
        settled = true;
        finish();
        reject(new Error(message.message));
      }
    };
    worker.onerror = (event) => {
      settled = true;
      finish();
      reject(new Error(event.message || "The PDF workflow worker could not start."));
    };

    if (kind === "merge" && secondary) worker.postMessage({ kind, id, file, secondary });
    else if (kind === "extract") worker.postMessage({ kind, id, file, ...pageRange() });
    else if (kind === "rotate") worker.postMessage({ kind, id, file, degrees: Number.parseInt(pageRotateSelect.value, 10) || 90, ...pageRange() });
    else if (kind === "watermark") {
      worker.postMessage({
        kind,
        id,
        file,
        text: watermarkTextInput.value.trim() || "FILED",
        diagonal: watermarkDiagonalInput.checked,
        ...pageRange()
      });
    }
    else if (kind !== "merge") worker.postMessage({ kind, id, file });
    else reject(new Error("Choose a second PDF to merge."));
  });
}

async function executeWorkflow(kind: WorkflowKind): Promise<void> {
  if (!selectedFile) return;
  if (activeWorkflowWorker) {
    stopActiveWorkflow();
    workflowStatus.textContent = "Workflow cancelled.";
    syncWorkflowControls();
    return;
  }

  const file = selectedFile;
  const generation = ++workflowGeneration;
  const secondary = mergeFileInput.files?.[0];
  clearWorkflowDownload();
  workflowPlan.dataset.state = "running";
  workflowBadge.textContent = "Running";
  workflowStatus.textContent = `Running ${kind} in a worker…`;

  try {
    const execution = await workflowInWorker(kind, file, generation, secondary);
    if (generation !== workflowGeneration) return;
    const blob = new Blob(execution.chunks.map((chunk) => chunk.buffer as ArrayBuffer), { type: "application/pdf" });
    workflowDownloadUrl = URL.createObjectURL(blob);
    workflowDownload.href = workflowDownloadUrl;
    workflowDownload.download = `${file.name.replace(/\.pdf$/i, "") || "document"}.${kind}.pdf`;
    workflowDownload.hidden = false;
    workflowPlan.dataset.state = "complete";
    workflowBadge.textContent = "Complete";
    const prefix = execution.firstPagePrefixBytes === undefined
      ? ""
      : ` First-page prefix ${execution.firstPagePrefixBytes.toLocaleString()} bytes.`;
    if (execution.documents && execution.documents.length > 1) {
      workflowDownloads.hidden = false;
      for (const part of execution.documents) {
        const extra = new Blob(part.chunks.map((chunk) => chunk.buffer as ArrayBuffer), { type: "application/pdf" });
        const url = URL.createObjectURL(extra);
        workflowExtraUrls.push(url);
        const link = window.document.createElement("a");
        link.className = "transform-download";
        link.href = url;
        link.download = part.name;
        link.textContent = part.name;
        workflowDownloads.append(link);
      }
    }
    if (kind === "flatten" || kind === "watermark") {
      if (kind === "flatten") {
        const baked = execution.appearancesPlaced ?? 0;
        const fallbacks = execution.textFallbacks ?? 0;
        const leftover = execution.formRemaining === true;
        workflowStatus.textContent = leftover
          ? `Flatten wrote ${execution.outputBytes.toLocaleString()} bytes, but a form dictionary is still present.`
          : `Flatten baked ${baked} appearance${baked === 1 ? "" : "s"}` +
            (fallbacks > 0 ? ` and ${fallbacks} text fallback${fallbacks === 1 ? "" : "s"}` : "") +
            `. Form stripped. Preview updated.`;
      } else {
        const label = execution.watermarkText || watermarkTextInput.value.trim() || "FILED";
        workflowStatus.textContent = `Watermarked “${label}”` +
          (execution.watermarkDiagonal === false ? "" : " on the diagonal") +
          `. Preview updated.`;
      }
      const previewFile = new File([blob], `${file.name.replace(/\.pdf$/i, "") || "document"}.${kind}.pdf`, {
        type: "application/pdf"
      });
      void openPreview(previewFile);
    } else {
      workflowStatus.textContent = execution.pageCount
        ? `${kind} wrote ${execution.pageCount} PDFs.`
        : `${kind} wrote ${execution.outputBytes.toLocaleString()} bytes.${prefix}`;
    }
  } catch (error) {
    if (generation !== workflowGeneration) return;
    if (error instanceof DOMException && error.name === "AbortError") return;
    workflowPlan.dataset.state = "error";
    workflowBadge.textContent = "Error";
    workflowStatus.textContent = error instanceof Error ? error.message : "The workflow could not run.";
  } finally {
    if (generation === workflowGeneration) syncWorkflowControls();
  }
}

async function executeThumbnail(): Promise<void> {
  if (!selectedFile) return;
  clearWorkflowDownload();
  workflowPlan.dataset.state = "running";
  workflowBadge.textContent = "Running";
  workflowStatus.textContent = "Attaching a first-page /Thumb…";
  try {
    const { ZioPdfDemo } = await import("zio-pdf-demo");
    const bytes = new Uint8Array(await selectedFile.arrayBuffer());
    const canvas = currentPreviewCanvas();
    let gray: Uint8Array | undefined;
    let width = 64;
    let height = 64;
    if (canvas) {
      width = Math.max(1, canvas.width);
      height = Math.max(1, canvas.height);
      const image = canvas.getContext("2d")?.getImageData(0, 0, width, height);
      if (image) {
        gray = new Uint8Array(width * height);
        for (let index = 0; index < gray.length; index += 1) {
          const offset = index * 4;
          gray[index] = Math.round(0.299 * image.data[offset] + 0.587 * image.data[offset + 1] + 0.114 * image.data[offset + 2]);
        }
      }
    }
    const output = await ZioPdfDemo.attachFirstPageThumbnail(bytes, gray, width, height);
    const blob = new Blob([output.buffer as ArrayBuffer], { type: "application/pdf" });
    workflowDownloadUrl = URL.createObjectURL(blob);
    workflowDownload.href = workflowDownloadUrl;
    workflowDownload.download = `${selectedFile.name.replace(/\.pdf$/i, "") || "document"}.thumb.pdf`;
    workflowDownload.hidden = false;
    workflowPlan.dataset.state = "complete";
    workflowBadge.textContent = "Complete";
    workflowStatus.textContent = gray
      ? `Attached a ${width}×${height} rendered /Thumb.`
      : "Attached a placeholder /Thumb.";
  } catch (error) {
    workflowPlan.dataset.state = "error";
    workflowBadge.textContent = "Error";
    workflowStatus.textContent = error instanceof Error ? error.message : "The thumbnail could not be attached.";
  } finally {
    syncWorkflowControls();
  }
}

function stopActiveTransform(): boolean {
  const wasRunning = activeTransformWorker !== undefined;
  transformGeneration += 1;
  if (activeTransformCancel) activeTransformCancel();
  else activeTransformWorker?.terminate();
  activeTransformCancel = undefined;
  activeTransformWorker = undefined;
  runTransformButton.textContent = "Run Pipeline";
  return wasRunning;
}

function resetTransformUi(): void {
  stopActiveTransform();
  clearTransformDownload();
  runTransformButton.disabled = true;
  transformStatus.textContent = "Run inspection to configure the pipeline.";
}

function updateScanProgress(loadedBytes: number, totalBytes: number): void {
  const safeTotal = Math.max(0, totalBytes);
  const safeLoaded = Math.max(0, Math.min(loadedBytes, safeTotal));
  const percent = safeTotal === 0 ? 0 : Math.min(100, Math.round((safeLoaded / safeTotal) * 100));
  scanProgressWrap.hidden = false;
  scanProgress.value = percent;
  scanProgress.textContent = `${percent}%`;
  scanProgressLabel.textContent = safeLoaded >= safeTotal && safeTotal > 0 ? "Building report" : "Streaming bytes";
  scanProgressDetail.textContent = `${formatBytes(safeLoaded)} / ${formatBytes(safeTotal)} · ${percent}%`;
  sourceStatus.textContent = safeLoaded >= safeTotal && safeTotal > 0 ? "Blob consumed once" : `${percent}% streamed`;
  inputStatus.textContent = safeLoaded >= safeTotal && safeTotal > 0
    ? "All bytes consumed. Finalizing typed evidence in the worker…"
    : `Streaming ${formatBytes(safeLoaded)} of ${formatBytes(safeTotal)} through Scala.js…`;
}

function scanInWorker(file: File, id: number): Promise<Analysis> {
  const worker = new Worker(new URL("./scan.worker.ts", import.meta.url), {
    type: "module",
    name: "zio-pdf-evidence"
  });
  activeScanWorker = worker;

  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (): void => {
      if (activeScanWorker === worker) activeScanWorker = undefined;
      if (activeScanCancel === cancel) activeScanCancel = undefined;
      worker.terminate();
    };
    const cancel = (): void => {
      if (settled) return;
      settled = true;
      finish();
      reject(new DOMException("Evidence scan cancelled", "AbortError"));
    };
    activeScanCancel = cancel;

    worker.onmessage = (event: MessageEvent<ScanWorkerMessage>) => {
      const message = event.data;
      if (message.id !== id || id !== scanGeneration) return;
      if (message.kind === "progress") {
        const runPhase = message.phase as RunPhase;
        setRunPhase(runPhase);
        updateScanProgress(message.loadedBytes, message.totalBytes);
      } else if (message.kind === "complete") {
        settled = true;
        finish();
        resolve(message.analysis);
      } else if (message.kind === "error") {
        settled = true;
        finish();
        reject(new Error(message.message));
      }
    };

    worker.onerror = (event) => {
      settled = true;
      finish();
      reject(new Error(event.message || "The PDF evidence worker could not start."));
    };

    worker.postMessage({ kind: "analyze", id, file });
  });
}

function resetReport(): void {
  resetTransformUi();
  emptyState.hidden = false;
  report.hidden = true;
  errorState.hidden = true;
  textPreview.hidden = true;
  textPreviewValue.textContent = "";
  ocrPanel.hidden = true;
  ocrButton.disabled = false;
  ocrStatus.textContent = "";
  ocrValue.textContent = "";
  observationList.replaceChildren();
  discoveredFonts = [];
  expandedFont = "";
  citations = [];
  textRecoveryRequests = [];
  citationOpen = false;
  fontFilter.value = "";
  fontInventoryList.replaceChildren();
  fontInventorySummary.textContent = "";
  fontInventory.hidden = true;
  mappingRoute.hidden = true;
  citationLayer.hidden = true;
  reportState.textContent = "idle";
  reportState.dataset.state = "idle";
  emptyStateCopy.textContent = "No inspection results.";
  transformPlan.hidden = true;
  transformPlan.removeAttribute("open");
  lastInspectionEncrypted = false;
  lastInspectionHasForm = false;
  lastInspectionPages = 1;
  stopActiveWorkflow();
  workflowPlan.hidden = true;
  workflowPlan.removeAttribute("open");
  workflowBadge.textContent = "Run inspection";
  workflowStatus.textContent = "Run inspection to enable write workflows.";
  workflowCopy.textContent = "Encrypted PDFs cannot be processed. Flatten bakes forms; watermark stamps Helvetica text.";
  clearWorkflowDownload();
  mergeFileInput.value = "";
  [runLinearizeButton, runAppendButton, runFlattenButton, runWatermarkButton, runMergeButton, runExtractButton, runRotateButton, runSplitButton, runThumbnailButton].forEach((button) => {
    button.disabled = true;
  });
  planSourceFont.replaceChildren(new Option("Run inspection first", ""));
  planTargetFont.replaceChildren(new Option("Run inspection first", ""));
  planSourceFont.disabled = true;
  planTargetFont.disabled = true;
  resetScanUi();
  scanFlow.hidden = true;
  setRunPhase("idle");
}

function resetWorkspace(): void {
  stopActiveScan();
  selectedFile = undefined;
  fileInput.value = "";
  analyzeButton.disabled = true;
  resetButton.disabled = true;
  delete dropZone.dataset.selected;
  dropTitle.textContent = "Choose a PDF";
  dropCopy.textContent = "or drop a file";
  sourceStatus.textContent = "Blob stream";
  fileFacts.hidden = true;
  scanCommand.hidden = true;
  inputStatus.textContent = runMessages.idle;
  destroyPreview();
  previewEmpty.hidden = false;
  previewEmpty.querySelector("strong")!.textContent = "No PDF selected";
  previewEmpty.querySelector("span")!.textContent = "Choose a PDF above.";
  setPreviewState("idle", "Waiting for a PDF");
  resetReport();
}

function selectFile(file: File | undefined): void {
  if (!file) {
    resetWorkspace();
    return;
  }

  stopActiveScan();
  selectedFile = file;
  resetReport();
  analyzeButton.disabled = false;
  resetButton.disabled = false;
  emptyStateCopy.textContent = "Run Evidence Scan.";
  transformPlan.hidden = false;
  workflowPlan.hidden = false;
  dropZone.dataset.selected = "true";
  dropTitle.textContent = "Replace PDF";
  dropCopy.textContent = "choose another file";
  setText("#file-name", file.name);
  setText("#file-size", formatBytes(file.size));
  sourceStatus.textContent = "Blob stream · not started";
  fileFacts.hidden = false;
  scanCommand.hidden = false;
  inputStatus.textContent = runMessages.ready;
  setRunPhase("ready");
  void openPreview(file);
}

function addObservation(label: string, value: string, detail: string, state: ObservationState): void {
  const item = document.createElement("li");
  item.dataset.state = state;
  const labelElement = document.createElement("span");
  const valueElement = document.createElement("strong");
  const detailElement = document.createElement("small");
  labelElement.textContent = label;
  valueElement.textContent = value;
  detailElement.textContent = detail;
  item.append(labelElement, valueElement, detailElement);
  observationList.append(item);
}

function operationLabel(operation: string): { title: string; detail: string } {
  switch (operation) {
    case "remap-existing-fonts":
      return {
        title: "RemapExistingFonts",
        detail: "Rebinds page resources only when encodings and metrics match."
      };
    case "tokenize-text":
      return {
        title: "TokenizeText",
        detail: "Extracts characters or words by page."
      };
    default:
      return { title: operation, detail: "Named, inspectable transform operation." };
  }
}

function planConfig(): {
  remapExistingFonts: boolean;
  sourceFont: string;
  targetFont: string;
  tokenize: boolean;
  tokenizer: "characters" | "words";
} {
  return {
    remapExistingFonts: planRemap.checked,
    sourceFont: planSourceFont.value.trim(),
    targetFont: planTargetFont.value.trim(),
    tokenize: planTokenize.checked,
    tokenizer: planTokenizer.value === "words" ? "words" : "characters"
  };
}

function syncPlanControls(): void {
  const remapEnabled = planRemap.checked;
  const tokenizeEnabled = planTokenize.checked;
  const running = activeTransformWorker !== undefined;
  const candidates = groupDocumentFonts(discoveredFonts).filter(isUnambiguousRemap);
  fontBindings.dataset.disabled = String(!remapEnabled);
  planRemap.disabled = running;
  planTokenize.disabled = running;
  planSourceFont.disabled = running || !remapEnabled || candidates.length === 0;
  planTargetFont.disabled = running || !remapEnabled || candidates.length === 0;
  swapFontsButton.disabled = running || !remapEnabled || candidates.length < 2;
  planTokenizer.disabled = running || !tokenizeEnabled;

  const source = groupForFont(planSourceFont.value);
  const target = groupForFont(planTargetFont.value);
  const remapReady = !remapEnabled || Boolean(
    source && isUnambiguousRemap(source)
      && target && isUnambiguousRemap(target)
      && source.resource.baseFont !== target.resource.baseFont
  );
  const hasOperation = remapEnabled || tokenizeEnabled;
  runTransformButton.disabled = running ? false : lastInspectionEncrypted || !selectedFile || !hasOperation || !remapReady;

  if (activeTransformWorker) return;
  const pipelineState = transformPlan.dataset.state;
  if (pipelineState === "complete" || pipelineState === "error") return;
  if (!hasOperation) transformStatus.textContent = "Select at least one pipeline step.";
  else if (remapEnabled && candidates.length < 2) transformStatus.textContent = "This PDF needs two unambiguous font resources to test replacement.";
  else if (remapEnabled && !remapReady) transformStatus.textContent = "Choose different source and replacement fonts.";
  else if (remapEnabled) transformStatus.textContent = "Ready to verify. No output is created unless this font pair passes.";
  else transformStatus.textContent = "Ready to run. Browser transforms are capped at 64 MiB.";
}

type FontGroup = { resource: FontResource; records: FontResource[]; remapCandidates: FontResource[] };

function groupDocumentFonts(fonts: FontResource[]): FontGroup[] {
  const grouped = new Map<string, FontResource[]>();
  fonts.forEach((font) => {
    const records = grouped.get(font.baseFont) ?? [];
    records.push(font);
    grouped.set(font.baseFont, records);
  });
  return Array.from(grouped.values()).map((records) => {
    const remapCandidates = records.filter((font) => font.existingResourceRemapCandidate);
    return {
      resource: remapCandidates[0] ?? records[0],
      records,
      remapCandidates
    };
  });
}

function isUnambiguousRemap(group: FontGroup): boolean {
  return group.remapCandidates.length === 1;
}

function groupForFont(fontName: string): FontGroup | undefined {
  return groupDocumentFonts(discoveredFonts).find(({ resource }) => resource.baseFont === fontName);
}

function syncFontSelectors(groups: FontGroup[]): void {
  const candidates = groups.filter(isUnambiguousRemap);
  const names = candidates.map(({ resource }) => resource.baseFont);
  const currentSource = names.includes(planSourceFont.value) ? planSourceFont.value : names[0] ?? "";
  const currentTarget = names.includes(planTargetFont.value) && planTargetFont.value !== currentSource
    ? planTargetFont.value
    : names.find((name) => name !== currentSource) ?? names[0] ?? "";

  const renderOptions = (select: HTMLSelectElement, selected: string): void => {
    select.replaceChildren(...candidates.map(({ resource }) => {
      const subtype = resource.subtype ? ` /${resource.subtype}` : "";
      const option = new Option(`${resource.baseFont} · #${resource.objectNumber}${subtype}`, resource.baseFont);
      option.selected = resource.baseFont === selected;
      return option;
    }));
    if (candidates.length === 0) select.append(new Option("No testable fonts", ""));
  };

  renderOptions(planSourceFont, currentSource);
  renderOptions(planTargetFont, currentTarget);
}

function endpointDescription(fontName: string, group: FontGroup | undefined): string {
  if (!group) return `“${fontName || "unnamed"}” is a manually entered name, not a discovered resource.`;
  const root = group.resource;
  const descendants = group.records.filter((font) => font.objectNumber !== root.objectNumber);
  const descendantDetail = descendants.length
    ? ` + ${descendants.map((font) => `#${font.objectNumber} /${font.subtype ?? "unknown"}`).join(", ")}`
    : "";
  return `#${root.objectNumber} /${root.subtype ?? "unknown"}${descendantDetail}`;
}

function renderMappingRoute(): void {
  if (discoveredFonts.length === 0) {
    mappingRoute.hidden = true;
    return;
  }

  mappingRoute.hidden = false;
  const sourceName = planSourceFont.value.trim() || "SourceFace";
  const targetName = planTargetFont.value.trim() || "TargetFace";
  const source = groupForFont(sourceName);
  const target = groupForFont(targetName);
  const candidatePair = Boolean(planRemap.checked && source && isUnambiguousRemap(source) && target && isUnambiguousRemap(target));
  const remapDisabled = !planRemap.checked;

  mappingRoute.dataset.state = remapDisabled ? "inactive" : candidatePair ? "candidate" : "unresolved";
  mappingSourceName.textContent = sourceName;
  mappingSourceDetail.textContent = endpointDescription(sourceName, source);
  mappingTargetName.textContent = targetName;
  mappingTargetDetail.textContent = endpointDescription(targetName, target);
  mappingRouteState.textContent = remapDisabled ? "disabled" : candidatePair ? "check required" : "select fonts";
  mappingRouteCopy.textContent = remapDisabled
    ? "Enable font replacement to add this step."
    : candidatePair
    ? "Run verifies encoding, glyph widths, and Unicode mapping before changing the PDF."
    : "Choose different testable source and replacement fonts.";
  renderIcons();
}

function renderFontInventory(fonts: FontResource[]): void {
  discoveredFonts = fonts;
  if (fonts.length === 0 && report.hidden) {
    fontInventory.hidden = true;
    mappingRoute.hidden = true;
    return;
  }
  fontInventory.hidden = false;
  fontInventoryList.replaceChildren();

  const groups = groupDocumentFonts(fonts);
  const candidates = groups.filter(isUnambiguousRemap);
  syncFontSelectors(groups);
  syncPlanControls();
  const filter = fontFilter.value.trim().toLowerCase();
  const visibleGroups = groups.filter(({ resource, records }) =>
    !filter || [resource.baseFont, ...records.map((font) => `${font.objectNumber} ${font.subtype ?? ""}`)]
      .join(" ")
      .toLowerCase()
      .includes(filter)
  );
  fontInventorySummary.textContent = filter
    ? `${visibleGroups.length} shown / ${groups.length} fonts`
    : `${groups.length} fonts · ${candidates.length} testable`;

  if (groups.length === 0) {
    const empty = document.createElement("li");
    empty.className = "font-inventory-empty";
    empty.textContent = "No direct /BaseFont dictionaries were discovered in this document.";
    fontInventoryList.append(empty);
    renderMappingRoute();
    return;
  }

  if (visibleGroups.length === 0) {
    const empty = document.createElement("li");
    empty.className = "font-inventory-empty";
    empty.textContent = "No document font resources match this filter.";
    fontInventoryList.append(empty);
    renderMappingRoute();
    return;
  }

  visibleGroups.forEach((group) => {
    const { resource, records, remapCandidates } = group;
    const item = document.createElement("li");
    const facts = document.createElement("div");
    const name = document.createElement("strong");
    const detail = document.createElement("small");
    const actions = document.createElement("div");
    const inspect = document.createElement("button");
    const status = isUnambiguousRemap(group)
      ? "testable"
      : remapCandidates.length > 1
      ? "ambiguous"
      : "inspect only";
    const related = records
      .filter((font) => font.objectNumber !== resource.objectNumber)
      .map((font) => `#${font.objectNumber} /${font.subtype ?? "unknown"}`);

    item.dataset.candidate = String(isUnambiguousRemap(group));
    item.dataset.expanded = String(expandedFont === resource.baseFont);
    name.textContent = resource.baseFont;
    detail.textContent = `#${resource.objectNumber} /${resource.subtype ?? "unknown"}${related.length ? ` + ${related.join(", ")}` : ""} · ${status}`;
    facts.append(name, detail);

    actions.className = "font-actions";
    inspect.className = "font-inspect";
    inspect.type = "button";
    inspect.title = `Inspect ${resource.baseFont} resources`;
    inspect.setAttribute("aria-label", `Inspect ${resource.baseFont} resources`);
    inspect.setAttribute("aria-expanded", String(expandedFont === resource.baseFont));
    const inspectIcon = document.createElement("i");
    inspectIcon.dataset.lucide = "braces";
    inspectIcon.setAttribute("aria-hidden", "true");
    inspect.append(inspectIcon);
    inspect.addEventListener("click", () => {
      expandedFont = expandedFont === resource.baseFont ? "" : resource.baseFont;
      renderFontInventory(discoveredFonts);
    });

    const statusLabel = document.createElement("span");
    statusLabel.className = "font-support";
    statusLabel.textContent = status;
    actions.append(statusLabel, inspect);
    item.append(facts, actions);

    if (expandedFont === resource.baseFont) {
      const inspector = document.createElement("div");
      const heading = document.createElement("span");
      const structure = document.createElement("code");
      const check = document.createElement("small");
      inspector.className = "font-detail";
      heading.textContent = isUnambiguousRemap(group)
        ? "rebindable resource root"
        : remapCandidates.length > 1
        ? "multiple resource roots"
        : "extract-only resource";
      structure.textContent = [resource, ...records.filter((font) => font.objectNumber !== resource.objectNumber)]
        .map((font, index) => `${index === 0 ? "root" : "linked"} #${font.objectNumber} /${font.subtype ?? "unknown"}`)
        .join("  ·  ");
      check.textContent = isUnambiguousRemap(group)
        ? "The execution gate compares encoding, CID metrics, and ToUnicode before changing any reference."
        : remapCandidates.length > 1
        ? "This BaseFont name resolves to multiple candidate roots, so the name-based transform API rejects it instead of guessing."
        : "This subtype is inventoried for inspection but cannot be selected for an existing-resource remap.";
      inspector.append(heading, structure, check);
      item.append(inspector);
    }
    fontInventoryList.append(item);
  });

  renderMappingRoute();
  renderIcons();
}

function renderTransformPlan(plan: TransformPlan): void {
  transformPlan.dataset.state = "ready";
  planBadge.textContent = "Ready";
  planNodes.replaceChildren();
  const operations = plan.operations.length === 0 ? ["identity"] : plan.operations;
  operations.forEach((operation, index) => {
    const node = document.createElement("li");
    const ordinal = document.createElement("span");
    const content = document.createElement("div");
    const title = document.createElement("strong");
    const detail = document.createElement("small");
    const label = operation === "identity"
      ? { title: "Identity", detail: "No transform operations selected; the immutable plan is empty." }
      : operationLabel(operation);
    ordinal.textContent = String(index + 1).padStart(2, "0");
    title.textContent = label.title;
    detail.textContent = label.detail;
    content.append(title, detail);
    node.append(ordinal, content);
    planNodes.append(node);
  });
  planScope.textContent = plan.requiresMaterializedDocument ? "document graph" : "identity";
  planContent.textContent = plan.readsContentStreams ? "reads content streams" : "no content read";
  planSource.textContent = plan.code;
  planProfile.hidden = false;
  syncPlanControls();
}

async function compileTransformPlan(): Promise<void> {
  const generation = ++planGeneration;
  syncPlanControls();
  transformPlan.dataset.state = "compiling";
  planBadge.textContent = "Updating";
  try {
    const { ZioPdfDemo } = await import("zio-pdf-demo");
    const config = planConfig();
    const plan = ZioPdfDemo.inspectTransformPlan(
      config.remapExistingFonts,
      config.sourceFont,
      config.targetFont,
      config.tokenize,
      config.tokenizer
    );
    if (generation === planGeneration) renderTransformPlan(plan);
  } catch (error) {
    if (generation === planGeneration) {
      transformPlan.dataset.state = "error";
      planBadge.textContent = "Plan error";
      transformStatus.textContent = error instanceof Error ? error.message : "The pipeline could not be compiled.";
    }
  }
}

function transformInWorker(file: File, id: number): Promise<TransformExecution> {
  const config = planConfig();
  const worker = new Worker(new URL("./scan.worker.ts", import.meta.url), {
    type: "module",
    name: "zio-pdf-transform"
  });
  activeTransformWorker = worker;

  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (): void => {
      if (activeTransformWorker === worker) activeTransformWorker = undefined;
      if (activeTransformCancel === cancel) activeTransformCancel = undefined;
      worker.terminate();
    };
    const cancel = (): void => {
      if (settled) return;
      settled = true;
      finish();
      reject(new DOMException("PDF transform cancelled", "AbortError"));
    };
    activeTransformCancel = cancel;

    worker.onmessage = (event: MessageEvent<ScanWorkerMessage>) => {
      const message = event.data;
      if (message.id !== id || id !== transformGeneration) return;
      if (message.kind === "transform-complete") {
        settled = true;
        finish();
        resolve(message.execution);
      } else if (message.kind === "error") {
        settled = true;
        finish();
        reject(new Error(message.message));
      }
    };
    worker.onerror = (event) => {
      settled = true;
      finish();
      reject(new Error(event.message || "The PDF transform worker could not start."));
    };

    worker.postMessage({ kind: "transform", id, file, ...config });
  });
}

async function executeTransform(): Promise<void> {
  if (!selectedFile) return;
  if (activeTransformWorker) {
    stopActiveTransform();
    transformPlan.dataset.state = "ready";
    planBadge.textContent = "Ready";
    transformStatus.textContent = "Pipeline cancelled.";
    syncPlanControls();
    return;
  }

  const file = selectedFile;
  const generation = ++transformGeneration;
  clearTransformDownload();
  transformPlan.dataset.state = "running";
  planBadge.textContent = "Running";
  runTransformButton.disabled = false;
  runTransformButton.textContent = "Stop Pipeline";
  transformStatus.textContent = "Verifying the selected resources and rendering the output in a worker…";

  try {
    const pending = transformInWorker(file, generation);
    syncPlanControls();
    const execution = await pending;
    if (generation !== transformGeneration) return;
    const blob = new Blob(execution.chunks.map((chunk) => chunk.buffer as ArrayBuffer), { type: "application/pdf" });
    transformDownloadUrl = URL.createObjectURL(blob);
    transformDownload.href = transformDownloadUrl;
    transformDownload.download = `${file.name.replace(/\.pdf$/i, "") || "document"}.transformed.pdf`;
    transformDownload.hidden = false;

    const outcomes: string[] = [];
    if (execution.sourceFont && execution.targetFont) {
      outcomes.push(`${execution.resourceBindingsRewritten} bindings changed from ${execution.sourceFont} to ${execution.targetFont}`);
    }
    if (execution.tokenPages > 0) outcomes.push(`${execution.tokenCount.toLocaleString()} tokens across ${execution.tokenPages} pages`);
    transformStatus.textContent = `${formatBytes(execution.outputBytes)} ready. ${outcomes.join(" · ")}`;
    transformPlan.dataset.state = "complete";
    planBadge.textContent = "Complete";
  } catch (error) {
    if (generation !== transformGeneration) return;
    if (error instanceof DOMException && error.name === "AbortError") return;
    transformPlan.dataset.state = "error";
    planBadge.textContent = "Blocked";
    const message = error instanceof Error ? error.message : "The pipeline could not run.";
    transformStatus.textContent = message;
    if (planRemap.checked) {
      mappingRoute.dataset.state = "unresolved";
      mappingRouteState.textContent = "incompatible";
      mappingRouteCopy.textContent = "This pair cannot be safely rebound. Choose another replacement font.";
    }
  } finally {
    if (generation === transformGeneration) {
      runTransformButton.textContent = "Run Pipeline";
      syncPlanControls();
    }
  }
}

function renderAnalysis(analysis: Analysis): void {
  const { inspection, content } = analysis;
  emptyState.hidden = true;
  errorState.hidden = true;
  report.hidden = false;
  reportTitle.textContent = inspection.status === "accepted" ? "Inspection Complete" : "Inspection Rejected";
  reportState.textContent = inspection.status;
  reportState.dataset.state = inspection.status;

  setText("#preflight-result", inspection.violation ?? inspection.status);
  setText("#validation-result", analysis.valid ? "valid" : "needs review");
  setText("#policy-result", analysis.strictPolicyPassed ? "passed" : "blocked");
  setText("#objects-result", String(inspection.elementsRead));
  setText("#elapsed-result", `${Math.max(0, Math.round(analysis.elapsedMs)).toLocaleString()} ms`);
  setText("#digest-result", `SHA-256 ${analysis.sha256.slice(0, 12)}…`);

  renderFontInventory(inspection.fonts);
  lastInspectionEncrypted = inspection.encrypted || analysis.cannotProcess === true;
  lastInspectionHasForm = inspection.acroFormObject !== undefined || inspection.acroFormFields > 0;
  lastInspectionPages = Math.max(1, content.pages);
  pageFromInput.value = "1";
  pageToInput.max = String(lastInspectionPages);
  pageFromInput.max = String(lastInspectionPages);
  pageToInput.value = String(lastInspectionPages);
  transformPlan.setAttribute("open", "");
  workflowPlan.setAttribute("open", "");
  workflowPlan.dataset.state = lastInspectionEncrypted ? "error" : "ready";
  syncWorkflowControls();
  void compileTransformPlan();
  citations = content.citations.filter((citation) => citation.excerpt.length > 0);
  textRecoveryRequests = content.textRecoveryRequests;
  const evidenceLastPage = Math.max(
    0,
    ...citations.map((citation) => citation.page),
    ...textRecoveryRequests.map((request) => request.page)
  );
  previewTotalPages = Math.max(previewTotalPages, evidenceLastPage);
  setPreviewPage(currentPreviewPage);
  citationOpen = citations.length > 0;
  renderCitationOverlay();

  observationList.replaceChildren();
  addObservation(
    "Linearization",
    inspection.linearizedObject === undefined ? "Not linearized" : `Object #${inspection.linearizedObject}`,
    inspection.linearizedObject === undefined ? "No fast-web marker" : "Linearized dictionary found",
    inspection.linearizedObject === undefined ? "neutral" : "positive"
  );
  addObservation(
    "PDF/A profile",
    inspection.pdfA3bDeclared ? "PDF/A-3b declared" : inspection.pdfAObject === undefined ? "No declaration" : "PDF/A metadata",
    inspection.pdfAObject === undefined
      ? "No XMP PDF/A declaration"
      : `Part ${inspection.pdfAPart ?? "?"}, conformance ${inspection.pdfAConformance ?? "?"}`,
    inspection.pdfA3bDeclared ? "positive" : inspection.pdfAObject === undefined ? "neutral" : "review"
  );
  addObservation(
    "Thumbnail",
    inspection.thumbnailPageObject === undefined ? "No embedded thumb" : `Page object #${inspection.thumbnailPageObject}`,
    inspection.thumbnailPageObject === undefined ? "The visual panel is the rendered preview" : `Image object #${inspection.thumbnailImageObject ?? "?"}`,
    inspection.thumbnailPageObject === undefined ? "neutral" : "positive"
  );
  addObservation(
    "AcroForm",
    inspection.acroFormObject === undefined && inspection.acroFormFields === 0
      ? "No form dictionary"
      : `${inspection.acroFormFields} field${inspection.acroFormFields === 1 ? "" : "s"}`,
    inspection.acroFormObject === undefined
      ? "No catalog /AcroForm"
      : `Form object #${inspection.acroFormObject}${inspection.acroFormNeedAppearances ? " · NeedAppearances" : ""}`,
    inspection.acroFormFields > 0 ? "review" : "neutral"
  );
  const encryptedBlocker = (analysis.processingBlockers ?? []).find((blocker) => blocker.kind === "Encrypted");
  addObservation(
    "Encryption",
    analysis.cannotProcess || inspection.encrypted ? "Cannot process" : "Not encrypted",
    encryptedBlocker?.reason ??
      (inspection.encrypted
        ? inspection.encryptionObject === undefined
          ? "Trailer has a direct /Encrypt dictionary"
          : `Trailer references encryption object #${inspection.encryptionObject}`
        : "No /Encrypt entry in the trailer"),
    analysis.cannotProcess || inspection.encrypted ? "review" : "positive"
  );
  const fontGroups = groupDocumentFonts(inspection.fonts);
  const remapCandidates = fontGroups.filter(({ resource }) => resource.existingResourceRemapCandidate);
  addObservation(
    "Font resources",
    `${fontGroups.length} found / ${remapCandidates.length} rebindable`,
    fontGroups.length === 0
      ? "No direct /BaseFont dictionary was available for an existing-resource remap."
      : "The transform panel is seeded from these exact /BaseFont values.",
    remapCandidates.length > 0 ? "positive" : "neutral"
  );
  addObservation(
    "Recovered text",
    `${content.textPages} / ${content.pages} pages, ${content.textCharacters.toLocaleString()} chars`,
    textRecoveryRequests.length > 0
      ? `${textRecoveryRequests.length} pages have explicit local-OCR recovery requests`
      : content.textPages === 0 ? "No literal Tj/TJ text recovered" : "Literal page-text extractor",
    textRecoveryRequests.length > 0 ? "review" : content.textPages === 0 ? "neutral" : "positive"
  );
  addObservation(
    "Fused evidence run",
    `${analysis.decodedEvents.toLocaleString()} decoded events in ${Math.max(0, Math.round(analysis.elapsedMs)).toLocaleString()} ms`,
    "One Blob read merges the parser, inspection, text summary, validation, policy, and SHA-256 observers.",
    "positive"
  );
  addObservation(
    "Image XObjects",
    `${content.images} image streams`,
    content.imageEvidence,
    content.images > 0 ? "positive" : "neutral"
  );
  addObservation(
    "Attachments and tables",
    `${content.attachments} attachment streams / ${content.tableCandidates} candidates`,
    `${content.attachmentEvidence} ${content.tableCandidateEvidence}`,
    content.tableCandidates > 0 ? "review" : content.images > 0 ? "positive" : "neutral"
  );

  if (content.textPreview) {
    textPreviewValue.textContent = content.textPreview;
    textPreviewMeta.textContent = `${content.textCharacters.toLocaleString()} characters`;
    textPreview.hidden = false;
  }
  renderTextRecoveryAction();
}

function currentPreviewCanvas(): HTMLCanvasElement | undefined {
  return Array.from(previewHost.querySelectorAll<HTMLCanvasElement>("canvas"))
    .sort((left, right) => right.width * right.height - left.width * left.height)[0];
}

async function recognizeCurrentPage(): Promise<void> {
  if (!textRecoveryForPage(currentPreviewPage)) {
    ocrStatus.textContent = `Page ${currentPreviewPage} already has native text evidence; no OCR recovery is needed.`;
    return;
  }
  const canvas = currentPreviewCanvas();
  if (!canvas) {
    ocrStatus.textContent = "Open a rendered page before requesting OCR.";
    return;
  }

  const generation = ++ocrGeneration;
  ocrButton.disabled = true;
  ocrStatus.textContent = `Preparing page ${currentPreviewPage} for local OCR…`;
  ocrValue.textContent = "";

  try {
    const { createWorker } = await import("tesseract.js");
    const worker = await createWorker("eng", 1, {
      logger: (message) => {
        if (generation === ocrGeneration && message.status) {
          const percent = typeof message.progress === "number" ? ` ${Math.round(message.progress * 100)}%` : "";
          ocrStatus.textContent = `${message.status}${percent}`;
        }
      }
    });
    try {
      const result = await worker.recognize(canvas);
      if (generation !== ocrGeneration) return;
      const text = result.data.text.trim();
      ocrStatus.textContent = text ? `OCR complete for page ${currentPreviewPage}.` : `No words recognized on page ${currentPreviewPage}.`;
      ocrValue.textContent = text || "No OCR text returned for this rendered page.";
    } finally {
      await worker.terminate();
    }
  } catch (error) {
    if (generation === ocrGeneration) {
      ocrStatus.textContent = error instanceof Error ? error.message : "OCR could not start in this browser.";
    }
  } finally {
    if (generation === ocrGeneration) ocrButton.disabled = false;
  }
}

async function analyze(): Promise<void> {
  if (!selectedFile) return;
  if (activeScanWorker) {
    stopActiveScan();
    emptyState.hidden = false;
    inputStatus.textContent = "Evidence scan cancelled. The preview remains available.";
    sourceStatus.textContent = "Blob stream · cancelled";
    setRunPhase("ready");
    return;
  }

  const file = selectedFile;
  const generation = ++scanGeneration;
  analyzeButton.dataset.running = "true";
  analyzeButtonLabel.textContent = "Stop Scan";
  analyzeButton.disabled = false;
  resetButton.disabled = false;
  emptyState.hidden = true;
  errorState.hidden = true;
  report.hidden = true;
  scanFlow.hidden = false;
  scanProgressWrap.hidden = false;
  scanProgress.value = 0;
  scanProgress.textContent = "0%";
  scanProgressDetail.textContent = `0 B / ${formatBytes(file.size)} · 0%`;
  sourceStatus.textContent = "Starting worker";
  setRunPhase("bridge");
  inputStatus.textContent = runMessages.bridge;

  try {
    const analysis = await scanInWorker(file, generation);
    if (generation !== scanGeneration) return;
    renderAnalysis(analysis);
    updateScanProgress(file.size, file.size);
    scanProgressLabel.textContent = "Evidence ready";
    inputStatus.textContent = runMessages.complete;
    sourceStatus.textContent = "Blob consumed once";
    setRunPhase("complete");
  } catch (error) {
    if (generation !== scanGeneration) return;
    errorState.hidden = false;
    reportState.textContent = "error";
    reportState.dataset.state = "error";
    errorMessage.textContent = error instanceof Error
      ? `${error.message} Choose another PDF or retry the scan.`
      : "The PDF could not be analyzed. Choose another PDF or retry the scan.";
    inputStatus.textContent = runMessages.error;
    setRunPhase("error");
  } finally {
    if (generation === scanGeneration) {
      delete analyzeButton.dataset.running;
      analyzeButtonLabel.textContent = "Run Evidence Scan";
      analyzeButton.disabled = !selectedFile;
      resetButton.disabled = !selectedFile;
    }
  }
}

fileInput.addEventListener("change", () => selectFile(fileInput.files?.[0]));
dropZone.addEventListener("dragover", (event) => {
  event.preventDefault();
  dropZone.dataset.dragging = "true";
});
dropZone.addEventListener("dragleave", () => delete dropZone.dataset.dragging);
dropZone.addEventListener("drop", (event) => {
  event.preventDefault();
  delete dropZone.dataset.dragging;
  selectFile(event.dataTransfer?.files[0]);
});
analyzeButton.addEventListener("click", () => void analyze());
resetButton.addEventListener("click", resetWorkspace);
previewPrevious.addEventListener("click", () => void renderPreviewPage(currentPreviewPage - 1));
previewNext.addEventListener("click", () => void renderPreviewPage(currentPreviewPage + 1));
previewZoomOut.addEventListener("click", () => {
  previewZoom = Math.max(0.5, Number((previewZoom - 0.25).toFixed(2)));
  setPreviewZoomLabel();
  void renderPreviewPage(currentPreviewPage);
});
previewZoomIn.addEventListener("click", () => {
  previewZoom = Math.min(2, Number((previewZoom + 0.25).toFixed(2)));
  setPreviewZoomLabel();
  void renderPreviewPage(currentPreviewPage);
});
ocrButton.addEventListener("click", () => void recognizeCurrentPage());
citationPin.addEventListener("click", () => {
  citationOpen = true;
  renderCitationOverlay();
  citationCard.scrollIntoView({ behavior: "smooth", block: "nearest" });
});
citationClose.addEventListener("click", () => {
  citationOpen = false;
  renderCitationOverlay();
});
let resizeFrame = 0;
window.addEventListener("resize", () => {
  window.cancelAnimationFrame(resizeFrame);
  resizeFrame = window.requestAnimationFrame(() => {
    if (previewDocument) void renderPreviewPage(currentPreviewPage);
    else renderCitationOverlay();
  });
});
window.addEventListener("beforeunload", clearTransformDownload);
window.addEventListener("beforeunload", () => previewWorker?.terminate());
runTransformButton.addEventListener("click", () => void executeTransform());
runLinearizeButton.addEventListener("click", () => void executeWorkflow("linearize"));
runAppendButton.addEventListener("click", () => void executeWorkflow("append"));
runFlattenButton.addEventListener("click", () => void executeWorkflow("flatten"));
runWatermarkButton.addEventListener("click", () => void executeWorkflow("watermark"));
runMergeButton.addEventListener("click", () => void executeWorkflow("merge"));
runExtractButton.addEventListener("click", () => void executeWorkflow("extract"));
runRotateButton.addEventListener("click", () => void executeWorkflow("rotate"));
runSplitButton.addEventListener("click", () => void executeWorkflow("split"));
runThumbnailButton.addEventListener("click", () => void executeThumbnail());
mergeFileInput.addEventListener("change", () => syncWorkflowControls());
watermarkTextInput.addEventListener("input", () => syncWorkflowControls());
window.addEventListener("beforeunload", clearWorkflowDownload);
[planRemap, planTokenize].forEach((control) => control.addEventListener("change", () => {
  clearTransformDownload();
  renderMappingRoute();
  void compileTransformPlan();
}));
[planSourceFont, planTargetFont].forEach((control) => control.addEventListener("change", () => {
  clearTransformDownload();
  renderFontInventory(discoveredFonts);
  void compileTransformPlan();
}));
planTokenizer.addEventListener("change", () => {
  clearTransformDownload();
  void compileTransformPlan();
});
fontFilter.addEventListener("input", () => renderFontInventory(discoveredFonts));
swapFontsButton.addEventListener("click", () => {
  clearTransformDownload();
  const source = planSourceFont.value;
  planSourceFont.value = planTargetFont.value;
  planTargetFont.value = source;
  renderFontInventory(discoveredFonts);
  void compileTransformPlan();
});

resetWorkspace();
syncPlanControls();
renderIcons();
