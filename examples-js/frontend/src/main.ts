import type { Template } from "@pdfme/common";
import type { Form } from "@pdfme/ui";
import {
  ArrowRightLeft,
  Braces,
  CircleAlert,
  FileSearch,
  FileUp,
  PanelTopOpen,
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
import type { Analysis, Citation, FontResource, TextRecoveryRequest, TransformPlan } from "zio-pdf-demo";
import "./styles.css";

const fileInput = document.querySelector<HTMLInputElement>("#file-input")!;
const dropZone = document.querySelector<HTMLLabelElement>("#drop-zone")!;
const workbench = document.querySelector<HTMLElement>("#workbench")!;
const analyzeButton = document.querySelector<HTMLButtonElement>("#analyze-button")!;
const resetButton = document.querySelector<HTMLButtonElement>("#reset-button")!;
const fileFacts = document.querySelector<HTMLElement>("#file-facts")!;
const inputStatus = document.querySelector<HTMLElement>("#input-status")!;
const sourceStatus = document.querySelector<HTMLElement>("#source-status")!;
const dropTitle = document.querySelector<HTMLElement>("#drop-title")!;
const dropCopy = document.querySelector<HTMLElement>("#drop-copy")!;
const emptyState = document.querySelector<HTMLElement>("#empty-state")!;
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
const planSourceFont = document.querySelector<HTMLInputElement>("#plan-source-font")!;
const planTargetFont = document.querySelector<HTMLInputElement>("#plan-target-font")!;
const planTokenizer = document.querySelector<HTMLSelectElement>("#plan-tokenizer")!;
const fontBindings = document.querySelector<HTMLElement>("#font-bindings")!;
const swapFontsButton = document.querySelector<HTMLButtonElement>("#swap-fonts")!;
const compilePlanButton = document.querySelector<HTMLButtonElement>("#compile-plan")!;
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

let selectedFile: File | undefined;
let preview: Form | undefined;
let previewGeneration = 0;
let currentPreviewPage = 1;
let previewTotalPages = 0;
let ocrGeneration = 0;
let planGeneration = 0;
let discoveredFonts: FontResource[] = [];
let expandedFont = "";
let citations: Citation[] = [];
let textRecoveryRequests: TextRecoveryRequest[] = [];
let citationOpen = false;

const MAX_PREVIEW_BYTES = 64 * 1024 * 1024;

type RunPhase = "idle" | "ready" | "bridge" | "evidence" | "complete" | "error";
type ObservationState = "positive" | "neutral" | "review";

const runMessages: Record<RunPhase, string> = {
  idle: "Choose a PDF to open its preview.",
  ready: "PDFMe is rendering a local preview. Analysis will read the Blob once.",
  bridge: "Loading the Scala.js scan bridge.",
  evidence: "One fused pass: decode, inspection, font inventory, text, validation, policy, and digest.",
  complete: "One fused scan complete.",
  error: "Scan stopped before a report was produced."
};

const stageOrder = ["source", "decode", "evidence", "report"] as const;
const iconSet = {
  ArrowRightLeft,
  Braces,
  CircleAlert,
  FileSearch,
  FileUp,
  PanelTopOpen,
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
  previewPage.textContent = previewTotalPages > 0 ? `Page ${page} / ${previewTotalPages}` : `Page ${page}`;
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
  return Array.from(previewHost.querySelectorAll<HTMLElement>("div")).find((element) => {
    const style = window.getComputedStyle(element);
    return (
      (style.overflowY === "auto" || style.overflowY === "scroll") &&
      element.scrollHeight > element.clientHeight &&
      (element.firstElementChild?.children.length ?? 0) > 0
    );
  });
}

function previewPageElement(page: number): HTMLElement | undefined {
  const pages = previewScrollContainer()?.firstElementChild?.children;
  const element = pages?.item(page - 1);
  return element === null || element === undefined ? undefined : (element as HTMLElement);
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
  preview?.destroy();
  preview = undefined;
  previewHost.replaceChildren();
  previewHost.hidden = true;
  previewError.hidden = true;
  citationLayer.hidden = true;
  citationOpen = false;
}

async function openPreview(file: File): Promise<void> {
  const generation = ++previewGeneration;
  preview?.destroy();
  preview = undefined;
  previewHost.replaceChildren();
  previewEmpty.hidden = true;
  previewError.hidden = true;
  previewHost.hidden = false;
  setPreviewState("loading", "Rendering locally");

  try {
    if (file.size > MAX_PREVIEW_BYTES) {
      throw new Error(
        `Local page preview is limited to ${formatBytes(MAX_PREVIEW_BYTES)}. ` +
          "The streaming Scala.js scan remains available for this file."
      );
    }
    const [{ Form }] = await Promise.all([import("@pdfme/ui")]);
    const basePdf = new Uint8Array(await file.arrayBuffer());
    if (generation !== previewGeneration) return;

    const template: Template = { basePdf, schemas: [] };
    preview = new Form({
      domContainer: previewHost,
      template,
      inputs: [{}],
      options: { zoomLevel: 0.82, sidebarOpen: false }
    });
    setPreviewPage(preview.getPageCursor() + 1);
    preview.onPageChange(({ currentPage, totalPages }) => {
      previewTotalPages = totalPages;
      setPreviewPage(currentPage + 1);
      renderCitationOverlay();
    });
    setPreviewState("ready", "Rendered locally");
  } catch (error) {
    if (generation !== previewGeneration) return;
    const message = error instanceof Error ? error.message : "The local previewer could not render this PDF.";
    previewHost.hidden = true;
    previewError.hidden = false;
    previewErrorMessage.textContent = message;
    inputStatus.textContent = `${message} Streaming scan is ready.`;
    setPreviewState("error", "Preview failed");
  }
}

function resetReport(): void {
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
  setRunPhase("idle");
}

function resetWorkspace(): void {
  selectedFile = undefined;
  fileInput.value = "";
  analyzeButton.disabled = true;
  resetButton.disabled = true;
  delete dropZone.dataset.selected;
  dropTitle.textContent = "Choose a PDF";
  dropCopy.textContent = "or drop one here";
  sourceStatus.textContent = "Blob stream";
  fileFacts.hidden = true;
  inputStatus.textContent = runMessages.idle;
  destroyPreview();
  previewEmpty.hidden = false;
  setPreviewState("idle", "No file");
  resetReport();
}

function selectFile(file: File | undefined): void {
  if (!file) {
    resetWorkspace();
    return;
  }

  selectedFile = file;
  resetReport();
  analyzeButton.disabled = false;
  resetButton.disabled = false;
  dropZone.dataset.selected = "true";
  dropTitle.textContent = "Replace PDF";
  dropCopy.textContent = "preview opens in this browser";
  setText("#file-name", file.name);
  setText("#file-size", formatBytes(file.size));
  sourceStatus.textContent = "Blob stream";
  fileFacts.hidden = false;
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
        detail: "Proves glyph-code and metric compatibility before rebinding page font resources."
      };
    case "tokenize-text":
      return {
        title: "TokenizeText",
        detail: "Resolves Unicode page text after the remap and emits caller-owned tokens."
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
    sourceFont: planSourceFont.value.trim() || "SourceFace",
    targetFont: planTargetFont.value.trim() || "TargetFace",
    tokenize: planTokenize.checked,
    tokenizer: planTokenizer.value === "words" ? "words" : "characters"
  };
}

function syncPlanControls(): void {
  const remapEnabled = planRemap.checked;
  const tokenizeEnabled = planTokenize.checked;
  fontBindings.dataset.disabled = String(!remapEnabled);
  planSourceFont.disabled = !remapEnabled;
  planTargetFont.disabled = !remapEnabled;
  swapFontsButton.disabled = !remapEnabled;
  planTokenizer.disabled = !tokenizeEnabled;
}

function selectDocumentFont(role: "source" | "target", font: FontResource): void {
  planRemap.checked = true;
  if (role === "source") planSourceFont.value = font.baseFont;
  else planTargetFont.value = font.baseFont;
  renderFontInventory(discoveredFonts);
  void compileTransformPlan();
}

type FontGroup = { resource: FontResource; records: FontResource[] };

function groupDocumentFonts(fonts: FontResource[]): FontGroup[] {
  const grouped = new Map<string, FontResource[]>();
  fonts.forEach((font) => {
    const records = grouped.get(font.baseFont) ?? [];
    records.push(font);
    grouped.set(font.baseFont, records);
  });
  return Array.from(grouped.values()).map((records) => ({
    resource: records.find((font) => font.existingResourceRemapCandidate) ?? records[0],
    records
  }));
}

function groupForFont(fontName: string): FontGroup | undefined {
  return groupDocumentFonts(discoveredFonts).find(({ resource }) => resource.baseFont === fontName);
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
  const candidatePair = Boolean(planRemap.checked && source?.resource.existingResourceRemapCandidate && target?.resource.existingResourceRemapCandidate);
  const remapDisabled = !planRemap.checked;

  mappingRoute.dataset.state = remapDisabled ? "inactive" : candidatePair ? "candidate" : "unresolved";
  mappingSourceName.textContent = sourceName;
  mappingSourceDetail.textContent = endpointDescription(sourceName, source);
  mappingTargetName.textContent = targetName;
  mappingTargetDetail.textContent = endpointDescription(targetName, target);
  mappingRouteState.textContent = remapDisabled ? "remap off" : candidatePair ? "candidate pair" : "needs resource";
  mappingRouteCopy.textContent = remapDisabled
    ? "The resources remain visible for inspection. Enable the remap leaf to include this route in the compiled plan."
    : candidatePair
    ? "A real document-resource pair is selected. Compatibility is proved only when the transform executes."
    : "Select two rebindable document resources to build a verifiable remap candidate.";
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
  const candidates = groups.filter(({ resource }) => resource.existingResourceRemapCandidate);
  const filter = fontFilter.value.trim().toLowerCase();
  const visibleGroups = groups.filter(({ resource, records }) =>
    !filter || [resource.baseFont, ...records.map((font) => `${font.objectNumber} ${font.subtype ?? ""}`)]
      .join(" ")
      .toLowerCase()
      .includes(filter)
  );
  fontInventorySummary.textContent = filter
    ? `${visibleGroups.length} shown / ${groups.length} resources`
    : `${groups.length} resources / ${candidates.length} candidates`;

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

  visibleGroups.forEach(({ resource, records }) => {
    const item = document.createElement("li");
    const facts = document.createElement("div");
    const name = document.createElement("strong");
    const detail = document.createElement("small");
    const actions = document.createElement("div");
    const inspect = document.createElement("button");
    const source = document.createElement("button");
    const target = document.createElement("button");
    const status = resource.existingResourceRemapCandidate ? "existing-resource candidate" : "extract-only font kind";
    const related = records
      .filter((font) => font.objectNumber !== resource.objectNumber)
      .map((font) => `#${font.objectNumber} /${font.subtype ?? "unknown"}`);

    item.dataset.candidate = String(resource.existingResourceRemapCandidate);
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

    source.className = "font-action";
    source.type = "button";
    source.textContent = "From";
    source.title = `Use ${resource.baseFont} as the remap source`;
    source.disabled = !resource.existingResourceRemapCandidate;
    source.dataset.selected = String(planSourceFont.value.trim() === resource.baseFont);
    source.addEventListener("click", () => selectDocumentFont("source", resource));

    target.className = "font-action";
    target.type = "button";
    target.textContent = "To";
    target.title = `Use ${resource.baseFont} as the remap target`;
    target.disabled = !resource.existingResourceRemapCandidate;
    target.dataset.selected = String(planTargetFont.value.trim() === resource.baseFont);
    target.addEventListener("click", () => selectDocumentFont("target", resource));

    actions.append(inspect, source, target);
    item.append(facts, actions);

    if (expandedFont === resource.baseFont) {
      const inspector = document.createElement("div");
      const heading = document.createElement("span");
      const structure = document.createElement("code");
      const check = document.createElement("small");
      inspector.className = "font-detail";
      heading.textContent = resource.existingResourceRemapCandidate ? "rebindable resource root" : "extract-only resource";
      structure.textContent = [resource, ...records.filter((font) => font.objectNumber !== resource.objectNumber)]
        .map((font, index) => `${index === 0 ? "root" : "linked"} #${font.objectNumber} /${font.subtype ?? "unknown"}`)
        .join("  ·  ");
      check.textContent = resource.existingResourceRemapCandidate
        ? "The execution gate compares encoding, CID metrics, and ToUnicode before changing any reference."
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
  planBadge.textContent = "Typed Plan";
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
}

async function compileTransformPlan(): Promise<void> {
  const generation = ++planGeneration;
  syncPlanControls();
  transformPlan.dataset.state = "compiling";
  planBadge.textContent = "compiling";
  compilePlanButton.disabled = true;
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
  } finally {
    if (generation === planGeneration) compilePlanButton.disabled = false;
  }
}

function renderAnalysis(analysis: Analysis): void {
  const { inspection, content } = analysis;
  emptyState.hidden = true;
  errorState.hidden = true;
  report.hidden = false;
  reportTitle.textContent = inspection.status === "accepted" ? "Inspection complete" : "Inspection rejected";
  reportState.textContent = inspection.status;
  reportState.dataset.state = inspection.status;

  setText("#preflight-result", inspection.violation ?? inspection.status);
  setText("#validation-result", analysis.valid ? "valid" : "needs review");
  setText("#policy-result", analysis.strictPolicyPassed ? "passed" : "blocked");
  setText("#objects-result", String(inspection.elementsRead));
  setText("#elapsed-result", `${Math.max(0, Math.round(analysis.elapsedMs)).toLocaleString()} ms`);
  setText("#digest-result", `SHA-256 ${analysis.sha256.slice(0, 12)}...`);

  renderFontInventory(inspection.fonts);
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
    "Encryption",
    inspection.encrypted ? "Encrypted" : "Not encrypted",
    inspection.encrypted
      ? inspection.encryptionObject === undefined
        ? "Trailer has a direct /Encrypt dictionary"
        : `Trailer references encryption object #${inspection.encryptionObject}`
      : "No /Encrypt entry in the trailer",
    inspection.encrypted ? "review" : "positive"
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
  ocrStatus.textContent = `Preparing page ${currentPreviewPage} for local OCR...`;
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

  analyzeButton.disabled = true;
  resetButton.disabled = true;
  emptyState.hidden = true;
  errorState.hidden = true;
  report.hidden = true;
  setRunPhase("bridge");
  inputStatus.textContent = runMessages.bridge;

  try {
    const { ZioPdfDemo } = await import("zio-pdf-demo");
    if (transformPlan.dataset.state !== "ready") await compileTransformPlan();
    renderAnalysis(await ZioPdfDemo.analyzeBlobWithProgress(selectedFile, (phase) => {
      const runPhase = phase as RunPhase;
      setRunPhase(runPhase);
      inputStatus.textContent = runMessages[runPhase] ?? "Streaming the document through Scala.js.";
    }));
    inputStatus.textContent = runMessages.complete;
  } catch (error) {
    errorState.hidden = false;
    reportState.textContent = "error";
    reportState.dataset.state = "error";
    errorMessage.textContent = error instanceof Error ? error.message : "The PDF could not be analyzed.";
    inputStatus.textContent = runMessages.error;
    setRunPhase("error");
  } finally {
    analyzeButton.disabled = !selectedFile;
    resetButton.disabled = !selectedFile;
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
window.addEventListener("resize", renderCitationOverlay);
compilePlanButton.addEventListener("click", () => void compileTransformPlan());
[planRemap, planTokenize].forEach((control) => control.addEventListener("change", () => {
  renderMappingRoute();
  void compileTransformPlan();
}));
[planSourceFont, planTargetFont].forEach((control) => control.addEventListener("input", () => {
  renderFontInventory(discoveredFonts);
  void compileTransformPlan();
}));
planTokenizer.addEventListener("change", () => void compileTransformPlan());
fontFilter.addEventListener("input", () => renderFontInventory(discoveredFonts));
swapFontsButton.addEventListener("click", () => {
  const source = planSourceFont.value;
  planSourceFont.value = planTargetFont.value;
  planTargetFont.value = source;
  renderFontInventory(discoveredFonts);
  void compileTransformPlan();
});

resetWorkspace();
syncPlanControls();
renderIcons();
