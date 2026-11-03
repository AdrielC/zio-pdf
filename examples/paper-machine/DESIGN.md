---
name: Paper Machine
description: A local PDF studio with the clarity of a circuit network analyzer.
colors:
  ink: "#142d3d"
  paper: "#f3f5f6"
  white: "#fff"
  signal: "#e2ed6b"
  blue: "#3e6585"
  muted: "#566571"
  line: "#cbd4da"
  field-muted: "#b4c9d5"
  trace: "#7290a3"
  action-hover: "#31536a"
  signal-hover: "#d0df42"
  node: "#1c394b"
  node-page: "#31566f"
  node-hover: "#395972"
  node-line: "#6d8899"
  node-page-line: "#a0bccd"
  bench: "#e8edf1"
  assembly: "#f1f3e8"
  assembly-line: "#c9cfbd"
  assembly-hover: "#dce2c9"
  syntax: "#26475d"
  notice: "#e4eaf0"
  checking: "#f3f5e6"
  progress-track: "#486170"
  queued-text: "#465b69"
  passed-bg: "#e7f0e7"
  passed-text: "#27542d"
  passed-line: "#a9bfa7"
  failed-bg: "#fbe9e2"
  failed-text: "#8c301c"
  failed-line: "#ba5a3e"
  unable-bg: "#f6ecd8"
  unable-text: "#6e4e1d"
typography:
  display:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "clamp(32px, 3.7vw, 58px)"
    fontWeight: 800
    lineHeight: 1.1
    letterSpacing: "-0.04em"
  title:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "20px"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.025em"
  body:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "14px"
    fontWeight: 400
    lineHeight: 1.5
  validation-headline:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "30px"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.035em"
  packet-title:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "18px"
    fontWeight: 700
    lineHeight: 1.5
  control:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "14px"
    fontWeight: 600
    lineHeight: 1.5
  label:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "12px"
    fontWeight: 600
    lineHeight: 1.5
  annotation:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "11px"
    fontWeight: 400
    lineHeight: 1.5
  small-annotation:
    fontFamily: "Chivo, Arial, sans-serif"
    fontSize: "10px"
    lineHeight: 1.5
  mono:
    fontFamily: "ui-monospace, SFMono-Regular, Consolas, monospace"
    fontSize: "12px"
    lineHeight: 1.7
rounded:
  square: "0"
spacing:
  control-gap: "6px"
  item-gap: "8px"
  compact: "12px"
  comfortable: "16px"
  panel-compact: "20px"
  panel: "24px"
components:
  button-primary:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.white}"
    rounded: "{rounded.square}"
    padding: "12px 20px"
  button-build:
    backgroundColor: "{colors.signal}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "10px 16px"
  button-text:
    backgroundColor: "transparent"
    textColor: "{colors.ink}"
    padding: "5px 0"
  page-navigation:
    backgroundColor: "transparent"
    textColor: "{colors.ink}"
    width: "30px"
    height: "30px"
  reference-button:
    backgroundColor: "{colors.white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "4px 8px"
  inspector:
    backgroundColor: "{colors.white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "24px"
  packet-field:
    backgroundColor: "{colors.white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "8px"
    height: "44px"
  validation-file:
    backgroundColor: "{colors.white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "23px 24px"
  object-selected:
    backgroundColor: "{colors.signal}"
    textColor: "{colors.ink}"
    width: "100px"
    height: "48px"
  pipeline-field:
    backgroundColor: "{colors.white}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "10px 12px"
  pipeline-binding:
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    padding: "12px 16px"
  pipeline-runbar:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.white}"
    rounded: "{rounded.square}"
    padding: "18px 24px"
  pipeline-operation-selected:
    backgroundColor: "{colors.signal}"
    textColor: "{colors.ink}"
    rounded: "{rounded.square}"
    width: "180px"
    height: "54px"
---

# Design System: Paper Machine

## Overview

**Creative North Star: "Circuit Network Analyzer"**

Paper Machine treats PDF structure as something a person can probe. A navy measurement field, electric-yellow signals and a cool paper bench give the studio the character of a working instrument. The visual language belongs to this example; it does not define the PDF library's identity.

Sturdy Chivo lettering makes actions and sections immediate. Monospace identifies actual syntax and addresses. Dense structural information sits beside calm document surfaces; previews are rendered PDF pages, including the example's synthetic vector artwork. Packet finishing, batch validation and reusable pipelines extend the same instrument through plain fields, contiguous rows and evidence attached to each file. Pipeline selection energizes the operation and its connected traces while native controls keep wiring and settings accessible. Three-way routing leaves skipped operations visible and pairs the chosen path with a written reason and execution trace.

**Key Characteristics:**

- Square controls and contiguous, bordered work areas.
- Solid selected objects, outlined related objects and orthogonal reference traces.
- Restrained depth, reserved for the rendered sheet.
- Motion attached to a live connection or a completed assembly.
- Literal routing outcomes, visible skipped paths and evidence tied to completed checks.

## Colors

The palette pairs a dark instrument with cool neutral surfaces and a single electric action accent. The frontmatter is normative and includes the existing navy ramp and validation states. Sidecar tonal strips are previews, not additional application tokens; the navy strip reuses observed surface colors.

### Primary

- **Electric Signal** (`signal`): selected objects and pipeline operations, connected traces, build/run actions, output markers and add-page markers. Selected pipeline labels switch to Instrument Navy, with Syntax Ink for their secondary readout.

### Neutral

- **Instrument Navy** (`ink`): primary text, primary action backgrounds and the measurement field.
- **Cool Paper** (`paper`): the application ground, syntax well and native object selector.
- **Bench White** (`white`): the masthead, inspector, page surfaces and text on navy.
- **Slate Blue** (`blue`): keyboard focus, thumbnail hover and the small wordmark marker.
- **Quiet Slate** (`muted`): supporting text on light surfaces.
- **Panel Seam** (`line`): work-area boundaries and small control strokes.
- **Field Readout** (`field-muted`): secondary text and addresses on navy.
- **Idle Trace** (`trace`): visible, inactive connections in the graph.
- **Navy Surface Ramp** (`node`, `node-page`, `node-hover`, `action-hover`): structural objects, pages and hover states, with `node-line` and `node-page-line` for their strokes.
- **Page Bench and Assembly Ground** (`bench`, `assembly`): cool rendered-page bench and pale assembly area; assembly rules and hover use `assembly-line` and `assembly-hover`.
- **Syntax Ink** (`syntax`): literal dictionary content on cool paper.

### Status

- **Queued / Checking** (`queued-text`, `checking`, `progress-track`): quiet queued readouts, a pale active-file wash and a dark progress track. The active badge and progress fill use Electric Signal.
- **Verified** (`passed-bg`, `passed-text`, `passed-line`): green only for completed passing checks and the matching verified declaration label.
- **Failed** (`failed-bg`, `failed-text`, `failed-line`): rust text on a pale warm field for completed failed checks and errors.
- **Unable to Check** (`unable-bg`, `unable-text`): amber for checks that could not establish a verdict.
- **Notice** (`notice`): a subdued blue-gray information strip.

**The Verdict Evidence Rule.** A metadata claim never receives verified styling by itself. Pair every status color with a literal outcome label; verification belongs to completed checks of the exact bytes.

**The Energized State Rule.** Yellow identifies an actionable or connected state. A selected object takes a solid yellow field; related objects retain their field and gain a yellow outline. In a pipeline, connected selection traces and the chosen branch trace use yellow; dashed boundaries and literal status text keep skipped operations distinct.

## Typography

**Display and Body Font:** Chivo, with Arial and sans-serif fallbacks. The variable font is self-hosted; its license accompanies the font in the example.

**Syntax Font:** The system monospace stack in the frontmatter.

The hierarchy is compact and sturdy: a heavy, tightly spaced display line, strong section titles and small factual annotations. Body text stays plain; numeric metadata uses tabular figures where alignment matters.

- **Display:** The frontmatter records the wide-screen ramp. At compact widths it becomes a fixed size (40px); below the stacking breakpoint it uses `clamp(33px, 8.3vw, 48px)`.
- **Title:** Section headings use the frontmatter role, reducing to (19px) below the stacking breakpoint.
- **Body:** The frontmatter role establishes the default. Supporting descriptions use smaller sizes (11–15px) according to context.
- **Validation headline:** A subordinate heavy headline (30px, reducing to 27px when stacked). Pipelines reuse the same wide-screen size and tight spacing, reducing to (26px) at their narrow breakpoint. The queue empty-state title uses (22px); packet legends and pipeline input headings use the frontmatter packet-title role.
- **Controls and labels:** Actions use the control role; compact actions and queue filenames use (13px). Labels and evidence use (12px), file metadata and receipts use the annotation role, and verdict badges reduce from (11px) to (10px) when stacked.
- **Small annotations:** Graph keys, profile captions and supporting technical notes use (9–11px); the live marker reduces to (8px) on narrow screens. These are supporting readouts, not heading sizes.
- **Mono:** The syntax well uses the frontmatter role. Graph addresses, references, fingerprints, pipeline operation identifiers and page addresses use smaller monospace annotations (10–11px). The editable recipe data uses (12px) and a line height (1.6). Source PDF artwork retains its own real type; do not simulate it with a bitmap font.

**The Literal Readout Rule.** Reserve monospace for PDF syntax, recipe data, identifiers and addresses; use Chivo for names, instructions and actions.

## Layout

Work areas share straight seams rather than floating as independent cards. The content has a maximum width (1680px) and proportional horizontal gutters (4%, increasing to 5% on small screens). Section padding steps from roomy (24px) to compact (20px).

Density adjusts at (1050px); work areas stack at (760px). Long page sequences, source thumbnails and dense object maps scroll inside their own regions. Small graphs fit their viewport vertically; taller graphs retain their natural height. The narrow-screen map preserves a readable minimum width (650px) and scrolls horizontally. Packet bookends and finishing operations use two equal columns with a gap (24px), stacking into one column with a gap (8px) at (760px). Each revealed field group keeps two shrinkable columns with a gap (12px); text inputs and selects are at least (44px) high.

The validation controls, navy run bar and file queue share one continuous width. Queue rows use padding (23px 24px), becoming (20px 18px) when stacked. Outcome groups wrap; the identification claim takes a full row on small screens while profile verdicts retain a readable minimum width (93px). Filenames and hashes wrap anywhere. Receipts use a narrow label column (110px, reducing to 75px) beside a shrinkable value column. Progress is a thin rule (5px) with a maximum width (440px); the narrow run bar wraps progress above its actions. The interface is reviewed at wide (1440px) and narrow (390px and 535px) CSS widths. First-surface composition and workflow strategy remain in `.impeccable/surface.md`.

Pipelines reuse the same seams, gutters and spacing scale. Their recipe toolbar and named-input bindings precede the full-width run bar; the editor/graph workbench and completed outputs follow. A comfortable step (16px) recurs in binding gaps, graph guidance, compact editor padding and narrow run-bar padding. The recipe toolbar wraps; its name field grows within an observed range (220–400px), while the Try a recipe selector retains a compact maximum width (280px). Bound-input rows flex from a preferred basis (280px), allowing several PDFs to share a line. The workbench pairs a fixed editor (300px) with a shrinkable navy graph; the editor narrows (240px) at (850px), then stacks above the graph at (620px). At that narrow breakpoint, each binding's slot and filename take a full row with actions below, the name field spans its row, the run bar wraps its status above actions, file-result actions move below metadata, and tabs may wrap.

The pipeline graph keeps its natural topology and scrolls locally in both axes. Its viewport is capped at (620px) high, reducing to (500px) at (620px); the topology starts from a minimum content width (500px), then adds external routing gutters and grows for parallel branches. Operations retain a vertical layer pitch (110px); edges that span more than one layer use alternating left/right gutter lanes, spaced (12px) apart, so they bypass unrelated nodes. Four input labels sit above each choice operation on navy backing, above the traces in drawing order. Visible instructions above the scroll region name both directions, and the native operation selector remains available above it when stacked. Final pipeline captures at (1440px), (390px) and (262px) show document width equal to viewport width; the graph's own horizontal overflow stays inside its region.

## Elevation & Depth

Tonal fields, borders and empty space establish depth. Work areas and controls are flat. The rendered sheet alone has a diffuse shadow (`0 8px 20px #142d3d22`), separating the document from its bench.

**The Sheet Depth Rule.** Keep instrument chrome flat; reserve ambient elevation for the rendered page.

## Shapes

Controls, selectors, thumbnails, work areas and graph nodes use square corners. Fine rules divide regions. Graph nodes have small circular connection ports; the local-process indicator is also circular. These circles have a functional role within the otherwise rectilinear system. References turn at right angles, and streams use dashed outlines.

## Components

### Buttons

Primary actions are navy with white text; assembly uses the yellow build variant. Both are square and use a brief background transition (0.2s), with a pressed offset (1px). Primary hover uses action-hover; build hover uses signal-hover. Text actions have no filled enclosure and underline on hover. Disabled buttons reduce opacity (0.4; build uses 0.5).

### Page navigation and assembly controls

Page navigation pairs square outline buttons (30px) with a tabular counter. Assembly controls are larger (32px) with a small gap (4px), increasing to (44px) and (6px) below the stacking breakpoint. Inline stroked SVGs carry direction and removal actions; accessible names provide their meaning. Boundary actions are disabled.

### Reference chips

Object addresses appear as compact, square outline buttons in monospace. Hover fills the reference with signal yellow. References outside the displayed graph remain visible but disabled.

### Inspector and selector

A white work area holds metadata, an ordinary native select and a cool-paper syntax well. The select has a thin seam border and square corners. The syntax region preserves whitespace and scrolls in both axes when needed. Buttons, links, selects and the syntax region share a visible slate-blue outline (3px, offset 4px). Packet text inputs and profile checkboxes use the same treatment. Buttons in the navy validation run bar switch the outline to Electric Signal for contrast.

### Object graph

Nodes pair a readable kind with a monospace address. Pages use a lighter navy fill; streams use a dashed stroke. Selection, related state and hover are distinct. Keyboard focus thickens the node stroke. Idle traces remain visible; active traces become yellow and animate dashed current (1.2s, linear). Nodes expose button semantics and pressed state, with Enter and Space activation in `app.js`.

### Studio tabs

Three text tabs sit on a shared seam; the active tab gains a navy underline (3px) and darker text. The desktop gap (26px) becomes (20px) when stacked and the small tool notes disappear. Tabs wrap at the pipeline narrow breakpoint (620px). Tab state, associated panels and focus order are explicit; Left and Right arrows cycle through the tools, while Home and End select the first and last. The open-file action and skip-link destination follow the active tool.

### Packet finishing

A ruled fieldset belongs to the assembly area. Front/back slots expose filename and page count, then text actions at least (44px) high. Each Choose and Remove action has a unique accessible name identifying its front or back slot; Remove stays disabled until a PDF is present. Native checkboxes reveal stamp or numbering fields only when enabled. The controls specify actual searchable stamp text, placement, page/number/Bates format, start and optional prefix. One Build PDF action applies the sequence and finishing; the result updates the real preview, graph and direct download. Activation of the built document clears bookends and disables finishing choices, hiding their fields so another build cannot silently apply them again. Optional validation sends the exact built output through the chosen profiles while retaining other queued receipts.

### Validation queue and evidence

A white control area holds native profile checkboxes with (44px) label targets, then Add PDFs and Clear queue. The navy run bar carries live progress, a stop-after-current-file action and the signal-filled run action. Queue rows identify each file before showing its XMP claim and separate profile outcomes: Queued, Checking, Verified, Failed or Unable to check. Inspect PDF returns to the explorer; the removal icon has a filename-specific accessible name. Row actions and profile changes are disabled during a run.

Evidence stays behind a native disclosure. Its receipt shows engine, checked time, fingerprint and readability; each profile's failures carry clause/test, message and literal context. Completed keyed rows remain mounted while other files advance, preserving open disclosures, text selection and keyboard focus. Profile changes clear earlier verdicts until rechecked. JSON and CSV exports become available for completed files. Never substitute a metadata declaration for a verified result, or label an interrupted/unreadable check as passed or failed.

### Pipeline recipe controls and bindings

Recipe actions use the existing text-button language beside a square name field. Capture current packet, Save recipe, Load recipe and Insert saved recipe stay explicit. Try a recipe is a native selector offering Stamp and number, Branch by page count and Branch by PDF/A verdict; it shares the existing field language. Guidance above the bindings states that only inputs reached by the selected path are required. Bound-input rows identify a slot before its filename and page count, or a literal unbound state. Use current PDF and Choose PDF are contextual text actions with targets at least (44px) high. Long slot names and filenames wrap anywhere; on narrow screens their information retains the full available width above the actions. These rows reuse Panel Seam and Quiet Slate without adding a new color family.

The operation editor uses labeled native selects, text fields, number fields and checkboxes. Fields reuse Bench White, Instrument Navy and square borders, with padding from the pipeline-field token. A native Selected operation control reaches every operation independently of the scrollable graph. Typed connection selectors and operation-specific settings share this column; Add operation is navy and Remove selected operation is a text action. Saved-fragment insertion reveals a pale assembly-ground panel with explicit input connection selectors and Insert/Cancel actions. Recipe JSON remains behind a native disclosure; Apply JSON validates the edited data before replacing the graph.

### Three-way conditions and explicit stops

Choose a path uses the same stacked native fields as other operations. Its first connection inspects a document for page count or an XMP claim, or a validation receipt for a completed verdict. Three subsequent connections retain separate matched, not-matched and unknown routes; validation relabels them passed, failed and unable to check. Branch output kind remains explicit: Document, PDF bytes or Validation receipt. Connection choices follow the required kind.

The Condition selector offers Page count, Declares PDF/A-3b (XMP claim), and Completed validation result. Page count reveals Minimum pages and Maximum pages number fields. Completed validation reveals Profile to inspect with PDF/A-3b, PDF/A-2b and PDF/A-3u choices. Supporting copy explains that only the selected branch runs and a missing or unfinished check never becomes a failed verdict. Keep the unknown route separate in both language and wiring; the claim option never acquires verified styling.

Stop with reason exposes Reason to stop and Branch output kind. Its explanation states that it stops only when reached; an unselected stop remains skipped. Use the ordinary labeled editor fields and explicit explanation, without a special alert color before execution.

### Pipeline graph

The executable recipe is a navy field with a title, operation/output count and visible scrolling guidance. Square operations show a Chivo name above a smaller monospace identifier and port kind. Idle operations reuse Structure Navy and Structure Stroke; hover uses Object Hover Navy. The selected operation uses a solid Electric Signal fill, Instrument Navy title and Syntax Ink secondary text. Connected orthogonal traces become yellow and thicken; pipeline traces stay static. A small yellow circle identifies each requested output. Choice operations label their four incoming ports test, yes, no and ? in small monospace on Instrument Navy backing; the editor provides the full wording for each route. Long connections turn through the external gutters instead of crossing unrelated operations.

After a completed run, operations add literal ran or skipped text to their identifier line and accessible name. Skipped nodes and wires use dashes (5px / 4px) with readable Field Readout text. The selected skipped node keeps navy text and a navy dashed boundary against its signal fill. The chosen branch connection remains signal yellow; these static routing states do not add animation.

Graph nodes expose button semantics and work with Enter or Space. Keyboard selection returns focus to the native operation selector. Nodes use a white focus outline (3px, offset 4px), while the scroll region itself uses an inset signal outline (3px). Keep the visible horizontal/vertical guidance and the native selector alongside this internally scrollable topology.

### Pipeline run state and results

The navy run bar reuses the signal-filled build button for Run pipeline and a white text action for Check recipe. Its status is announced politely. A checked recipe names its reachable-operation count; edits announce that the recipe changed and hide prior outputs. Running announces PDF writing and selected-profile checking, disables the pane's buttons and fields, and hides stale results. Errors reuse the established failed field, text and border, with an alert and path-specific issue list; controls become available again. Completion announces that outputs and receipts are ready and reveals contiguous result rows below the workbench.

A completed-run summary names operations run, skipped operations, outputs and validator passes. Routing decisions appear as a ruled ordered list with each literal outcome, selected operation and reason. Non-validation decisions read matched, not matched or unknown; validation decisions read passed, failed or unable to check. A native Execution trace disclosure lists execution order, skipped operations and each validation pass's checked profiles and SHA-256. The disclosure target is at least (44px) high, and trace values wrap anywhere on narrow screens.

File rows place filename, pages, size and source operation before Inspect output and Download PDF. Receipt rows pair explicitly labeled profile verdicts with the exact SHA-256 and a native evidence disclosure. The run report is a text action beside the results title. A completed pipeline is distinct from a passing PDF/A check: the receipt's labeled verdict retains its own status color. Run-bar focus uses Electric Signal for contrast. Conditional controls, routing decisions and execution traces reuse the existing palette, type families, square shape, status colors and action variants.

### Feedback

Loading and errors occupy a bordered status strip with live announcements. Successful assembly reveals the result through a short horizontal clip animation (0.55s). Reduced-motion preferences disable animation, transitions and smooth scrolling.

## Do's and Don'ts

### Do:

- **Do** keep actual PDF previews and literal object addresses attached to their controls.
- **Do** distinguish a solid selected node from outlined related nodes.
- **Do** preserve local scrolling, keyboard focus and reduced-motion behavior when extending a work area.
- **Do** use the larger narrow-screen assembly controls with their established spacing.
- **Do** keep queue outcomes labeled, receipts tied to exact bytes and completed disclosures stable while a batch advances.
- **Do** reveal finishing fields progressively and clear enabled finishing choices after a successful build.
- **Do** use real PDF renders for every preview; no generated raster artwork ships with the studio.
- **Do** keep pipeline binding information full-width above its actions on narrow screens, and preserve the native selector and visible two-axis scrolling guidance beside the graph.
- **Do** distinguish pipeline completion from PDF/A verification, and hide prior pipeline results when recipe settings change.
- **Do** preserve the separate unknown route, typed connections and plain explanation of which path requires each PDF input.
- **Do** pair chosen and skipped graph states with literal routing reasons and ran/skipped text; keep long edges in their external gutters.

### Don't:

- **Don't** introduce rounded card shells around the contiguous instrument panels.
- **Don't** use monospace for display headlines or action labels.
- **Don't** add ambient shadows to ordinary controls or panel seams.
- **Don't** turn sample artwork colors or tiny technical annotations into a general palette or heading scale.
- **Don't** apply verified styling to an XMP claim, stale fingerprint or unfinished check.
- **Don't** use a low-contrast focus outline on the navy run bar.
- **Don't** compress pipeline operation text to make a branching graph fit the document width; keep that overflow local to the graph.
- **Don't** turn an XMP claim or an unknown condition into a failed or verified PDF/A verdict.
