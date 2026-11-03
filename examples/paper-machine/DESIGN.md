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
  object-selected:
    backgroundColor: "{colors.signal}"
    textColor: "{colors.ink}"
    width: "100px"
    height: "48px"
---

# Design System: Paper Machine

## Overview

**Creative North Star: "Circuit Network Analyzer"**

Paper Machine treats PDF structure as something a person can probe. A navy measurement field, electric-yellow signals and a cool paper bench give the studio the character of a working instrument. The visual language belongs to this example; it does not define the PDF library's identity.

Sturdy Chivo lettering makes actions and sections immediate. Monospace identifies actual syntax and addresses. Dense structural information sits beside calm document surfaces; previews are rendered PDF pages, including the example's synthetic vector artwork.

**Key Characteristics:**

- Square controls and contiguous, bordered work areas.
- Solid selected objects, outlined related objects and orthogonal reference traces.
- Restrained depth, reserved for the rendered sheet.
- Motion attached to a live connection or a completed assembly.

## Colors

The palette pairs a dark instrument with cool neutral surfaces and a single electric accent. The frontmatter is the normative palette; supplemental state colors remain in the source and component examples. The sidecar's synthesized tonal strips are previews, not additional application tokens.

### Primary

- **Electric Signal** (`signal`): selected objects, connected traces, the build action and add-page markers.

### Neutral

- **Instrument Navy** (`ink`): primary text, primary action backgrounds and the measurement field.
- **Cool Paper** (`paper`): the application ground, syntax well and native object selector.
- **Bench White** (`white`): the masthead, inspector, page surfaces and text on navy.
- **Slate Blue** (`blue`): keyboard focus, thumbnail hover and the small wordmark marker.
- **Quiet Slate** (`muted`): supporting text on light surfaces.
- **Panel Seam** (`line`): work-area boundaries and small control strokes.
- **Field Readout** (`field-muted`): secondary text and addresses on navy.
- **Idle Trace** (`trace`): visible, inactive connections in the graph.

**The Energized State Rule.** Yellow identifies an actionable or connected state. A selected object takes a solid yellow field; related objects retain their field and gain a yellow outline.

## Typography

**Display and Body Font:** Chivo, with Arial and sans-serif fallbacks. The variable font is self-hosted; its license accompanies the font in the example.

**Syntax Font:** The system monospace stack in the frontmatter.

The hierarchy is compact and sturdy: a heavy, tightly spaced display line, strong section titles and small factual annotations. Body text stays plain; numeric metadata uses tabular figures where alignment matters.

- **Display:** The frontmatter records the wide-screen ramp. At compact widths it becomes a fixed size (40px); below the stacking breakpoint it uses `clamp(33px, 8.3vw, 48px)`.
- **Title:** Section headings use the frontmatter role, reducing to (19px) below the stacking breakpoint.
- **Body:** The frontmatter role establishes the default. Supporting descriptions use smaller sizes (11–15px) according to context.
- **Mono:** The syntax well uses the frontmatter role. Graph addresses, references and page addresses use smaller monospace annotations (10–11px).

**The Literal Readout Rule.** Reserve monospace for PDF syntax and addresses; use Chivo for names, instructions and actions.

## Layout

Work areas share straight seams rather than floating as independent cards. The content has a maximum width (1680px) and proportional horizontal gutters (4%, increasing to 5% on small screens). Section padding steps from roomy (24px) to compact (20px).

Density adjusts at (1050px); work areas stack at (760px). Long page sequences, source thumbnails and dense object maps scroll inside their own regions. Small graphs fit their viewport vertically; taller graphs retain their natural height. The narrow-screen map preserves a readable minimum width (650px) and scrolls horizontally. First-surface composition and workflow strategy remain in `.impeccable/surface.md`.

## Elevation & Depth

Tonal fields, borders and empty space establish depth. Work areas and controls are flat. The rendered sheet alone has a diffuse shadow (`0 8px 20px #142d3d22`), separating the document from its bench.

**The Sheet Depth Rule.** Keep instrument chrome flat; reserve ambient elevation for the rendered page.

## Shapes

Controls, selectors, thumbnails, work areas and graph nodes use square corners. Fine rules divide regions. Graph nodes have small circular connection ports; the local-process indicator is also circular. These circles have a functional role within the otherwise rectilinear system. References turn at right angles, and streams use dashed outlines.

## Components

### Buttons

Primary actions are navy with white text; assembly uses the yellow build variant. Both are square and use a brief background transition (0.2s), with a pressed offset (1px). Primary hover lightens the navy; build hover deepens the yellow. Text actions have no filled enclosure and underline on hover. Disabled buttons reduce opacity (0.4; build uses 0.5).

### Page navigation and assembly controls

Page navigation pairs square outline buttons (30px) with a tabular counter. Assembly controls are larger (32px) with a small gap (4px), increasing to (44px) and (6px) below the stacking breakpoint. Inline stroked SVGs carry direction and removal actions; accessible names provide their meaning. Boundary actions are disabled.

### Reference chips

Object addresses appear as compact, square outline buttons in monospace. Hover fills the reference with signal yellow. References outside the displayed graph remain visible but disabled.

### Inspector and selector

A white work area holds metadata, an ordinary native select and a cool-paper syntax well. The select has a thin seam border and square corners. The syntax region preserves whitespace and scrolls in both axes when needed. Buttons, links, selects and the syntax region share a visible slate-blue outline (3px, offset 4px).

### Object graph

Nodes pair a readable kind with a monospace address. Pages use a lighter navy fill; streams use a dashed stroke. Selection, related state and hover are distinct. Keyboard focus thickens the node stroke. Idle traces remain visible; active traces become yellow and animate dashed current (1.2s, linear). Nodes expose button semantics and pressed state, with Enter and Space activation in `app.js`.

### Feedback

Loading and errors occupy a bordered status strip with live announcements. Successful assembly reveals the result through a short horizontal clip animation (0.55s). Reduced-motion preferences disable animation, transitions and smooth scrolling.

## Do's and Don'ts

### Do:

- **Do** keep actual PDF previews and literal object addresses attached to their controls.
- **Do** distinguish a solid selected node from outlined related nodes.
- **Do** preserve local scrolling, keyboard focus and reduced-motion behavior when extending a work area.
- **Do** use the larger narrow-screen assembly controls with their established spacing.

### Don't:

- **Don't** introduce rounded card shells around the contiguous instrument panels.
- **Don't** use monospace for display headlines or action labels.
- **Don't** add ambient shadows to ordinary controls or panel seams.
- **Don't** turn sample artwork colors or tiny technical annotations into a general palette or heading scale.
