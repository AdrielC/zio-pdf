"use strict";

const pipelineState = {
  recipe: null,
  selected: null,
  bindings: Object.create(null),
  running: false,
  preparing: 0,
  result: null,
  fragment: null,
  revision: 0,
  fileMode: "load",
};
const operationNames = {
  input: "PDF input",
  select: "Select pages",
  assemble: "Assemble PDFs",
  stamp: "Stamp pages",
  number: "Number pages",
  write: "Write PDF",
  "convert-pdfa-3b": "Convert to PDF/A-3b",
  validate: "Validate PDF/A",
  preflight: "Court preflight",
  "preflight-rules": "Court rules",
  choose: "Choose a path",
  stop: "Stop with reason",
  keep: "Keep a value",
  "stop-after": "Stop after inputs",
};
function pipelineKind(node) {
  if (["choose", "stop", "stop-after"].includes(node.op)) return node.kind;
  if (node.op === "keep") return node.kinds[node.index];
  return ["write", "convert-pdfa-3b"].includes(node.op)
    ? "file"
    : ["validate", "preflight", "preflight-rules"].includes(node.op)
      ? "receipt"
      : "document";
}
function pipelineError(error) {
  const box = $("pipeline-errors");
  box.replaceChildren(element("p", "", error.message));
  if (error.issues?.length) {
    const list = element("ul");
    for (const issue of error.issues)
      list.append(element("li", "", `${issue.path}: ${issue.message}`));
    box.append(list);
  }
  box.hidden = false;
  $("pipeline-status").textContent = "Recipe needs attention";
}
async function pipelineRequest(path, body) {
  const response = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(180000),
  });
  const result = await response.json();
  if (!response.ok) {
    const error = new Error(result.error || "The pipeline could not run.");
    error.issues = result.issues;
    throw error;
  }
  return result;
}
function pipelineDownload(value, name) {
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(value, null, 2)], { type: "application/json" }),
  );
  const link = element("a");
  link.href = url;
  link.download = name;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
function pipelineChanged(editor = false) {
  pipelineState.revision++;
  pipelineState.result = null;
  $("pipeline-results").hidden = true;
  $("pipeline-errors").hidden = true;
  $("pipeline-status").textContent = "Recipe changed · check or run when ready";
  $("pipeline-json").value = JSON.stringify(pipelineState.recipe, null, 2);
  drawPipelineGraph();
  if (editor) drawPipelineEditor();
}
function pipelineSelect(id) {
  pipelineState.selected = id;
  drawPipelineEditor();
  drawPipelineGraph();
}
function svgNode(tag, attributes) {
  const node = document.createElementNS(NS, tag);
  for (const [key, value] of Object.entries(attributes))
    node.setAttribute(key, value);
  return node;
}
function drawPipelineGraph() {
  const recipe = pipelineState.recipe;
  if (!recipe) return;
  const svg = $("pipeline-graph");
  svg.replaceChildren();
  const positions = new Map(),
    depth = new Map();
  let pending = [...recipe.nodes];
  for (let round = 0; round < 24 && pending.length; round++) {
    const ready = pending.filter((n) => n.inputs.every((id) => depth.has(id)));
    for (const node of ready)
      depth.set(
        node.id,
        node.inputs.length
          ? Math.max(...node.inputs.map((id) => depth.get(id))) + 1
          : 0,
      );
    pending = pending.filter((n) => !depth.has(n.id));
  }
  for (const node of pending) depth.set(node.id, 0);
  const rows = new Map();
  for (const node of recipe.nodes) {
    const row = depth.get(node.id);
    if (!rows.has(row)) rows.set(row, []);
    rows.get(row).push(node);
  }
  const longEdges = recipe.nodes.flatMap((node) =>
    node.inputs.flatMap((input, port) =>
      depth.get(node.id) - depth.get(input) > 1
        ? [`${node.id}:${port}`]
        : [],
    ),
  );
  const gutter = 28 + Math.ceil(longEdges.length / 2) * 12;
  const width = Math.max(
    500,
    ...[...rows.values()].map((nodes) => nodes.length * 208 + 40),
  ) + gutter * 2;
  const height = Math.max(250, (Math.max(0, ...depth.values()) + 1) * 110 + 50);
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.style.width = `${width}px`;
  const defs = svgNode("defs", {}),
    marker = svgNode("marker", {
      id: "pipeline-arrow",
      viewBox: "0 0 10 10",
      refX: 9,
      refY: 5,
      markerWidth: 5,
      markerHeight: 5,
      orient: "auto-start-reverse",
    });
  marker.append(svgNode("path", { d: "M0 0 L10 5 L0 10 Z", fill: "#b4c9d5" }));
  defs.append(marker);
  svg.append(defs);
  for (const [row, nodes] of rows)
    nodes.forEach((node, index) =>
      positions.set(node.id, {
        x: (width - nodes.length * 208) / 2 + index * 208 + 14,
        y: row * 110 + 24,
      }),
    );
  for (const node of recipe.nodes)
    for (const [port, input] of node.inputs.entries()) {
      if (!positions.has(input)) continue;
      const a = positions.get(input),
        b = positions.get(node.id),
        middle = (a.y + 54 + b.y) / 2;
      const decision = pipelineState.result?.decisions?.find(
        (d) => d.node === node.id,
      );
      const selectedPort = decision
        ? { yes: 1, no: 2, unknown: 3 }[decision.outcome]
        : -1;
      const taken = node.op === "choose" && port > 0 && port === selectedPort;
      const skipped =
        pipelineState.result?.skipped?.includes(input) ||
        pipelineState.result?.skipped?.includes(node.id) ||
        (decision && port > 0 && port !== selectedPort);
      const destination =
        b.x + (node.op === "choose" ? [24, 68, 112, 156][port] : 90);
      const lane = longEdges.indexOf(`${node.id}:${port}`);
      const rail = lane % 2 === 0
        ? 20 + Math.floor(lane / 2) * 12
        : width - 20 - Math.floor(lane / 2) * 12;
      const path = lane < 0
        ? `M${a.x + 90} ${a.y + 54} V${middle} H${destination} V${b.y - 4}`
        : `M${a.x + 90} ${a.y + 54} V${a.y + 66} H${rail} V${b.y - 28} H${destination} V${b.y - 4}`;
      svg.append(
        svgNode("path", {
          d: path,
          class: `pipeline-wire${[input, node.id].includes(pipelineState.selected) ? " active" : ""}${taken ? " taken" : ""}${skipped ? " skipped" : ""}`,
          "marker-end": "url(#pipeline-arrow)",
        }),
      );
    }
  // Labels sit above every trace, so input arrows never obscure their words.
  for (const node of recipe.nodes) {
    if (node.op === "choose") {
      const b = positions.get(node.id);
      for (const [port, text] of ["test", "yes", "no", "?"].entries()) {
        const destination = b.x + [24, 68, 112, 156][port];
        svg.append(svgNode("rect", {
          x: destination - 17, y: b.y - 22, width: 34, height: 14,
          class: "pipeline-port-background",
        }));
        const label = svgNode("text", {
          x: destination,
          y: b.y - 11,
          class: "pipeline-port-label",
          "text-anchor": "middle",
        });
        label.textContent = text;
        svg.append(label);
      }
    }
  }
  for (const node of recipe.nodes) {
    const point = positions.get(node.id),
      output = recipe.outputs.includes(node.id);
    const group = svgNode("g", {
      transform: `translate(${point.x},${point.y})`,
      class: `pipeline-node${pipelineState.selected === node.id ? " selected" : ""}${pipelineState.result?.skipped?.includes(node.id) ? " skipped" : ""}`,
      role: "button",
      tabindex: 0,
      "aria-label": `${node.id}: ${operationNames[node.op]}${output ? ", output" : ""}${pipelineState.result ? (pipelineState.result.executed.includes(node.id) ? ", ran" : ", skipped") : ""}`,
    });
    group.append(svgNode("rect", { width: 180, height: 54 }));
    for (const [text, y, cls] of [
      [operationNames[node.op] || node.op, 22, ""],
      [
        `${node.id.length > 15 ? node.id.slice(0, 14) + "…" : node.id} · ${pipelineState.result ? (pipelineState.result.executed.includes(node.id) ? "ran" : "skipped") : pipelineKind(node)}`,
        40,
        "pipeline-node-id",
      ],
    ]) {
      const label = svgNode("text", { x: 12, y, class: cls });
      label.textContent = text;
      group.append(label);
    }
    if (output) group.append(svgNode("circle", { cx: 169, cy: 12, r: 3 }));
    group.addEventListener("click", () => pipelineSelect(node.id));
    group.addEventListener("keydown", (event) => {
      if (["Enter", " "].includes(event.key)) {
        event.preventDefault();
        pipelineSelect(node.id);
        $("pipeline-node").focus();
      }
    });
    svg.append(group);
  }
  $("pipeline-map-summary").textContent =
    `${recipe.nodes.length} operations · ${recipe.outputs.length} outputs`;
  centerPipelineSelection();
}
function centerPipelineSelection() {
  const scroll = $("pipeline-graph").parentElement;
  const selected = scroll.querySelector(".pipeline-node.selected");
  if (!selected || !scroll.clientWidth) return;
  const node = selected.getBoundingClientRect();
  const viewport = scroll.getBoundingClientRect();
  scroll.scrollLeft += node.left + node.width / 2 - viewport.left - viewport.width / 2;
  scroll.scrollTop += node.top + node.height / 2 - viewport.top - viewport.height / 2;
}
new ResizeObserver(centerPipelineSelection).observe($("pipeline-graph").parentElement);
function pipelineField(label, control) {
  const wrapper = element("label", "", label);
  wrapper.append(control);
  return wrapper;
}
function pipelineInput(value, onChange, type = "text", max = null) {
  const input = element("input");
  input.type = type;
  input.value = value;
  if (max) input.maxLength = max;
  input.addEventListener("input", () => {
    onChange(type === "number" ? Number(input.value) : input.value);
    pipelineChanged();
    if (max === 48) drawPipelineBindings();
  });
  return input;
}
function pipelineOptions(values, selected, onChange) {
  const select = element("select");
  for (const [value, label] of values) {
    const option = element("option", "", label);
    option.value = value;
    select.append(option);
  }
  select.value = String(selected);
  select.addEventListener("change", () => onChange(select.value));
  return select;
}
function drawPipelineEditor() {
  const recipe = pipelineState.recipe,
    selector = $("pipeline-node");
  selector.replaceChildren();
  for (const node of recipe.nodes) {
    const option = element(
      "option",
      "",
      `${node.id} · ${operationNames[node.op]}`,
    );
    option.value = node.id;
    selector.append(option);
  }
  let node =
    recipe.nodes.find((n) => n.id === pipelineState.selected) ||
    recipe.nodes[0];
  pipelineState.selected = node?.id;
  selector.value = node?.id || "";
  const settings = $("pipeline-settings");
  settings.replaceChildren();
  $("pipeline-remove").disabled = !node || pipelineState.running;
  if (!node) {
    settings.append(element("p", "", "Add a PDF input to start a recipe."));
    return;
  }
  if (!["input", "stop"].includes(node.op)) {
    const count =
      node.op === "choose"
        ? 4
        : ["keep", "stop-after"].includes(node.op)
          ? node.kinds.length
        : node.op === "assemble"
          ? Math.max(1, node.inputs.length)
          : 1;
    for (let i = 0; i < count; i++) {
      const kind =
        ["keep", "stop-after"].includes(node.op)
          ? node.kinds[i]
          : node.op === "choose"
          ? i === 0
            ? node.test.kind === "validation"
              ? "receipt"
              : "document"
            : node.kind
          : ["validate", "preflight", "preflight-rules", "convert-pdfa-3b"].includes(node.op)
            ? "file"
            : "document";
      const available = recipe.nodes.filter(
        (n) => n.id !== node.id && pipelineKind(n) === kind,
      );
      const choices = [
        ["", "Choose a connection"],
        ...available.map((n) => [n.id, `${n.id} · ${operationNames[n.op]}`]),
      ];
      const select = pipelineOptions(choices, node.inputs[i] || "", (value) => {
        node.inputs[i] = value;
        pipelineChanged(true);
      });
      const row = element("div", "pipeline-port");
      row.append(
        pipelineField(
          node.op === "choose"
            ? (node.test.kind === "validation"
                ? [
                    "Validation receipt",
                    "When passed",
                    "When failed",
                    "When unable to check",
                  ]
                : [
                    "Document to inspect",
                    "When matched",
                    "When not matched",
                    "When unknown",
                  ])[i]
            : ["keep", "stop-after"].includes(node.op)
              ? `Input ${i + 1} · ${kind}`
            : node.op === "assemble"
              ? `Input ${i + 1}`
              : `Input ${kind}`,
          select,
        ),
      );
      if (node.op === "assemble" && node.inputs.length > 1) {
        const remove = element("button", "text-button", "Remove");
        remove.setAttribute("aria-label", `Remove assembly input ${i + 1}`);
        remove.onclick = () => {
          node.inputs.splice(i, 1);
          node.catalog = Math.min(node.catalog, node.inputs.length - 1);
          pipelineChanged(true);
        };
        row.append(remove);
      }
      settings.append(row);
    }
    if (node.op === "assemble" && node.inputs.length < 4) {
      const add = element("button", "text-button", "Add input connection");
      add.onclick = () => {
        node.inputs.push("");
        pipelineChanged(true);
      };
      settings.append(add);
    }
  }
  const text = (title, key, type = "text", max = null) =>
    settings.append(
      pipelineField(
        title,
        pipelineInput(node[key], (value) => (node[key] = value), type, max),
      ),
    );
  const choice = (title, key, options) =>
    settings.append(
      pipelineField(
        title,
        pipelineOptions(options, node[key], (value) => {
          node[key] = ["catalog", "index"].includes(key) ? Number(value) : value;
          if (key === "kind") {
            if (value === "document")
              recipe.outputs = recipe.outputs.filter((id) => id !== node.id);
            if (node.op === "choose")
              node.inputs = node.inputs.map((id, index) =>
                index === 0 ||
                recipe.nodes.some(
                  (n) => n.id === id && pipelineKind(n) === value,
                )
                  ? id
                  : "",
              );
          }
          pipelineChanged(key === "kind");
        }),
      ),
    );
  switch (node.op) {
    case "keep":
      choice("Value to keep", "index", node.kinds.map((kind, index) => [String(index), `Input ${index + 1} · ${kind}`]));
      settings.append(element("p", "", "Evaluates every connected input, then keeps the selected value. Earlier operations still run."));
      break;
    case "choose": {
      choice("Branch output kind", "kind", [
        ["document", "Document"],
        ["file", "PDF bytes"],
        ["receipt", "Validation receipt"],
      ]);
      const test = pipelineOptions(
        [
          ["page-count", "Page count"],
          ["has-page-numbers", "Visible page numbers"],
          ["declares-3b", "Declares PDF/A-3b (XMP claim)"],
          ["validation", "Completed validation result"],
        ],
        node.test.kind,
        (value) => {
          node.test =
            value === "page-count"
              ? { kind: value, min: 1, max: 3 }
              : value === "validation"
                ? { kind: value, profile: "3b" }
                : { kind: value };
          const expected = value === "validation" ? "receipt" : "document";
          node.inputs[0] =
            recipe.nodes.find(
              (n) => n.id !== node.id && pipelineKind(n) === expected,
            )?.id || "";
          pipelineChanged(true);
        },
      );
      settings.append(pipelineField("Condition", test));
      if (node.test.kind === "page-count") {
        for (const [label, key] of [
          ["Minimum pages", "min"],
          ["Maximum pages", "max"],
        ])
          settings.append(
            pipelineField(
              label,
              pipelineInput(
                node.test[key],
                (value) => (node.test[key] = value),
                "number",
              ),
            ),
          );
      }
      if (node.test.kind === "validation")
        settings.append(
          pipelineField(
            "Profile to inspect",
            pipelineOptions(
              [
                ["3b", "PDF/A-3b"],
                ["2b", "PDF/A-2b"],
                ["3u", "PDF/A-3u"],
              ],
              node.test.profile,
              (value) => {
                node.test.profile = value;
                pipelineChanged();
              },
            ),
          ),
        );
      settings.append(
        element(
          "p",
          "",
          "Only the selected branch runs. Unknown is a separate route; a missing or unfinished check is never a failed verdict.",
        ),
      );
      break;
    }
    case "stop":
    case "stop-after":
      text("Reason to stop", "message", "text", 160);
      choice("Branch output kind", "kind", [
        ["document", "Document"],
        ["file", "PDF bytes"],
        ["receipt", "Validation receipt"],
      ]);
      settings.append(
        element(
          "p",
          "",
          node.op === "stop-after"
            ? "Evaluates every connected input before stopping with this reason. Unselected stops stay skipped."
            : "Stops the run only if this operation is reached. Unselected stops stay skipped.",
        ),
      );
      break;
    case "input":
      text("Input slot", "slot", "text", 48);
      settings.append(
        element(
          "p",
          "",
          "Bind this slot to a PDF above. Several nodes can share one slot.",
        ),
      );
      break;
    case "select": {
      const input = element("input");
      input.value = node.pages.join(", ");
      input.placeholder = "4, 1, 3";
      input.oninput = () => {
        node.pages = input.value
          .split(",")
          .map((n) => (n.trim() ? Number(n.trim()) : 0));
        pipelineChanged();
      };
      settings.append(
        pipelineField("Pages in output order", input),
        element(
          "p",
          "",
          "Use one-based page numbers, separated by commas. Each page can appear once.",
        ),
      );
      break;
    }
    case "assemble":
      choice(
        "Retain catalog from",
        "catalog",
        node.inputs.map((id, i) => [
          String(i),
          `Input ${i + 1}: ${id || "not connected"}`,
        ]),
      );
      break;
    case "stamp":
      text("Stamp text", "text", "text", 80);
      choice("Placement", "placement", [
        ["top", "Top right"],
        ["center", "Center"],
      ]);
      break;
    case "number":
      text("Start at", "start", "number");
      choice("Number format", "style", [
        ["page", "Page 1 of N"],
        ["number", "1, 2, 3…"],
        ["bates", "000001 (Bates)"],
      ]);
      text("Prefix", "prefix", "text", 24);
      break;
    case "preflight-rules": {
      const editor = element("textarea");
      editor.rows = 12;
      editor.value = JSON.stringify(node.rules, null, 2);
      editor.setAttribute("aria-label", "Court rules JSON");
      editor.onchange = () => {
        try {
          node.rules = JSON.parse(editor.value);
          pipelineChanged();
        } catch {
          pipelineError(new Error("Use valid court rules JSON."));
        }
      };
      settings.append(pipelineField("Court rules JSON", editor));
      break;
    }
    case "validate": {
      const fieldset = element("fieldset");
      fieldset.append(element("legend", "", "Validation profiles"));
      for (const profile of ["3b", "2b", "3u"]) {
        const input = element("input");
        input.type = "checkbox";
        input.checked = node.profiles.includes(profile);
        input.onchange = () => {
          node.profiles = input.checked
            ? [...node.profiles, profile]
            : node.profiles.filter((p) => p !== profile);
          pipelineChanged();
        };
        const label = pipelineField(`PDF/A-${profile}`, input);
        label.className = "pipeline-profile";
        label.prepend(input);
        fieldset.append(label);
      }
      settings.append(fieldset);
      break;
    }
  }
  if (pipelineKind(node) !== "document") {
    const checkbox = element("input");
    checkbox.type = "checkbox";
    checkbox.checked = recipe.outputs.includes(node.id);
    checkbox.onchange = () => {
      recipe.outputs = checkbox.checked
        ? [...recipe.outputs, node.id]
        : recipe.outputs.filter((id) => id !== node.id);
      pipelineChanged();
    };
    const label = pipelineField("Include in run outputs", checkbox);
    label.className = "pipeline-output-toggle";
    label.prepend(checkbox);
    settings.append(label);
  }
}
async function bindPipelineInput(slot, file = null) {
  if (pipelineState.running) return;
  const revision = pipelineState.revision;
  pipelineState.preparing++;
  $("pipeline-run").disabled = true;
  try {
    $("pipeline-status").textContent = `Binding ${slot}…`;
    if (file && (!file.size || file.size > 12 * 1024 * 1024))
      throw new Error("Choose a non-empty PDF up to 12 MiB.");
    if (!file && !state.doc) throw new Error("Open a PDF in Explore first.");
    const doc = await request(
      `/api/pipeline-inputs/${encodeURIComponent(slot)}${file ? "" : `?document=${state.doc.id}`}`,
      {
        method: "POST",
        ...(file
          ? {
              body: file,
              headers: { "X-File-Name": encodeURIComponent(file.name) },
            }
          : {}),
      },
    );
    pipelineState.bindings[slot] = doc;
    if (revision === pipelineState.revision)
      $("pipeline-status").textContent = `Input ${slot} is ready`;
    drawPipelineBindings();
  } catch (error) {
    pipelineError(error);
  } finally {
    pipelineState.preparing--;
    $("pipeline-run").disabled =
      pipelineState.running || pipelineState.preparing > 0;
  }
}
function drawPipelineBindings() {
  const holder = $("pipeline-bindings");
  holder.replaceChildren();
  const slots = [
    ...new Set(
      pipelineState.recipe.nodes
        .filter((n) => n.op === "input")
        .map((n) => n.slot),
    ),
  ];
  for (const slot of slots) {
    const doc = pipelineState.bindings[slot],
      row = element("div", "pipeline-binding"),
      info = element("div");
    info.append(
      element("strong", "", slot),
      element(
        "span",
        "",
        doc
          ? `${doc.name} · ${doc.pages.length} pages`
          : "Choose a PDF for this slot",
      ),
    );
    row.append(info);
    const current = element("button", "text-button", "Use current PDF");
    current.setAttribute("aria-label", `Use current PDF for ${slot}`);
    current.onclick = () => bindPipelineInput(slot);
    const choose = element("button", "text-button", "Choose PDF");
    choose.setAttribute("aria-label", `Choose PDF for ${slot}`);
    const file = element("input");
    file.type = "file";
    file.accept = "application/pdf,.pdf";
    file.hidden = true;
    file.onchange = () => {
      if (file.files[0]) bindPipelineInput(slot, file.files[0]);
    };
    choose.onclick = () => file.click();
    row.append(current, choose, file);
    holder.append(row);
    current.disabled = choose.disabled = pipelineState.running;
  }
  if (!slots.length)
    holder.append(
      element("p", "", "Add a PDF input operation to bind a document."),
    );
}
function pipelineSetRecipe(recipe) {
  if (pipelineState.running)
    throw new Error("Wait for the current run before replacing its recipe.");
  for (const control of $("pipeline-pane").querySelectorAll(
    "button,input,select,textarea",
  ))
    control.disabled = false;
  pipelineState.recipe = recipe;
  $("pipeline-run").disabled = pipelineState.preparing > 0;
  pipelineState.selected = recipe.nodes[0]?.id;
  $("pipeline-name").value = recipe.name;
  pipelineChanged(true);
  drawPipelineBindings();
}
async function checkPipeline() {
  const revision = pipelineState.revision;
  try {
    const checked = await pipelineRequest(
      "/api/pipelines/check",
      pipelineState.recipe,
    );
    if (revision !== pipelineState.revision) return;
    $("pipeline-errors").hidden = true;
    $("pipeline-status").textContent =
      `Recipe ready · ${checked.order.length} reachable operations`;
  } catch (error) {
    if (revision === pipelineState.revision) pipelineError(error);
  }
}
async function enterPipelines() {
  if (!pipelineState.recipe) return;
  drawPipelineBindings();
  if (
    state.doc &&
    pipelineState.recipe.nodes.some(
      (n) => n.op === "input" && n.slot === "body",
    ) &&
    !pipelineState.bindings.body
  )
    await bindPipelineInput("body");
}
function addPipelineOperation() {
  const recipe = pipelineState.recipe,
    op = $("pipeline-operation").value;
  if (recipe.nodes.length >= 24)
    return pipelineError(
      new Error("A recipe may contain at most 24 operations."),
    );
  let suffix = 1;
  while (recipe.nodes.some((n) => n.id === `step${suffix}`)) suffix++;
  const id = `step${suffix}`,
    kind = ["validate", "preflight", "preflight-rules", "convert-pdfa-3b"].includes(op) ? "file" : "document";
  let selected = recipe.nodes.find(
    (n) => n.id === pipelineState.selected && pipelineKind(n) === kind,
  );
  selected ||= [...recipe.nodes]
    .reverse()
    .find((n) => pipelineKind(n) === kind);
  const node = {
    id,
    op,
    inputs: ["input", "stop"].includes(op) ? [] : selected ? [selected.id] : [],
  };
  if (op === "stop") {
    Object.assign(node, {
      kind: "document",
      message: "This path requires manual review",
    });
    recipe.version = 2;
  }
  if (op === "choose") {
    if (recipe.nodes.length > 22)
      return pipelineError(
        new Error(
          "Leave room for the choice and its unknown stop (24 operations maximum).",
        ),
      );
    recipe.version = 2;
    const stopId = `${id}-unknown`;
    recipe.nodes.push({
      id: stopId,
      op: "stop",
      message: "The condition could not be determined",
      kind: "document",
      inputs: [],
    });
    Object.assign(node, {
      test: { kind: "page-count", min: 1, max: 3 },
      kind: "document",
      inputs: [
        selected?.id || "",
        selected?.id || "",
        selected?.id || "",
        stopId,
      ],
    });
  }
  if (op === "input") node.slot = `input${suffix}`;
  if (op === "select") node.pages = [1];
  if (op === "assemble") node.catalog = 0;
  if (op === "stamp") Object.assign(node, { text: "DRAFT", placement: "top" });
  if (op === "number")
    Object.assign(node, { start: 1, style: "page", prefix: "" });
  if (op === "validate") node.profiles = ["3b"];
  if (op === "preflight") node.profile = "ca-2dca-petition-2026-07-01-v1";
  if (
    ["select", "stamp", "number", "assemble", "choose"].includes(op) &&
    selected
  ) {
    for (const other of recipe.nodes)
      other.inputs = other.inputs.map((input) =>
        input === selected.id ? id : input,
      );
  }
  recipe.nodes.push(node);
  if (["write", "validate", "preflight", "preflight-rules", "convert-pdfa-3b"].includes(op)) recipe.outputs.push(id);
  pipelineState.selected = id;
  pipelineChanged(true);
  drawPipelineBindings();
}
async function loadPipelineFile(file) {
  pipelineState.preparing++;
  $("pipeline-run").disabled = true;
  try {
    if (!file || file.size > 32 * 1024)
      throw new Error("Choose recipe JSON up to 32 KiB.");
    const value = JSON.parse(await file.text());
    const checked = await pipelineRequest("/api/pipelines/check", value);
    if (pipelineState.fileMode === "insert") {
      pipelineState.fragment = checked.recipe;
      const slots = [
        ...new Set(
          checked.recipe.nodes
            .filter((n) => n.op === "input")
            .map((n) => n.slot),
        ),
      ];
      const available = pipelineState.recipe.nodes.filter(
        (n) => pipelineKind(n) === "document",
      );
      if (!available.length)
        throw new Error("Add a document operation before inserting a recipe.");
      $("fragment-name").textContent =
        `${checked.recipe.name} · connect its document inputs to the current graph.`;
      $("fragment-bindings").replaceChildren();
      for (const slot of slots) {
        const select = pipelineOptions(
          available.map((n) => [n.id, `${n.id} · ${operationNames[n.op]}`]),
          pipelineState.selected,
          () => {},
        );
        select.dataset.slot = slot;
        $("fragment-bindings").append(pipelineField(`Input ${slot}`, select));
      }
      $("pipeline-fragment").hidden = false;
      $("fragment-apply").focus();
    } else {
      pipelineSetRecipe(checked.recipe);
      await enterPipelines();
      await checkPipeline();
    }
  } catch (error) {
    pipelineError(error);
  } finally {
    pipelineState.preparing--;
    $("pipeline-run").disabled =
      pipelineState.running || pipelineState.preparing > 0;
  }
}
function renderPipelineResults(result) {
  const holder = $("pipeline-output-list");
  holder.replaceChildren();
  $("pipeline-executed").textContent =
    `${result.executed.length} operations ran once · ${result.skipped?.length || 0} skipped · ${result.outputs.length} outputs · ${result.validatorPasses} validator pass${result.validatorPasses === 1 ? "" : "es"}. File downloads and receipts refer to this completed run.`;
  if (result.decisions?.length) {
    const section = element("section", "pipeline-decisions");
    section.append(element("h4", "", "Routing decisions"));
    const list = element("ol");
    for (const decision of result.decisions) {
      const item = element("li");
      const validation = result.recipe.nodes.find(
        (node) => node.id === decision.node,
      )?.test?.kind === "validation";
      const outcome = validation
        ? { yes: "passed", no: "failed", unknown: "unable to check" }
        : { yes: "matched", no: "not matched", unknown: "unknown" };
      item.append(
        element(
          "strong",
          "",
          `${decision.node}: ${outcome[decision.outcome]} → ${decision.selected}`,
        ),
        element("p", "", decision.reason),
      );
      list.append(item);
    }
    section.append(list);
    holder.append(section);
  }
  const trace = element("details", "pipeline-trace");
  trace.append(
    element(
      "summary",
      "",
      `Execution trace · ${result.executed.length} ran · ${result.skipped?.length || 0} skipped`,
    ),
  );
  trace.append(element("p", "", `Ran: ${result.executed.join(" → ")}`));
  for (const [index, pass] of (result.validationRuns || []).entries())
    trace.append(
      element(
        "p",
        "",
        `Validation pass ${index + 1}: ${pass.profiles.map((p) => `PDF/A-${p}`).join(", ")} · SHA-256 ${pass.sha256}`,
      ),
    );
  if (result.skipped?.length)
    trace.append(element("p", "", `Skipped: ${result.skipped.join(", ")}`));
  holder.append(trace);
  for (const output of result.outputs) {
    if (output.kind === "file") {
      const doc = output.document,
        row = element("div", "pipeline-file-output"),
        info = element("div"),
        actions = element("div", "pipeline-file-actions");
      info.append(
        element("strong", "", doc.name),
        element(
          "span",
          "",
          `${doc.pages.length} pages · ${bytes(doc.bytes)} · ${output.node}`,
        ),
      );
      const inspect = element("button", "text-button", "Inspect output");
      inspect.onclick = () => {
        activate(doc);
        selectStudioTool(false);
      };
      const link = element("a", "button primary", "Download PDF");
      link.href = `/api/documents/${doc.id}/file`;
      link.download = doc.name;
      actions.append(inspect, link);
      row.append(info, actions);
      holder.append(row);
    } else {
      const report = output.report,
        row = element("section", "pipeline-receipt");
      if (report.kind === "court-preflight") {
        row.append(element("h4", "", report.profile.name));
        const statusLabels = { pass: "Pass", "needs-review": "Needs review", "fix-required": "Fix required" };
        row.append(element("p", "", statusLabels[report.status]));
        const source = element("a", "text-button", "Court formatting rules");
        source.href = report.profile.source;
        const details = element("details");
        details.append(element("summary", "", `${report.findings.length} findings`));
        details.open = true;
        const doc = result.outputs.find((item) => item.kind === "file" && item.document.sha256 === report.sha256)?.document;
        for (const finding of report.findings) {
          const line = element("p", "", `${statusLabels[finding.status]} · ${finding.rule} · ${finding.message}`);
          if (doc && finding.page) {
            const jump = element("button", "text-button", `Open page ${finding.page}`);
            jump.onclick = () => {
              activate(doc);
              selectStudioTool(false);
              showPage(finding.page);
            };
            line.append(jump);
          }
          details.append(line);
        }
        row.append(source, details, element("p", "receipt-hash", `SHA-256 ${report.sha256}`));
        holder.append(row);
        continue;
      }
      row.append(element("h4", "", `Validation · ${output.node}`));
      const verdicts = element("div", "pipeline-verdicts");
      for (const result of report.validation.results || [])
        verdicts.append(
          resultBadge(
            result.status,
            `PDF/A-${result.profile}: ${result.status === "passed" ? "Verified" : result.status === "failed" ? "Failed" : "Unable to check"}`,
          ),
        );
      if (report.validation.error)
        row.append(element("p", "", report.validation.error));
      row.append(
        verdicts,
        element("p", "receipt-hash", `SHA-256 ${report.sha256}`),
      );
      if (report.validation.results) {
        const details = element("details");
        details.append(element("summary", "", "Validation evidence"));
        renderEvidence(details, report);
        row.append(details);
      }
      holder.append(row);
    }
  }
  $("pipeline-results").hidden = false;
}
$("pipeline-tab").onclick = () => selectStudioTool("pipeline");
$("pipeline-node").onchange = (event) => pipelineSelect(event.target.value);
$("pipeline-name").oninput = () => {
  pipelineState.recipe.name = $("pipeline-name").value;
  pipelineChanged();
};
$("pipeline-add").onclick = addPipelineOperation;
$("pipeline-remove").onclick = () => {
  const recipe = pipelineState.recipe,
    node = recipe.nodes.find((n) => n.id === pipelineState.selected);
  if (!node) return;
  recipe.nodes = recipe.nodes.filter((n) => n !== node);
  for (const other of recipe.nodes)
    other.inputs = other.inputs.flatMap((id) =>
      id === node.id
        ? node.inputs.length === 1 && pipelineKind(node) === "document"
          ? node.inputs
          : []
        : [id],
    );
  recipe.outputs = recipe.outputs.filter((id) => id !== node.id);
  pipelineState.selected = recipe.nodes[0]?.id;
  pipelineChanged(true);
  drawPipelineBindings();
};
$("pipeline-check").onclick = checkPipeline;
$("pipeline-save").onclick = async () => {
  try {
    const result = await pipelineRequest(
      "/api/pipelines/check",
      pipelineState.recipe,
    );
    pipelineDownload(result.recipe, "paper-machine-recipe.json");
  } catch (error) {
    pipelineError(error);
  }
};
for (const mode of ["load", "insert"])
  $(`pipeline-${mode}`).onclick = () => {
    pipelineState.fileMode = mode;
    $("pipeline-file").click();
  };
$("pipeline-file").onchange = () => {
  loadPipelineFile($("pipeline-file").files[0]);
  $("pipeline-file").value = "";
};
$("fragment-cancel").onclick = () => {
  pipelineState.fragment = null;
  $("pipeline-fragment").hidden = true;
};
$("fragment-apply").onclick = async () => {
  try {
    const fragment = pipelineState.fragment,
      recipe = structuredClone(pipelineState.recipe),
      bindings = Object.create(null);
    for (const select of $("fragment-bindings").querySelectorAll("select"))
      bindings[select.dataset.slot] = select.value;
    let n = 1;
    while (recipe.nodes.some((node) => node.id.startsWith(`part${n}-`))) n++;
    const aliases = Object.fromEntries(
      fragment.nodes.map((node) => [
        node.id,
        node.op === "input" ? bindings[node.slot] : `part${n}-${node.id}`,
      ]),
    );
    recipe.version = Math.max(recipe.version, fragment.version);
    recipe.nodes.push(
      ...fragment.nodes
        .filter((node) => node.op !== "input")
        .map((node) => ({
          ...node,
          id: aliases[node.id],
          inputs: node.inputs.map((id) => aliases[id]),
        })),
    );
    recipe.outputs = [
      ...new Set([
        ...recipe.outputs,
        ...fragment.outputs.map((id) => aliases[id]),
      ]),
    ];
    const checked = await pipelineRequest("/api/pipelines/check", recipe);
    pipelineSetRecipe(checked.recipe);
    pipelineState.fragment = null;
    $("pipeline-fragment").hidden = true;
    await checkPipeline();
  } catch (error) {
    pipelineError(error);
  }
};
$("pipeline-apply").onclick = async () => {
  try {
    const value = JSON.parse($("pipeline-json").value);
    const checked = await pipelineRequest("/api/pipelines/check", value);
    pipelineSetRecipe(checked.recipe);
    await enterPipelines();
    await checkPipeline();
  } catch (error) {
    pipelineError(error);
  }
};
$("pipeline-capture").onclick = async () => {
  try {
    if (!state.doc || !state.order.length)
      return pipelineError(
        new Error("Open a PDF and select some pages in Explore first."),
      );
    const nodes = [
      { id: "body", op: "input", slot: "body", inputs: [] },
      { id: "cut", op: "select", pages: [...state.order], inputs: ["body"] },
    ];
    let last = "cut",
      catalog = 0;
    const sources = [];
    for (const [slot, name] of [
      ["head", "front"],
      ["tail", "back"],
    ])
      if (state.bookends[slot]) {
        nodes.push({ id: name, op: "input", slot: name, inputs: [] });
        pipelineState.bindings[name] = await request(
          `/api/pipeline-inputs/${name}?document=${state.bookends[slot].id}`,
          { method: "POST" },
        );
        sources.push(name);
      }
    if (sources.length) {
      const inputs = [
        ...(sources.includes("front") ? ["front"] : []),
        last,
        ...(sources.includes("back") ? ["back"] : []),
      ];
      catalog = inputs.indexOf(last);
      nodes.push({ id: "assembly", op: "assemble", catalog, inputs });
      last = "assembly";
    }
    if ($("stamp-enabled").checked) {
      nodes.push({
        id: "stamp",
        op: "stamp",
        text: $("stamp-text").value,
        placement: $("stamp-placement").value,
        inputs: [last],
      });
      last = "stamp";
    }
    if ($("number-enabled").checked) {
      nodes.push({
        id: "numbers",
        op: "number",
        start: Number($("number-start").value),
        style: $("number-style").value,
        prefix: $("number-prefix").value,
        inputs: [last],
      });
      last = "numbers";
    }
    nodes.push({ id: "pdf", op: "write", inputs: [last] });
    const outputs = ["pdf"];
    if ($("packet-validate").checked) {
      nodes.push({
        id: "checks",
        op: "validate",
        profiles: selectedProfiles(),
        inputs: ["pdf"],
      });
      outputs.push("checks");
    }
    pipelineSetRecipe({ version: 1, name: "My packet", nodes, outputs });
    await bindPipelineInput("body");
    await checkPipeline();
  } catch (error) {
    pipelineError(error);
  }
};
$("pipeline-run").onclick = async () => {
  if (pipelineState.running || pipelineState.preparing || !pipelineState.recipe)
    return;
  const runRecipe = structuredClone(pipelineState.recipe);
  pipelineState.running = true;
  pipelineState.result = null;
  drawPipelineGraph();
  $("pipeline-errors").hidden = true;
  $("pipeline-results").hidden = true;
  $("pipeline-status").textContent =
    "Running the graph · writing PDFs and checking selected profiles…";
  for (const control of $("pipeline-pane").querySelectorAll(
    "button,input,select,textarea",
  ))
    control.disabled = true;
  try {
    const checked = await pipelineRequest("/api/pipelines/check", runRecipe);
    const slots = [
        ...new Set(
          runRecipe.nodes
            .filter((n) => n.op === "input" && checked.order.includes(n.id))
            .map((n) => n.slot),
        ),
      ],
      bindings = Object.create(null);
    for (const slot of slots) {
      if (pipelineState.bindings[slot])
        bindings[slot] = pipelineState.bindings[slot].id;
    }
    const result = await pipelineRequest("/api/pipelines/run", {
      recipe: runRecipe,
      bindings,
    });
    pipelineState.result = result;
    renderPipelineResults(result);
    drawPipelineGraph();
    $("pipeline-status").textContent =
      "Pipeline complete · outputs and receipts are ready";
  } catch (error) {
    pipelineError(error);
  } finally {
    pipelineState.running = false;
    for (const control of $("pipeline-pane").querySelectorAll(
      "button,input,select,textarea",
    ))
      control.disabled = false;
    drawPipelineBindings();
  }
};
$("pipeline-report").onclick = () => {
  if (pipelineState.result)
    pipelineDownload(pipelineState.result, "paper-machine-pipeline-run.json");
};
document.addEventListener("pdf-activated", () => {
  if (location.hash === "#pipeline") enterPipelines();
});
for (const control of $("pipeline-pane").querySelectorAll(
  "button,input,select,textarea",
))
  control.disabled = true;
for (const id of [
  "pipeline-load",
  "pipeline-file",
  "pipeline-json",
  "pipeline-apply",
])
  $(id).disabled = false;
(async () => {
  try {
    const response = await fetch("/api/pipelines/example");
    if (!response.ok)
      throw new Error("The example recipe could not load. Reload the studio.");
    pipelineSetRecipe(await response.json());
    await checkPipeline();
    if (location.hash === "#pipeline") {
      await enterPipelines();
    }
  } catch (error) {
    pipelineError(error);
  }
})();

$("pipeline-example").onchange = async () => {
  if (pipelineState.running) return;
  pipelineState.preparing++;
  $("pipeline-run").disabled = true;
  try {
    const query = $("pipeline-example").value;
    if (!query) return;
    const response = await fetch(
      `/api/pipelines/example${query === "packet" ? "" : `?${query}`}`,
    );
    if (!response.ok) throw new Error("The example could not load.");
    pipelineSetRecipe(await response.json());
    await enterPipelines();
    await checkPipeline();
  } catch (error) {
    pipelineError(error);
  } finally {
    $("pipeline-example").value = "";
    pipelineState.preparing--;
    $("pipeline-run").disabled =
      pipelineState.running || pipelineState.preparing > 0;
  }
};
