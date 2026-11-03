"use strict";

const $ = (id) => document.getElementById(id);
const NS = "http://www.w3.org/2000/svg";
const state = {
  doc: null,
  page: 1,
  selected: null,
  order: [],
  busy: false,
  preview: 0,
  bookends: { head: null, tail: null },
};
const paths = {
  left: "m14 6-6 6 6 6",
  right: "m10 6 6 6-6 6",
  remove: "m6 6 12 12M6 18 18 6",
  add: "M12 5v14M5 12h14",
};
function icon(name) {
  const svg = document.createElementNS(NS, "svg");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("aria-hidden", "true");
  const path = document.createElementNS(NS, "path");
  path.setAttribute("d", paths[name]);
  svg.append(path);
  return svg;
}
function element(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}
function address(id) {
  return id.replace(":", " ") + " R";
}
function bytes(size) {
  return size < 1024 ? `${size} B` : `${(size / 1024).toFixed(1)} KiB`;
}
function notify(message = "", error = false) {
  $("notice").textContent = message;
  $("notice").classList.toggle("error", error);
}
function busy(value) {
  state.busy = value;
  document.querySelectorAll("[data-busy]").forEach((button) => {
    button.disabled = value;
  });
  $("build").disabled = value || !state.order.length;
  $("workspace").setAttribute("aria-busy", String(value));
}
async function request(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    signal: AbortSignal.timeout(45000),
  });
  const result = await response.json();
  if (!response.ok)
    throw new Error(
      result.error || "The PDF operation failed. Please try another file.",
    );
  return result;
}
async function operation(message, run) {
  if (state.busy) return;
  busy(true);
  notify(message);
  try {
    await run();
    notify();
  } catch (error) {
    notify(
      error.name === "TimeoutError"
        ? "The operation took too long. Try a smaller or simpler PDF."
        : error.message,
      true,
    );
  } finally {
    busy(false);
  }
}
function previewURL(page) {
  return `/api/documents/${state.doc.id}/preview?page=${page}`;
}
function thumbnail(page, className) {
  const image = element("img", className);
  image.src = previewURL(page);
  image.alt = `Page ${page}`;
  image.loading = "lazy";
  return image;
}
function activate(doc) {
  state.doc = doc;
  resetPacketOptions();
  state.page = 1;
  state.order = doc.pages.map((page) => page.number);
  state.selected = doc.pages[0].ref;
  $("filename").textContent = doc.name;
  $("filemeta").textContent =
    `${bytes(doc.bytes)} · PDF ${doc.version} · ${doc.pages.length} page${doc.pages.length === 1 ? "" : "s"}`;
  $("map-summary").textContent =
    `${doc.objectCount} objects · ${doc.streamCount} streams` +
    (doc.objectCount > doc.objects.length
      ? ` · showing ${doc.objects.length}, including all pages`
      : " · every reference is real");
  $("object-select").replaceChildren();
  for (const obj of doc.objects) {
    const option = element("option", "", `${address(obj.id)} · ${obj.kind}`);
    option.value = obj.id;
    $("object-select").append(option);
  }
  const trailer = element("option", "", "Document trailer");
  trailer.value = "trailer";
  $("object-select").append(trailer);
  $("trailer").disabled = false;
  $("restore").disabled = false;
  $("inspect-page").disabled = false;
  $("result").hidden = true;
  drawGraph();
  showObject(state.selected);
  showPage(1);
  drawSequence();
  if (typeof updateValidationStatus === "function") updateValidationStatus();
  document.dispatchEvent(new Event("pdf-activated"));
}

function drawGraph() {
  const graph = $("graph");
  graph.replaceChildren();
  const nodes = state.doc.objects;
  const columns = [[], [], [], [], []];
  for (const node of nodes) {
    const col =
      node.id === state.doc.root
        ? 0
        : node.kind === "Pages"
          ? 1
          : node.kind === "Page"
            ? 2
            : node.streamBytes > 0 || node.kind === "Stream"
              ? 3
              : 4;
    columns[col].push(node);
  }
  const height = Math.max(
    440,
    Math.max(...columns.map((column) => column.length)) * 84 + 40,
  );
  graph.setAttribute("viewBox", `0 0 740 ${height}`);
  graph.setAttribute("height", height);
  graph.classList.toggle("graph-tall", height > 440);
  const positions = new Map();
  columns.forEach((column, col) =>
    column.forEach((node, index) => {
      let y =
        column.length === 1
          ? height / 2 - 24
          : 30 + index * ((height - 100) / Math.max(1, column.length - 1));
      if (col === 3) {
        const owner = columns[2].find((page) => page.refs.includes(node.id));
        if (owner && positions.has(owner.id)) y = positions.get(owner.id).y;
      }
      positions.set(node.id, { x: [15, 160, 315, 485, 625][col], y });
    }),
  );
  for (const from of nodes)
    for (const to of from.refs) {
      if (!positions.has(to)) continue;
      const a = positions.get(from.id),
        b = positions.get(to);
      const forward = b.x > a.x;
      const x1 = a.x + (forward ? 100 : 0),
        x2 = b.x + (forward ? 0 : 100);
      const mid = (x1 + x2) / 2;
      const edge = document.createElementNS(NS, "path");
      // Route long resource references between the rows, clear of stream boxes.
      edge.setAttribute(
        "d",
        a.x === 315 && b.x === 625
          ? `M${a.x + 50} ${a.y + 48} V${a.y + 62} H610 V${b.y + 24} H${b.x}`
          : `M${x1} ${a.y + 24} H${mid} V${b.y + 24} H${x2}`,
      );
      const title = document.createElementNS(NS, "title");
      title.textContent = `${address(from.id)} references ${address(to)}`;
      edge.append(title);
      edge.setAttribute("class", "trace");
      edge.dataset.from = from.id;
      edge.dataset.to = to;
      graph.append(edge);
    }
  for (const obj of nodes) {
    const point = positions.get(obj.id);
    const group = document.createElementNS(NS, "g");
    group.setAttribute("transform", `translate(${point.x},${point.y})`);
    group.setAttribute(
      "class",
      "node" +
        (obj.kind === "Page" ? " page" : obj.streamBytes ? " stream" : ""),
    );
    group.dataset.id = obj.id;
    group.setAttribute("role", "button");
    group.setAttribute("tabindex", "0");
    group.setAttribute("aria-label", `${obj.kind}, object ${address(obj.id)}`);
    const rect = document.createElementNS(NS, "rect");
    rect.setAttribute("width", "100");
    rect.setAttribute("height", "48");
    group.append(rect);
    for (const [text, className, y] of [
      [obj.kind, "node-title", 19],
      [address(obj.id), "node-address", 36],
    ]) {
      const label = document.createElementNS(NS, "text");
      label.setAttribute("x", "11");
      label.setAttribute("y", y);
      label.setAttribute("class", className);
      label.textContent = text.length > 13 ? text.slice(0, 12) + "…" : text;
      group.append(label);
    }
    for (const cx of [0, 100]) {
      const port = document.createElementNS(NS, "circle");
      port.setAttribute("class", "port");
      port.setAttribute("cx", cx);
      port.setAttribute("cy", "24");
      port.setAttribute("r", "2.5");
      group.append(port);
    }
    const select = () => showObject(obj.id);
    group.addEventListener("click", select);
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        select();
      }
    });
    graph.append(group);
  }
}

function showObject(id) {
  state.selected = id;
  const obj = state.doc.objects.find((node) => node.id === id);
  $("object-select").value = obj ? id : "trailer";
  $("object-kind").textContent = obj
    ? `${obj.kind} · ${address(id)}`
    : "Document trailer";
  $("object-size").textContent = obj?.streamBytes
    ? `${bytes(obj.streamBytes)} raw stream`
    : "";
  $("syntax").textContent = obj ? obj.syntax : state.doc.trailer;
  $("syntax").scrollTop = 0;
  $("references").replaceChildren();
  const refs = obj ? obj.refs : [];
  $("reference-label").textContent = obj ? "REFERENCES" : "REWRITE NOTE";
  if (!obj)
    $("references").textContent =
      "Rebuilt output has a fresh xref, Size and Root.";
  else if (!refs.length) $("references").textContent = "No indirect references";
  for (const ref of refs) {
    const button = element("button", "ref-button", address(ref));
    const available = state.doc.objects.some((node) => node.id === ref);
    button.disabled = !available;
    button.title = available
      ? `Inspect object ${address(ref)}`
      : "Outside the displayed graph";
    button.addEventListener("click", () => showObject(ref));
    $("references").append(button);
  }
  const related = new Set(refs);
  state.doc.objects
    .filter((node) => node.refs.includes(id))
    .forEach((node) => related.add(node.id));
  $("graph")
    .querySelectorAll(".node")
    .forEach((node) => {
      const selected = node.dataset.id === id;
      node.classList.toggle("selected", selected);
      node.classList.toggle("related", related.has(node.dataset.id));
      node.setAttribute("aria-pressed", String(selected));
    });
  $("graph")
    .querySelectorAll(".trace")
    .forEach((edge) => {
      edge.classList.toggle(
        "active",
        edge.dataset.from === id || edge.dataset.to === id,
      );
    });
  const page = state.doc.pages.find((page) => page.ref === id);
  if (page && page.number !== state.page) showPage(page.number);
}

function showPage(number) {
  state.page = number;
  const page = state.doc.pages[number - 1];
  $("page-counter").textContent = `${number} / ${state.doc.pages.length}`;
  $("previous").disabled = number <= 1;
  $("next").disabled = number >= state.doc.pages.length;
  $("page-ref").textContent = `Page ${number} · ${address(page.ref)}`;
  const token = ++state.preview;
  const image = $("preview");
  image.hidden = true;
  $("preview-message").hidden = false;
  $("preview-message").textContent = "Rendering page…";
  image.onload = () => {
    if (token !== state.preview) return;
    image.hidden = false;
    $("preview-message").hidden = true;
  };
  image.onerror = () => {
    if (token !== state.preview) return;
    $("preview-message").textContent =
      "This page could not be previewed. Its objects are still available to inspect.";
  };
  image.alt = `Rendered page ${number} of ${state.doc.name}`;
  image.src = previewURL(number);
  drawSources();
}

function drawSources() {
  $("source-pages").replaceChildren();
  for (const page of state.doc.pages) {
    const included = state.order.includes(page.number);
    const button = element(
      "button",
      "source-page" + (page.number === state.page ? " active" : ""),
    );
    button.setAttribute(
      "aria-label",
      `View page ${page.number}${included ? "" : " and add it to the output"}`,
    );
    button.setAttribute("aria-pressed", String(page.number === state.page));
    button.append(
      thumbnail(page.number),
      element("span", "", `Page ${page.number}`),
    );
    if (!included) {
      const add = element("span", "add-page");
      add.append(icon("add"));
      button.append(add);
    }
    button.addEventListener("click", () => {
      if (!state.order.includes(page.number)) {
        state.order.push(page.number);
        drawSequence();
      }
      showPage(page.number);
      showObject(page.ref);
    });
    $("source-pages").append(button);
  }
}

function drawSequence() {
  $("sequence").replaceChildren();
  if (!state.order.length)
    $("sequence").append(
      element(
        "li",
        "sequence-empty",
        "Your cut is empty. Add a page from the bench above.",
      ),
    );
  state.order.forEach((number, index) => {
    const item = element("li", "sequence-item");
    const preview = element("button", "sequence-preview");
    preview.setAttribute(
      "aria-label",
      `Inspect source page ${number}, output position ${index + 1}`,
    );
    preview.append(thumbnail(number), element("span", "position", index + 1));
    preview.addEventListener("click", () => {
      showPage(number);
      showObject(state.doc.pages[number - 1].ref);
    });
    item.append(preview, element("span", "", `Page ${number}`));
    const controls = element("div", "sequence-controls");
    for (const [name, label, disabled, action] of [
      [
        "left",
        `Move page ${number} earlier`,
        index === 0,
        () => {
          [state.order[index - 1], state.order[index]] = [
            state.order[index],
            state.order[index - 1],
          ];
        },
      ],
      [
        "right",
        `Move page ${number} later`,
        index === state.order.length - 1,
        () => {
          [state.order[index + 1], state.order[index]] = [
            state.order[index],
            state.order[index + 1],
          ];
        },
      ],
      [
        "remove",
        `Remove page ${number} from output`,
        false,
        () => {
          state.order.splice(index, 1);
        },
      ],
    ]) {
      const button = element("button");
      button.append(icon(name));
      button.disabled = disabled;
      button.setAttribute("aria-label", label);
      button.title = label;
      button.addEventListener("click", () => {
        action();
        drawSequence();
        drawSources();
        const focusIndex = Math.min(index, state.order.length - 1);
        if (focusIndex >= 0)
          $("sequence")
            .children[focusIndex].querySelector(".sequence-preview")
            .focus({ preventScroll: true });
        else $("restore").focus({ preventScroll: true });
      });
      controls.append(button);
    }
    item.append(controls);
    $("sequence").append(item);
  });
  const added =
    (state.bookends.head?.pages.length || 0) +
    (state.bookends.tail?.pages.length || 0);
  $("selection-summary").textContent = state.order.length
    ? `${state.order.length} selected → ${state.order.join(" · ")}${added ? ` + ${added} front/back = ${state.order.length + added} pages` : ""}`
    : "No pages selected";
  $("build").disabled = state.busy || !state.order.length;
}

async function sample() {
  await operation(
    "Building the synthetic field notes with Kyo PDF…",
    async () => {
      activate(await request("/api/sample", { method: "POST" }));
      $("sample-note").textContent =
        "The built-in field notes are synthetic vector artwork.";
    },
  );
}
async function openFile(file) {
  if (!file) return;
  if (file.size > 12 * 1024 * 1024) {
    notify("Choose a PDF up to 12 MiB. This file is too large.", true);
    return;
  }
  await operation("Reading the PDF structure…", async () => {
    activate(
      await request("/api/documents", {
        method: "POST",
        body: file,
        headers: { "X-File-Name": encodeURIComponent(file.name) },
      }),
    );
    $("sample-note").textContent = "Your file stays on this machine.";
  });
}
$("open").addEventListener("click", () => {
  if (typeof validationState !== "undefined" && validationState.active)
    $("validation-files").click();
  else $("file").click();
});
$("file").addEventListener("change", () => {
  openFile($("file").files[0]);
  $("file").value = "";
});
$("sample").addEventListener("click", sample);
$("previous").addEventListener("click", () => {
  showPage(state.page - 1);
  showObject(state.doc.pages[state.page - 1].ref);
});
$("next").addEventListener("click", () => {
  showPage(state.page + 1);
  showObject(state.doc.pages[state.page - 1].ref);
});
$("inspect-page").addEventListener("click", () => {
  showObject(state.doc.pages[state.page - 1].ref);
  $("syntax").focus({ preventScroll: true });
});
$("object-select").addEventListener("change", (event) =>
  showObject(event.target.value),
);
$("trailer").addEventListener("click", () => showObject("trailer"));
$("restore").addEventListener("click", () => {
  state.order = state.doc.pages.map((page) => page.number);
  drawSequence();
  drawSources();
});
$("build").addEventListener("click", () =>
  operation(
    "Assembling pages, stamps, numbering and the new trailer…",
    async () => {
      const order = [...state.order];
      const validate = $("packet-validate").checked;
      if (
        validate &&
        (!selectedProfiles().length ||
          validationState.running ||
          validationState.items.length >= 20)
      ) {
        throw new Error(
          "Choose validation profiles and leave a free queue slot before building with validation.",
        );
      }
      const options = new URLSearchParams({
        pages: order.join(","),
        head: state.bookends.head?.id || "",
        tail: state.bookends.tail?.id || "",
        stamp: $("stamp-enabled").checked ? $("stamp-text").value : "",
        placement: $("stamp-placement").value,
        numbered: String($("number-enabled").checked),
        start: $("number-enabled").checked ? $("number-start").value : "1",
        style: $("number-enabled").checked ? $("number-style").value : "page",
        prefix: $("number-enabled").checked ? $("number-prefix").value : "",
      });
      if ($("stamp-enabled").checked && !options.get("stamp").trim())
        throw new Error("Enter the stamp text.");
      const added =
        (state.bookends.head?.pages.length || 0) +
        (state.bookends.tail?.pages.length || 0);
      const doc = await request(`/api/documents/${state.doc.id}/compose`, {
        method: "POST",
        body: options,
      });
      activate(doc);
      $("result-summary").textContent =
        `${doc.pages.length} output pages${added ? ` · ${added} front/back pages` : ""} · ${bytes(doc.bytes)} · preview and graph updated`;
      $("download").href = `/api/documents/${doc.id}/file`;
      $("download").download = doc.name;
      $("result").hidden = false;
      if (validate) await validateBuiltDocument(doc);
    },
  ),
);
function resetPacketOptions() {
  state.bookends = { head: null, tail: null };
  for (const slot of ["head", "tail"]) {
    $(`${slot}-details`).textContent = "No PDF added";
    $(`${slot}-clear`).disabled = true;
    $(`${slot}-file`).value = "";
  }
  $("stamp-enabled").checked = false;
  $("number-enabled").checked = false;
  $("packet-validate").checked = false;
  $("stamp-fields").hidden = true;
  $("number-fields").hidden = true;
}
for (const type of ["stamp", "number"]) {
  $(`${type}-enabled`).addEventListener("change", () => {
    $(`${type}-fields`).hidden = !$(`${type}-enabled`).checked;
  });
}
for (const slot of ["head", "tail"]) {
  $(`${slot}-choose`).addEventListener("click", () =>
    $(`${slot}-file`).click(),
  );
  $(`${slot}-clear`).addEventListener("click", () => {
    state.bookends[slot] = null;
    $(`${slot}-details`).textContent = "No PDF added";
    $(`${slot}-clear`).disabled = true;
    drawSequence();
  });
  $(`${slot}-file`).addEventListener("change", () => {
    const file = $(`${slot}-file`).files[0];
    $(`${slot}-file`).value = "";
    if (!file) return;
    operation(
      `Reading ${slot === "head" ? "front" : "back"} pages…`,
      async () => {
        if (!file.size || file.size > 12 * 1024 * 1024)
          throw new Error("Choose a PDF up to 12 MiB.");
        const doc = await request(`/api/bookends/${slot}`, {
          method: "POST",
          body: file,
          headers: { "X-File-Name": encodeURIComponent(file.name) },
        });
        if (
          doc.pages.length +
            state.order.length +
            (state.bookends[slot === "head" ? "tail" : "head"]?.pages.length ||
              0) >
          64
        )
          throw new Error(
            "The final packet may contain at most 64 pages. Choose a smaller PDF.",
          );
        state.bookends[slot] = doc;
        $(`${slot}-details`).textContent =
          `${doc.name} · ${doc.pages.length} page${doc.pages.length === 1 ? "" : "s"}`;
        $(`${slot}-clear`).disabled = false;
        drawSequence();
      },
    );
  });
}
document.addEventListener("dragover", (event) => {
  if (event.dataTransfer.types.includes("Files")) event.preventDefault();
});
document.addEventListener("drop", (event) => {
  if (event.dataTransfer.types.includes("Files")) {
    event.preventDefault();
    if (
      typeof addValidationFiles === "function" &&
      (event.dataTransfer.files.length > 1 || validationState.active)
    ) {
      selectStudioTool(true);
      addValidationFiles(event.dataTransfer.files);
    } else openFile(event.dataTransfer.files[0]);
  }
});
sample();
