"use strict";

const validationState = {
  active: false,
  items: [],
  running: false,
  stop: false,
  nextId: 0,
};
function selectedProfiles() {
  return [...$("validation-profiles").querySelectorAll("input:checked")].map(
    (input) => input.value,
  );
}
function counted(value, noun) {
  return `${value} ${noun}${value === 1 ? "" : "s"}`;
}
function declarationText(found) {
  if (!found || found.state === "Unreadable") return "XMP unreadable";
  if (found.state === "NotDeclared") return "Not declared";
  if (found.state === "Conflicting") return "Conflicting XMP";
  return `Declares PDF/A-${found.parts.join(" / ")}${found.conformances?.join(" / ").toLowerCase() || ""}`;
}
function validationNotice(text = "", error = false) {
  $("validation-notice").textContent = text;
  $("validation-notice").classList.toggle("error", error);
}
function selectStudioTool(tool) {
  const mode = tool === true ? "validate" : tool === false ? "explore" : tool;
  const validate = mode === "validate";
  validationState.active = validate;
  for (const [name, pane] of [
    ["explore", "explore-pane"],
    ["validate", "validation-pane"],
    ["pipeline", "pipeline-pane"],
  ]) {
    const active = name === mode;
    $(pane).hidden = !active;
    $(`${name}-tab`).setAttribute("aria-selected", String(active));
    $(`${name}-tab`).tabIndex = active ? 0 : -1;
  }
  $("open-label").textContent = validate ? "Add PDFs" : "Open a PDF";
  $("open-limits").textContent = validate
    ? "Local files · up to 20 PDFs · 12 MiB each"
    : "Local files · up to 12 MiB · 64 pages";
  $("open").disabled = state.busy || (validate && validationState.running);
  document.querySelector(".skip-link").href = validate
    ? "#validation-pane"
    : mode === "pipeline"
      ? "#pipeline-pane"
      : "#workspace";
  history.replaceState(null, "", `#${mode}`);
  if (mode === "pipeline" && typeof enterPipelines === "function")
    enterPipelines();
}

function updateValidationStatus() {
  if (!state.doc) return;
  const found = validationState.items.find(
    (item) =>
      item.result?.sha256 === state.doc.sha256 &&
      item.result.validation?.results?.some(
        (result) => result.profile === "3b",
      ),
  );
  const verdict = found?.result.validation.results.find(
    (result) => result.profile === "3b",
  );
  $("pdfa-status").textContent =
    verdict?.status === "passed"
      ? "Verified PDF/A-3b"
      : verdict?.status === "failed"
        ? "PDF/A-3b validation failed"
        : `${declarationText(state.doc.identification)} · unverified`;
  $("pdfa-status").classList.toggle("verified", verdict?.status === "passed");
  $("validate-current").disabled = validationState.running;
}
function addValidationFiles(files, synthetic = false, originHash = null) {
  if (validationState.running) return;
  let rejected = 0;
  for (const file of files) {
    const key = originHash
      ? `sha:${originHash}`
      : `${file.name}:${file.size}:${file.lastModified}`;
    if (validationState.items.some((item) => item.key === key)) continue;
    if (
      file.size > 12 * 1024 * 1024 ||
      file.size === 0 ||
      validationState.items.length >= 20
    ) {
      rejected++;
      continue;
    }
    validationState.items.push({
      id: ++validationState.nextId,
      key,
      file,
      synthetic,
      status: "queued",
      result: null,
      error: "",
    });
  }
  validationNotice(
    rejected
      ? `${rejected} file(s) could not be added. Use non-empty PDFs up to 12 MiB and a queue of at most 20.`
      : "",
    rejected > 0,
  );
  drawValidationQueue();
}
function resultBadge(status, text) {
  return element("span", `validation-badge ${status}`, text);
}
function drawValidationQueue() {
  const list = $("validation-queue");
  const retained = new Set(
    validationState.items.map((item) => String(item.id)),
  );
  for (const row of [...list.children]) {
    if (!retained.has(row.dataset.itemId)) row.remove();
  }
  $("validation-empty").hidden = validationState.items.length > 0;
  for (const item of validationState.items) {
    const profiles = item.profiles || selectedProfiles();
    const profileKey = profiles.join(",");
    const view = item.view;
    // Keep completed rows mounted while other files advance: disclosure state,
    // text selection and keyboard focus belong to the person inspecting them.
    if (
      view &&
      view.status === item.status &&
      view.result === item.result &&
      view.error === item.error &&
      view.profileKey === profileKey
    ) {
      for (const button of view.row.querySelectorAll("button")) {
        button.disabled = validationState.running;
      }
      continue;
    }
    const row = element("li", "validation-file");
    row.dataset.itemId = String(item.id);
    row.classList.toggle("checking", item.status === "checking");
    const heading = element("div", "validation-file-heading");
    const identity = element("div", "validation-identity");
    identity.append(
      element("strong", "", item.file.name),
      element(
        "span",
        "",
        `${bytes(item.file.size)}${item.synthetic ? " · synthetic specimen" : ""}`,
      ),
    );
    heading.append(identity);
    const actions = element("div", "validation-file-actions");
    const inspect = element("button", "text-button", "Inspect PDF");
    inspect.disabled = validationState.running;
    inspect.addEventListener("click", async () => {
      selectStudioTool(false);
      if (!item.result || state.doc?.sha256 !== item.result.sha256)
        await openFile(item.file);
      updateValidationStatus();
    });
    const remove = element("button", "icon-button");
    remove.append(icon("remove"));
    remove.disabled = validationState.running;
    remove.setAttribute(
      "aria-label",
      `Remove ${item.file.name} from validation queue`,
    );
    remove.addEventListener("click", () => {
      validationState.items = validationState.items.filter(
        (value) => value.id !== item.id,
      );
      drawValidationQueue();
      updateValidationStatus();
    });
    actions.append(inspect, remove);
    heading.append(actions);
    row.append(heading);
    const outcomes = element("div", "validation-outcomes");
    const claim = element("div", "validation-claim");
    claim.append(
      element("span", "result-label", "XMP identification"),
      element(
        "strong",
        "",
        item.result
          ? declarationText(item.result.identification)
          : "Awaiting inspection",
      ),
    );
    outcomes.append(claim);
    const results = item.result?.validation?.results || [];
    for (const profile of profiles) {
      const check = results.find((value) => value.profile === profile);
      const status =
        check?.status || (item.status === "done" ? "unable" : item.status);
      const box = element("div", "validation-check");
      box.append(
        element("span", "result-label", `PDF/A-${profile}`),
        resultBadge(
          status,
          status === "passed"
            ? "Verified"
            : status === "failed"
              ? "Failed"
              : status === "unable"
                ? "Unable to check"
                : status === "checking"
                  ? "Checking…"
                  : "Queued",
        ),
      );
      outcomes.append(box);
    }
    row.append(outcomes);
    if (item.result || item.error) {
      const details = element("details", "validation-details");
      details.append(element("summary", "", "Evidence & failure details"));
      if (item.error)
        details.append(element("p", "validation-error", item.error));
      if (item.result) renderEvidence(details, item.result);
      row.append(details);
    }
    if (view?.row.isConnected) view.row.replaceWith(row);
    else list.append(row);
    item.view = {
      row,
      status: item.status,
      result: item.result,
      error: item.error,
      profileKey,
    };
  }
  const completed = validationState.items.filter(
    (item) => item.status === "done",
  ).length;
  const verified = validationState.items.reduce(
    (total, item) =>
      total +
      (item.result?.validation?.results || []).filter(
        (value) => value.status === "passed",
      ).length,
    0,
  );
  $("validation-summary").textContent = completed
    ? `${counted(completed, "file")} checked · ${verified} of ${completed * selectedProfiles().length} profile checks verified. Each result is tied to its SHA-256 fingerprint.`
    : "A declaration is evidence. A completed validator pass is a verified result.";
  const active = validationState.items.find(
    (item) => item.status === "checking",
  );
  $("validation-progress-text").textContent = active
    ? `${completed} / ${validationState.items.length} checked · ${active.file.name}`
    : validationState.items.length
      ? `${completed} / ${validationState.items.length} checked${validationState.stop ? " · stopped" : ""}`
      : "Add PDFs to start a validation pass.";
  $("validation-progress").hidden = !validationState.items.length;
  $("validation-progress").max = Math.max(1, validationState.items.length);
  $("validation-progress").value = completed;
  $("validation-run").disabled =
    validationState.running ||
    !validationState.items.length ||
    !selectedProfiles().length;
  $("open").disabled =
    state.busy || (validationState.active && validationState.running);
  $("validation-stop").disabled =
    !validationState.running || validationState.stop;
  $("validation-clear").disabled =
    validationState.running || !validationState.items.length;
  $("validation-add").disabled = validationState.running;
  $("validation-examples").disabled = validationState.running;
  $("validation-files").disabled = validationState.running;
  $("validation-profiles").disabled = validationState.running;
  $("validation-json").disabled = validationState.running || !completed;
  $("validation-csv").disabled = validationState.running || !completed;
  updateValidationStatus();
}
function renderEvidence(parent, result) {
  const receipt = element("dl", "validation-receipt");
  for (const [label, value] of [
    ["Engine", `${result.engine} ${result.engineVersion}`],
    ["Checked", result.checkedAt],
    ["SHA-256", result.sha256],
    [
      "Kyo readability",
      result.readability.status === "passed"
        ? `${counted(result.readability.pages, "page")} resolved`
        : result.readability.message,
    ],
  ]) {
    receipt.append(
      element("dt", "", label),
      element("dd", label === "SHA-256" ? "fingerprint" : "", value),
    );
  }
  parent.append(receipt);
  for (const note of result.identification.notes || [])
    parent.append(element("p", "", note));
  if (result.validation.error)
    parent.append(element("p", "validation-error", result.validation.error));
  for (const check of result.validation.results || []) {
    const group = element("div", "validation-rule-group");
    group.append(
      element(
        "h3",
        "",
        `PDF/A-${check.profile}: ${check.status === "passed" ? "verified" : check.status === "failed" ? "failed" : "unable to check"}`,
      ),
      element(
        "p",
        "",
        `${counted(check.checks, "check")} · ${counted(check.failedRules, "failed rule")} · ${counted(check.failedChecks, "failed check")}`,
      ),
    );
    const failures = element("ol", "validation-failures");
    for (const failure of check.failures) {
      const line = element("li");
      line.append(
        element(
          "strong",
          "",
          `Clause ${failure.clause} · test ${failure.test}`,
        ),
        element("p", "", failure.message),
      );
      if (failure.context) line.append(element("code", "", failure.context));
      failures.append(line);
    }
    if (check.failures.length)
      group.append(
        failures,
        element(
          "p",
          "detail-limit",
          "Showing up to 24 failure examples; the verdict includes all completed checks.",
        ),
      );
    parent.append(group);
  }
}
async function runValidationQueue(onlyIds = null) {
  const profiles = selectedProfiles();
  const targets = onlyIds
    ? validationState.items.filter((item) => onlyIds.has(item.id))
    : validationState.items;
  if (validationState.running || !targets.length || !profiles.length) return;
  validationState.running = true;
  validationState.stop = false;
  validationNotice();
  for (const item of targets) {
    item.status = "queued";
    item.result = null;
    item.error = "";
    item.profiles = [...profiles];
  }
  drawValidationQueue();
  for (const item of targets) {
    if (validationState.stop) break;
    item.status = "checking";
    drawValidationQueue();
    try {
      const response = await fetch(
        `/api/validate?profiles=${profiles.join(",")}`,
        { method: "POST", body: item.file, signal: AbortSignal.timeout(60000) },
      );
      const report = await response.json();
      if (!response.ok)
        throw new Error(report.error || "Validation request failed");
      item.result = report;
    } catch (error) {
      item.error =
        error.name === "TimeoutError"
          ? "The local validator did not respond in time. Run this file again."
          : error.message;
    }
    item.status = "done";
    drawValidationQueue();
  }
  validationState.running = false;
  drawValidationQueue();
}
function saveValidationReport(format) {
  const items = validationState.items.filter((item) => item.status === "done");
  let contents, mime;
  if (format === "json") {
    contents = JSON.stringify(
      {
        schemaVersion: 1,
        exportedAt: new Date().toISOString(),
        files: items.map((item) => ({
          name: item.file.name,
          profiles: item.profiles,
          result: item.result,
          error: item.error || null,
        })),
      },
      null,
      2,
    );
    mime = "application/json";
  } else {
    const cell = (value) => {
      let text = String(value ?? "");
      if (/^[=+\-@\t\r]/.test(text)) text = "'" + text;
      return '"' + text.replaceAll('"', '""') + '"';
    };
    const rows = [
      [
        "file",
        "sha256",
        "declared",
        "profile",
        "verdict",
        "failed_rules",
        "checked_at",
        "engine_version",
      ],
    ];
    for (const item of items)
      for (const profile of item.profiles) {
        const result = item.result?.validation?.results?.find(
          (value) => value.profile === profile,
        );
        rows.push([
          item.file.name,
          item.result?.sha256,
          item.result ? declarationText(item.result.identification) : "Unknown",
          profile,
          result?.status || "unable",
          result?.failedRules,
          item.result?.checkedAt,
          item.result?.engineVersion,
        ]);
      }
    contents = rows.map((row) => row.map(cell).join(",")).join("\r\n");
    mime = "text/csv";
  }
  const url = URL.createObjectURL(new Blob([contents], { type: mime }));
  const link = element("a");
  link.href = url;
  link.download = `paper-machine-validation.${format}`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
$("explore-tab").addEventListener("click", () => selectStudioTool(false));
$("validate-tab").addEventListener("click", () => selectStudioTool(true));
for (const [index, id] of [
  "explore-tab",
  "validate-tab",
  "pipeline-tab",
].entries()) {
  $(id).addEventListener("keydown", (event) => {
    if (["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      const modes = ["explore", "validate", "pipeline"];
      const target =
        event.key === "Home"
          ? 0
          : event.key === "End"
            ? 2
            : (index + (event.key === "ArrowRight" ? 1 : 2)) % 3;
      selectStudioTool(modes[target]);
      $(`${modes[target]}-tab`).focus();
    }
  });
}
$("validation-add").addEventListener("click", () =>
  $("validation-files").click(),
);
$("validation-files").addEventListener("change", () => {
  addValidationFiles($("validation-files").files);
  $("validation-files").value = "";
});
$("validation-run").addEventListener("click", () => runValidationQueue());
$("validation-stop").addEventListener("click", () => {
  validationState.stop = true;
  validationNotice(
    "Stopping after the current file completes. Remaining files will stay queued.",
  );
  drawValidationQueue();
});
$("validation-clear").addEventListener("click", () => {
  validationState.items = [];
  validationState.stop = false;
  validationNotice();
  drawValidationQueue();
});
$("validation-profiles").addEventListener("change", () => {
  for (const item of validationState.items) {
    item.result = null;
    item.status = "queued";
    item.error = "";
    item.profiles = null;
  }
  validationState.stop = false;
  validationNotice(
    selectedProfiles().length
      ? "Profile selection changed. Run the queue to check these profiles."
      : "Select at least one validation profile.",
  );
  drawValidationQueue();
});
$("validation-json").addEventListener("click", () =>
  saveValidationReport("json"),
);
$("validation-csv").addEventListener("click", () =>
  saveValidationReport("csv"),
);
$("validation-examples").addEventListener("click", async () => {
  $("validation-examples").disabled = true;
  try {
    const files = [];
    for (const [kind, name] of [
      ["valid", "synthetic-3b-with-attachment.pdf"],
      ["false-claim", "synthetic-false-claim.pdf"],
      ["ordinary", "synthetic-field-notes.pdf"],
    ]) {
      const response = await fetch(`/api/validation-samples/${kind}`);
      if (!response.ok)
        throw new Error(
          "The synthetic examples could not be loaded. Try again.",
        );
      files.push(
        new File([await response.blob()], name, {
          type: "application/pdf",
          lastModified: 0,
        }),
      );
    }
    addValidationFiles(files, true);
  } catch (error) {
    validationNotice(error.message, true);
  } finally {
    $("validation-examples").disabled = validationState.running;
  }
});
$("validate-current").addEventListener("click", async () => {
  try {
    const response = await fetch(`/api/documents/${state.doc.id}/file`);
    if (!response.ok)
      throw new Error(
        "The document expired. Open the PDF again before validating.",
      );
    const file = new File([await response.blob()], state.doc.name, {
      type: "application/pdf",
    });
    addValidationFiles([file], false, state.doc.sha256);
    selectStudioTool(true);
  } catch (error) {
    notify(error.message, true);
  }
});
selectStudioTool(
  location.hash === "#pipeline" ? "pipeline" : location.hash === "#validate",
);
drawValidationQueue();
async function validateBuiltDocument(doc) {
  const response = await fetch(`/api/documents/${doc.id}/file`);
  if (!response.ok)
    throw new Error("The built PDF expired before validation. Open it again.");
  addValidationFiles(
    [new File([await response.blob()], doc.name, { type: "application/pdf" })],
    false,
    doc.sha256,
  );
  const item = validationState.items.find(
    (item) => item.key === `sha:${doc.sha256}`,
  );
  selectStudioTool(true);
  await runValidationQueue(new Set([item.id]));
}
