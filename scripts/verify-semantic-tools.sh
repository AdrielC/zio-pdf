#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
platform="${1:-jvm}"
case "$platform" in
  jvm) project=kyoPdfPipeline ;;
  native) project=kyoPdfPipelineNative ;;
  *) echo 'Use jvm or native' >&2; exit 2 ;;
esac
python="${KYO_PDF_SEMANTIC_PYTHON:?Set KYO_PDF_SEMANTIC_PYTHON to a Python with scripts/semantic-requirements.txt installed}"
"$python" scripts/test-semantic-worker.py
export KYO_PDF_SEMANTIC_FIXTURES="$PWD/target/semantic-fixtures"
sbt --server -J-Xms128m -J-Xmx3g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false -batch "$project/testFull"
"$python" scripts/test-semantic-worker.py check-graph

if [[ "$platform" == jvm ]]; then
  studio_log="$(mktemp)"
  studio_pid=
  cleanup() {
    local status=$?
    if [[ -n "$studio_pid" ]]; then
      kill "$studio_pid" 2>/dev/null || true
      wait "$studio_pid" 2>/dev/null || true
    fi
    if (( status != 0 )); then tail -100 "$studio_log"; fi
    rm -- "$studio_log"
  }
  trap cleanup EXIT
  sbt --server -J-Xms128m -J-Xmx2g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false -batch 'paperMachine/run 0' >"$studio_log" 2>&1 &
  studio_pid=$!
  studio_url=
  for ((attempt = 0; attempt < 180; attempt++)); do
    studio_url="$(sed -n 's/^PAPER_MACHINE_URL=\(http:\/\/127\.0\.0\.1:[0-9][0-9]*\)$/\1/p' "$studio_log" | tail -1)"
    if [[ -n "$studio_url" ]] && curl --fail --silent --max-time 2 "$studio_url/" >/dev/null; then break; fi
    if ! kill -0 "$studio_pid" 2>/dev/null; then echo 'Studio exited before readiness' >&2; exit 1; fi
    sleep 1
  done
  if [[ -z "$studio_url" ]]; then echo 'Studio did not become ready' >&2; exit 1; fi
  "$python" examples/paper-machine/test_pipelines.py "$studio_url" \
    PipelineTest.test_registered_redaction_and_ocr_through_actual_tool \
    PipelineTest.test_mcp_tool_returns_pdf_and_named_branch_decision \
    PipelineTest.test_strict_json_and_structural_limits
fi
