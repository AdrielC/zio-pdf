#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
: "${KYO_PDF_GOTENBERG_PORT:?Set KYO_PDF_GOTENBERG_PORT to the local Gotenberg port}"
command -v qpdf >/dev/null
command -v pdftotext >/dev/null

curl --fail --silent --show-error --retry 25 --retry-all-errors --retry-delay 2 --max-time 3 \
  "http://127.0.0.1:$KYO_PDF_GOTENBERG_PORT/health" >/dev/null

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

sbt --server -J-Xms128m -J-Xmx2g -Dsbt.server.autostart=false -batch 'paperMachine/run 0' >"$studio_log" 2>&1 &
studio_pid=$!
studio_url=
ready=false
for ((attempt = 0; attempt < 300; attempt++)); do
  studio_url="$(sed -n 's/^PAPER_MACHINE_URL=\(http:\/\/127\.0\.0\.1:[0-9][0-9]*\)$/\1/p' "$studio_log" | tail -1)"
  if [[ -n "$studio_url" ]] && curl --fail --silent --max-time 2 "$studio_url/" >/dev/null; then
    ready=true
    break
  fi
  if ! kill -0 "$studio_pid" 2>/dev/null; then
    echo "Paper Machine exited before it was ready" >&2
    exit 1
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then
  echo "Paper Machine did not start within five minutes" >&2
  exit 1
fi

KYO_PDF_TEST_GOTENBERG=1 python3 examples/paper-machine/test_pipelines.py "$studio_url" \
  PipelineTest.test_gotenberg_archive_packet_passes_3b_through_tool
