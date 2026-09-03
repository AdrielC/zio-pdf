#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INPUT_PDF="${1:-${PDF_PATH:-}}"

if [[ -z "${INPUT_PDF}" || ! -f "${INPUT_PDF}" ]]; then
  echo "usage: PDF_PATH=/path/to/doc.pdf $0 [input.pdf]" >&2
  exit 1
fi

PROOF_ROOT="$(mktemp -d)"
LINEARIZED="${PROOF_ROOT}/linearized.pdf"
trap 'rm -rf "${PROOF_ROOT}"' EXIT

cd "${REPO_ROOT}"
METRICS="$(
  INPUT_PDF="${INPUT_PDF}" OUTPUT_PDF="${LINEARIZED}" \
    sbt -batch 'examples/runMain zio.pdf.examples.LinearizeFromFile' 2>/dev/null \
    | grep -E '^(linearized_bytes|source_bytes|first_page_prefix_bytes|size_ratio|first_page_savings_pct)='
)"

eval "$(printf '%s\n' "${METRICS}" | sed 's/^/export /')"

if [[ -z "${first_page_prefix_bytes:-}" || -z "${linearized_bytes:-}" ]]; then
  echo "failed to read linearization metrics from sbt output" >&2
  exit 1
fi

RANGE_END=$((first_page_prefix_bytes - 1))
FULL_BYTES="${linearized_bytes}"

if command -v curl >/dev/null 2>&1; then
  RANGE_FETCHED="$(
    curl -sS -r "0-${RANGE_END}" "file://${LINEARIZED}" -o /dev/null -w '%{size_download}'
  )"
else
  RANGE_FETCHED="${first_page_prefix_bytes}"
fi

echo "source_bytes=${source_bytes:-unknown}"
echo "linearized_bytes=${FULL_BYTES}"
echo "first_page_prefix_bytes=${first_page_prefix_bytes}"
echo "byte_range_fetch_bytes=${RANGE_FETCHED}"
echo "full_file_fetch_bytes=${FULL_BYTES}"
echo "fetch_reduction_pct=$(awk "BEGIN { printf \"%.1f\", (1 - ${RANGE_FETCHED}/${FULL_BYTES}) * 100 }")"
echo "size_ratio=${size_ratio:-unknown}"

if command -v qpdf >/dev/null 2>&1; then
  qpdf --check "${LINEARIZED}" >/dev/null
  echo "qpdf_check=passed"
fi
