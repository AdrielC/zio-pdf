#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROOF_ROOT="$(mktemp -d)"
OUTPUT_PDF="${PROOF_ROOT}/zio-pdf-writer-proof.pdf"
trap 'rm -rf "${PROOF_ROOT}"' EXIT

for tool in qpdf pdfinfo; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "${tool} is required for independent PDF validation." >&2
    exit 1
  fi
done

cd "${REPO_ROOT}"
OUTPUT_PDF="${OUTPUT_PDF}" sbt -batch 'examples/runMain zio.pdf.examples.GeneratePdf'
qpdf --check "${OUTPUT_PDF}"
pdfinfo "${OUTPUT_PDF}" | grep -Eq '^Pages:[[:space:]]+1$'

echo "Independent qpdf and pdfinfo validation passed."
