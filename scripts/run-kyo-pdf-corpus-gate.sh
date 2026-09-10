#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 /path/to/pdf-corpus" >&2
  exit 64
fi

if ! command -v qpdf >/dev/null 2>&1; then
  echo "qpdf is required for rewritten-output validation" >&2
  exit 69
fi

corpus_dir=$1
if [[ ! -d $corpus_dir ]]; then
  echo "not a directory: $corpus_dir" >&2
  exit 66
fi

corpus_dir=$(cd "$corpus_dir" && pwd -P)
KYO_PDF_CORPUS_DIR=$corpus_dir sbt -batch 'kyoPdf/testOnly com.tybera.kyopdf.ExternalCorpusSpec'
