#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONSUMER_ROOT="${REPO_ROOT}/tests/consumer"

cd "${REPO_ROOT}"
VERSION="$({ sbt -batch -no-colors 'show root / version'; } | sed -n 's/^\[info\] //p' | tail -n 1)"
if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.+-][0-9A-Za-z.-]+)*$ ]]; then
  echo "Could not determine a valid zio-pdf version: ${VERSION:-<empty>}" >&2
  exit 1
fi

sbt -batch root/publishLocal

cd "${CONSUMER_ROOT}"
sbt -batch -Dzio.pdf.version="${VERSION}" run
echo "External consumer resolved and executed zio-pdf ${VERSION}."
