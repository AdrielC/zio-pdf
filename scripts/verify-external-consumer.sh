#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONSUMER_ROOT="${REPO_ROOT}/tests/consumer"
PROOF_ROOT="$(mktemp -d)"
PUBLISH_REPO="${PROOF_ROOT}/repo"
trap 'rm -rf "${PROOF_ROOT}"' EXIT

cd "${REPO_ROOT}"
VERSION="$(
  sbt -batch -no-colors 'show root / version' |
    grep -Eo '[0-9]+\.[0-9]+\.[0-9]+([.+-][0-9A-Za-z.-]+)*' |
    tail -n 1
)"
if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.+-][0-9A-Za-z.-]+)*$ ]]; then
  echo "Could not determine a valid zio-pdf version: ${VERSION:-<empty>}" >&2
  exit 1
fi

sbt -batch ";set root / publishTo := Some(Resolver.file(\"consumer-proof\", file(\"${PUBLISH_REPO}\"))(Resolver.mavenStylePatterns));root/publish"

cd "${CONSUMER_ROOT}"
sbt -batch -Dzio.pdf.local.repo="${PUBLISH_REPO}" -Dzio.pdf.version="${VERSION}" run
echo "External consumer resolved and executed zio-pdf ${VERSION}."
