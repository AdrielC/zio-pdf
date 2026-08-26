#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONSUMER_ROOT="${REPO_ROOT}/tests/consumer"
PROOF_ROOT="$(mktemp -d)"
PUBLISH_REPO="${PROOF_ROOT}/repo"
trap 'rm -rf "${PROOF_ROOT}"' EXIT

cd "${REPO_ROOT}"
VERSION=""
VERSION_OUTPUT=""
for attempt in 1 2 3; do
  if VERSION_OUTPUT="$(sbt -batch -no-colors 'show root / version' 2>&1)"; then
    candidate="$(
      printf '%s\n' "${VERSION_OUTPUT}" |
        grep -Eo '[0-9]+\.[0-9]+\.[0-9]+([.+-][0-9A-Za-z.-]+)*' |
        tail -n 1 || true
    )"
    if [[ "${candidate}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.+-][0-9A-Za-z.-]+)*$ ]]; then
      VERSION="${candidate}"
      break
    fi
  fi

  echo "Version lookup attempt ${attempt}/3 did not produce a usable version." >&2
  printf '%s\n' "${VERSION_OUTPUT}" >&2
  if [[ "${attempt}" -lt 3 ]]; then
    sleep "$((attempt * 2))"
  fi
done
if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.+-][0-9A-Za-z.-]+)*$ ]]; then
  echo "Could not determine a valid zio-pdf version: ${VERSION:-<empty>}" >&2
  exit 1
fi

sbt -batch ";set root / publishTo := Some(Resolver.file(\"consumer-proof\", file(\"${PUBLISH_REPO}\"))(Resolver.mavenStylePatterns));root/publish"

cd "${CONSUMER_ROOT}"
sbt -batch -Dzio.pdf.local.repo="${PUBLISH_REPO}" -Dzio.pdf.version="${VERSION}" run
echo "External consumer resolved and executed zio-pdf ${VERSION}."
