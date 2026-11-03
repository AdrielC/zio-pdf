#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROOF_ROOT="$(mktemp -d)"
PUBLISH_REPO="${PROOF_ROOT}/repo"
trap 'rm -rf "${PROOF_ROOT}"' EXIT
PROOF_SBT=(sbt --server -J-Xms256m -J-Xmx2g -Dsbt.server.autostart=false)
ARTIFACT="kyo-pdf_native0.5_3"

cd "${REPO_ROOT}"
"${PROOF_SBT[@]}" -batch ";set kyoPdfNative / publishTo := Some(Resolver.file(\"native-proof\", file(\"${PUBLISH_REPO}\"))(using Resolver.mavenStylePatterns));kyoPdfNative/publish"
pom="$(find "${PUBLISH_REPO}" -name "${ARTIFACT}-*.pom" -type f)"
[[ -n "${pom}" && -f "${pom}" ]]
version="$(basename "$(dirname "${pom}")")"
jar tf "${pom%.pom}.jar" > "${PROOF_ROOT}/classes"
jar tf "${pom%.pom}-sources.jar" > "${PROOF_ROOT}/sources"
jar tf "${pom%.pom}-javadoc.jar" > "${PROOF_ROOT}/docs"
grep -Fqx 'io/github/adrielc/kyopdf/PdfAIdentification$.nir' "${PROOF_ROOT}/classes"
grep -Fqx 'io/github/adrielc/kyopdf/XmpReader.scala' "${PROOF_ROOT}/sources"
grep -Fq '<artifactId>kyo-core_native0.5_3</artifactId>' "${pom}"
unzip -p "${pom%.pom}.jar" | strings > "${PROOF_ROOT}/class-strings"
if grep -Eq 'file:/|/Users/|/home/runner/' "${PROOF_ROOT}/class-strings"; then
  echo "Native artifact contains local source paths." >&2
  exit 1
fi
if [[ -e "${pom%.pom}-tests.jar" || -e "${pom%.pom}-tests-sources.jar" ]]; then
  echo "Native artifact publishes test classes or sources." >&2
  exit 1
fi
if grep -Eq '<artifactId>kyo-(core|parse|schema)_3</artifactId>' "${pom}"; then
  echo "Native artifact contains a JVM Kyo dependency." >&2
  exit 1
fi

cd "${REPO_ROOT}/tests/native-consumer"
"${PROOF_SBT[@]}" -batch -Dzio.pdf.local.repo="${PUBLISH_REPO}" -Dzio.pdf.version="${version}" run
