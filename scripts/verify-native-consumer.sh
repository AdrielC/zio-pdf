#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROOF_ROOT="$(mktemp -d)"
PUBLISH_REPO="${PROOF_ROOT}/repo"
trap 'rm -rf "${PROOF_ROOT}"' EXIT
PROOF_SBT=(sbt --server -J-Xms128m -J-Xmx3g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false)
ARTIFACT="kyo-pdf_native0.5_3"

cd "${REPO_ROOT}"
projects=(blocksChunkNative blocksTypeIdNative blocksMaybeNative blocksMarkdownNative blocksSchemaNative kyoPdfNative)
commands=""
for project in "${projects[@]}"; do
  commands+=";set $project / publishTo := Some(Resolver.file(\"native-proof\", file(\"${PUBLISH_REPO}\"))(using Resolver.mavenStylePatterns))"
done
for project in "${projects[@]}"; do commands+=";$project/publish"; done
"${PROOF_SBT[@]}" -batch "$commands"
pom="$(find "${PUBLISH_REPO}" -name "${ARTIFACT}-*.pom" -type f)"
[[ -n "${pom}" && -f "${pom}" ]]
version="$(basename "$(dirname "${pom}")")"
jar tf "${pom%.pom}.jar" > "${PROOF_ROOT}/classes"
jar tf "${pom%.pom}-sources.jar" > "${PROOF_ROOT}/sources"
jar tf "${pom%.pom}-javadoc.jar" > "${PROOF_ROOT}/docs"
grep -Fqx 'io/github/adrielc/kyopdf/PdfAIdentification$.nir' "${PROOF_ROOT}/classes"
grep -Fqx 'io/github/adrielc/kyopdf/XmpReader.scala' "${PROOF_ROOT}/sources"
grep -Fq '<artifactId>kyo-core_native0.5_3</artifactId>' "${pom}"
grep -Fq '<artifactId>kyo-pdf-blocks-schema_native0.5_3</artifactId>' "${pom}"
grep -Fqx 'io/github/adrielc/scan/Scan$.nir' "${PROOF_ROOT}/classes"
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
