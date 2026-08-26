#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

sbt -batch root/packageBin root/makePom

JAR_PATH="$(find target -type f -name 'zio-pdf_3-*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | sort | tail -n 1)"
POM_PATH="$(find target -type f -name 'zio-pdf_3-*.pom' | sort | tail -n 1)"

if [[ -z "${JAR_PATH}" || -z "${POM_PATH}" ]]; then
  echo "Published JAR or POM was not generated." >&2
  exit 1
fi

for entry in \
  'zio/pdf/PdfObjectScanner$.class' \
  'zio/pdf/StreamingDecode$.class' \
  'zio/pdf/PdfEngine$.class'; do
  if ! jar tf "${JAR_PATH}" | grep -Fqx "${entry}"; then
    echo "Published JAR is missing ${entry}." >&2
    exit 1
  fi
done

if jar tf "${JAR_PATH}" | grep -Eq '(^|/)(PublishSettings|target|\.git)(/|\.|$)'; then
  echo "Published JAR contains build or repository internals." >&2
  exit 1
fi

if unzip -p "${JAR_PATH}" | strings | grep -Eqi 'UnsupportedOperationException|not implemented|placeholder backend'; then
  echo "Published JAR contains an unsupported-operation or placeholder marker." >&2
  exit 1
fi

grep -Fq '<groupId>io.github.adrielc</groupId>' "${POM_PATH}"
grep -Fq '<name>Apache-2.0</name>' "${POM_PATH}"
grep -Fq '<id>AdrielC</id>' "${POM_PATH}"
grep -Fq 'https://github.com/AdrielC/zio-pdf' "${POM_PATH}"

if grep -Eqi '<repositories>|<pluginRepositories>' "${POM_PATH}"; then
  echo "Published POM contains a repository override." >&2
  exit 1
fi

echo "zio-pdf artifact audit passed: ${JAR_PATH}"
