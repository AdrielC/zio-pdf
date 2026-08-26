#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

ARTIFACT_ROOT="$(mktemp -d)"
PUBLISH_REPO="${ARTIFACT_ROOT}/repo"
JAR_LIST="$(mktemp)"
JS_JAR_LIST="$(mktemp)"
CLASS_STRINGS="$(mktemp)"
JS_CLASS_STRINGS="$(mktemp)"
trap 'rm -rf "${ARTIFACT_ROOT}"; rm -f "${JAR_LIST}" "${JS_JAR_LIST}" "${CLASS_STRINGS}" "${JS_CLASS_STRINGS}"' EXIT

sbt -batch ";set root / publishTo := Some(Resolver.file(\"artifact-proof\", file(\"${PUBLISH_REPO}\"))(Resolver.mavenStylePatterns));set scalaJs / publishTo := Some(Resolver.file(\"artifact-proof\", file(\"${PUBLISH_REPO}\"))(Resolver.mavenStylePatterns));root/publish;scalaJs/publish"

JAR_PATH="$(find "${ARTIFACT_ROOT}" -type f -name 'zio-pdf_3*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | sort | tail -n 1)"
POM_PATH="$(find "${ARTIFACT_ROOT}" -type f -name 'zio-pdf_3*.pom' | sort | tail -n 1)"
JS_JAR_PATH="$(find "${ARTIFACT_ROOT}" -type f -name 'zio-pdf_sjs1_3*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | sort | tail -n 1)"
JS_POM_PATH="$(find "${ARTIFACT_ROOT}" -type f -name 'zio-pdf_sjs1_3*.pom' | sort | tail -n 1)"

if [[ -z "${JAR_PATH}" || -z "${POM_PATH}" || -z "${JS_JAR_PATH}" || -z "${JS_POM_PATH}" ]]; then
  echo "JVM or Scala.js release artifact was not generated." >&2
  exit 1
fi

jar tf "${JAR_PATH}" > "${JAR_LIST}"
jar tf "${JS_JAR_PATH}" > "${JS_JAR_LIST}"

for entry in \
  'zio/pdf/PdfObjectScanner$.class' \
  'zio/pdf/StreamingDecode$.class' \
  'zio/pdf/PdfEngine$.class'; do
  if ! grep -Fqx "${entry}" "${JAR_LIST}"; then
    echo "Published JAR is missing ${entry}." >&2
    exit 1
  fi
done

for entry in \
  'zio/pdf/ByteLimit$.class' \
  'zio/pdf/PdfObjectScanner$.class' \
  'zio/pdf/PdfSource$.class'; do
  if ! grep -Fqx "${entry}" "${JS_JAR_LIST}"; then
    echo "Scala.js release JAR is missing ${entry}." >&2
    exit 1
  fi
done

unzip -p "${JAR_PATH}" | strings > "${CLASS_STRINGS}"
unzip -p "${JS_JAR_PATH}" | strings > "${JS_CLASS_STRINGS}"

for artifact_listing in "${JAR_LIST}" "${JS_JAR_LIST}"; do
  if grep -Eq '(^|/)(PublishSettings|target|\.git)(/|\.|$)' "${artifact_listing}"; then
    echo "Release JAR contains build or repository internals." >&2
    exit 1
  fi
done

if grep -Eq '^zio/(blocks/pure|scan)/' "${JAR_LIST}"; then
  echo "Release JAR contains benchmark-only experimental namespaces." >&2
  exit 1
fi

for artifact_strings in "${CLASS_STRINGS}" "${JS_CLASS_STRINGS}"; do
  if grep -Eqi 'UnsupportedOperationException|not implemented|placeholder backend' "${artifact_strings}"; then
    echo "Release JAR contains an unsupported-operation or placeholder marker." >&2
    exit 1
  fi
done

for pom in "${POM_PATH}" "${JS_POM_PATH}"; do
  grep -Fq '<groupId>io.github.adrielc</groupId>' "${pom}"
  grep -Fq '<name>Apache-2.0</name>' "${pom}"
  grep -Fq '<id>AdrielC</id>' "${pom}"
  grep -Fq 'https://github.com/AdrielC/zio-pdf' "${pom}"
  if grep -Eqi '<repositories>|<pluginRepositories>' "${pom}"; then
    echo "Release POM contains a repository override." >&2
    exit 1
  fi
done

echo "zio-pdf JVM and Scala.js artifact audit passed."
