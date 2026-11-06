#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROOF_ROOT="$(mktemp -d)"
trap 'rm -rf "$PROOF_ROOT"' EXIT
PROOF_SBT=(sbt --server -J-Xms256m -J-Xmx2g -Dsbt.server.autostart=false)
cd "$REPO_ROOT"

if [[ "${1:-}" == --central ]]; then
  version="${2:?Supply the released version}"
  repository_args=()
else
  version="$("${PROOF_SBT[@]}" -batch -no-colors 'show kyoPdfPipeline / version' |
    sed -nE 's/^\[info\] ([0-9]+\.[0-9]+\.[0-9]+[^ ]*)$/\1/p' | tail -1)"
  [[ -n "$version" ]]
  repository="$PROOF_ROOT/repo"
  "${PROOF_SBT[@]}" -batch ";set ThisBuild / version := \"$version\";set kyoPdf / publishTo := Some(Resolver.file(\"policy-proof\", file(\"$repository\"))(using Resolver.mavenStylePatterns));set kyoPdfZio / publishTo := Some(Resolver.file(\"policy-proof\", file(\"$repository\"))(using Resolver.mavenStylePatterns));set kyoPdfPipeline / publishTo := Some(Resolver.file(\"policy-proof\", file(\"$repository\"))(using Resolver.mavenStylePatterns));set kyoPdfFs2 / publishTo := Some(Resolver.file(\"policy-proof\", file(\"$repository\"))(using Resolver.mavenStylePatterns));kyoPdf/publish;kyoPdfZio/publish;kyoPdfPipeline/publish;kyoPdfFs2/publish"

  for artifact in kyo-pdf_3 kyo-pdf-zio_3 kyo-pdf-pipeline_3 kyo-pdf-fs2_3; do
    base="$repository/io/github/adrielc/$artifact/$version/$artifact-$version"
    for suffix in .pom .jar -sources.jar -javadoc.jar; do test -s "$base$suffix"; done
    if grep -Eq '<repositories>|<pluginRepositories>' "$base.pom"; then
      echo 'Public POM contains a repository override.' >&2
      exit 1
    fi
    for suffix in -tests.jar -tests-sources.jar -tests-javadoc.jar; do
      if [[ -e "$base$suffix" ]]; then
        echo 'Public artifact contains test code or documentation.' >&2
        exit 1
      fi
    done
    jar tf "$base.jar" > "$PROOF_ROOT/$artifact.classes"
    jar tf "$base-sources.jar" > "$PROOF_ROOT/$artifact.sources"
  done
  grep -Fqx 'io/github/adrielc/kyopdf/pipeline/Policy$.class' "$PROOF_ROOT/kyo-pdf-pipeline_3.classes"
  grep -Fqx 'io/github/adrielc/kyopdf/pipeline/Policy.scala' "$PROOF_ROOT/kyo-pdf-pipeline_3.sources"
  grep -Fqx 'io/github/adrielc/kyopdf/zio/PdfZIO$.class' "$PROOF_ROOT/kyo-pdf-zio_3.classes"
  repository_args=("-Dzio.pdf.local.repo=$repository")
fi

cd "$REPO_ROOT/tests/policy-consumer"
"${PROOF_SBT[@]}" -batch "${repository_args[@]}" "-Dzio.pdf.version=$version" run
echo "External consumer resolved and executed the Policy API $version."
