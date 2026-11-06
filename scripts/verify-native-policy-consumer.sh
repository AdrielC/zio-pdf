#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROOF_ROOT="$(mktemp -d)"
trap 'rm -rf "$PROOF_ROOT"' EXIT
PROOF_SBT=(sbt --server -J-Xms128m -J-Xmx3g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false)
cd "$REPO_ROOT"

if [[ "${1:-}" == --central ]]; then
  version="${2:?Supply the released version}"
  repository_args=()
else
  version="$("${PROOF_SBT[@]}" -batch -no-colors 'show kyoPdfPipelineNative / version' |
    sed -nE 's/^\[info\] ([0-9]+\.[0-9]+\.[0-9]+[^ ]*)$/\1/p' | tail -1)"
  [[ -n "$version" ]]
  repository="$PROOF_ROOT/repo"
  commands=";set ThisBuild / version := \"$version\""
  projects=(kyoPdfNative blocksChunkNative blocksTypeIdNative blocksMaybeNative blocksMarkdownNative blocksSchemaNative blocksToonNative kyoPdfPipelineNative kyoPdfFs2Native)
  for project in "${projects[@]}"; do
    commands+=";set $project / publishTo := Some(Resolver.file(\"native-policy-proof\", file(\"$repository\"))(using Resolver.mavenStylePatterns))"
  done
  for project in "${projects[@]}"; do commands+=";$project/publish"; done
  "${PROOF_SBT[@]}" -batch "$commands"

  for module in kyo-pdf kyo-pdf-blocks-chunk kyo-pdf-blocks-typeid kyo-pdf-blocks-maybe kyo-pdf-blocks-markdown kyo-pdf-blocks-schema kyo-pdf-blocks-schema-toon kyo-pdf-pipeline kyo-pdf-fs2; do
    artifact="${module}_native0.5_3"
    base="$repository/io/github/adrielc/$artifact/$version/$artifact-$version"
    for suffix in .pom .jar -sources.jar -javadoc.jar; do test -s "$base$suffix"; done
    if grep -Eq '<repositories>|<pluginRepositories>|<artifactId>kyo-pdf(_3|-(core|pipeline)_3)</artifactId>' "$base.pom"; then
      echo 'Native POM contains a repository override or JVM dependency.' >&2
      exit 1
    fi
    for suffix in -tests.jar -tests-sources.jar -tests-javadoc.jar; do test ! -e "$base$suffix"; done
    jar tf "$base.jar" > "$PROOF_ROOT/$module.classes"
    jar tf "$base-sources.jar" > "$PROOF_ROOT/$module.sources"
    jar tf "$base-javadoc.jar" > "$PROOF_ROOT/$module.docs"
    grep -Fqx 'index.html' "$PROOF_ROOT/$module.docs"
  done
  grep -Fqx 'io/github/adrielc/kyopdf/pipeline/Policy$.nir' "$PROOF_ROOT/kyo-pdf-pipeline.classes"
  grep -Fqx 'io/github/adrielc/kyopdf/pipeline/Policy.scala' "$PROOF_ROOT/kyo-pdf-pipeline.sources"
  grep -Fqx 'zio/blocks/schema/SchemaExpr$.nir' "$PROOF_ROOT/kyo-pdf-blocks-schema.classes"
  grep -Fqx 'META-INF/zio-blocks-native/UPSTREAM.json' "$PROOF_ROOT/kyo-pdf-blocks-schema.classes"
  repository_args=("-Dzio.pdf.local.repo=$repository")
fi

cd "$REPO_ROOT/tests/native-policy-consumer"
"${PROOF_SBT[@]}" -batch "${repository_args[@]}" "-Dzio.pdf.version=$version" "run $PROOF_ROOT/native-policy.pdf"
qpdf --check "$PROOF_ROOT/native-policy.pdf"
pdfinfo "$PROOF_ROOT/native-policy.pdf" | grep -Eq '^Pages:[[:space:]]+1$'
echo "External Native consumer resolved and executed Policy pipelines $version."
