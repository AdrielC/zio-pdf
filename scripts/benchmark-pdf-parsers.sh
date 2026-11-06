#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
cd "$repo"
# The comparison uses only public v0.6.0 sources, never another remote or checkout.
revision=88d0ebb131e91a2978da790e0aea7404388b0420
python3 - "$revision" "$scratch" <<'PY'
from pathlib import Path
import subprocess, sys
revision, scratch = sys.argv[1:]
for name in ("PdfParser", "PdfDocumentParser", "PdfContentParser"):
    path = f"kyo-pdf-core/src/main/scala/io/github/adrielc/kyopdf/{name}.scala"
    source = subprocess.check_output(["git", "show", f"{revision}:{path}"], text=True)
    source = source.replace("package io.github.adrielc.kyopdf", "package io.github.adrielc.kyopdf.baseline\nimport io.github.adrielc.kyopdf.*", 1)
    source = source.replace("PdfParser.scan(", "io.github.adrielc.kyopdf.baseline.PdfParser.scan(").replace("PdfParser.validate(", "io.github.adrielc.kyopdf.baseline.PdfParser.validate(")
    Path(scratch, f"{name}.scala").write_text(source)
source = subprocess.check_output(["git", "show", f"{revision}:kyo-pdf-core/src/main/scala/io/github/adrielc/kyopdf/PdfDocument.scala"], text=True)
source = source[source.index("final case class PdfInput private"):source.index("object PdfDocument:")]
source = "package io.github.adrielc.kyopdf.baseline\nimport io.github.adrielc.kyopdf.*\nimport kyo.*\n" + source
source = source.replace("PdfDocumentParser.decode(", "io.github.adrielc.kyopdf.baseline.PdfDocumentParser.decode(")
Path(scratch, "PdfInput.scala").write_text(source)
PY
platform="${1:-jvm}"
case "$platform" in
  jvm) project=kyoPdf; allocation=AllocationJvm.scala ;;
  native) project=kyoPdfNative; allocation=AllocationNative.scala ;;
  *) echo 'Usage: benchmark-pdf-parsers.sh [jvm|native]' >&2; exit 1 ;;
esac
cp tests/parser-benchmark/ParserBenchmark.scala "$scratch/"
cp "tests/parser-benchmark/$allocation" "$scratch/Allocation.scala"
sbt --server -J-Xms128m -J-Xmx3g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false -batch \
  ";set $project / Test / unmanagedSources ++= (file(\"$scratch\") ** \"*.scala\").get();$project/testOnly *ParserBenchmark"
