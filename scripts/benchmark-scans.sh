#!/usr/bin/env bash
set -euo pipefail
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
cd "$repo"
case "${1:-jvm}" in
  jvm) project=kyoPdfFs2; allocation=AllocationJvm.scala ;;
  native) project=kyoPdfFs2Native; allocation=AllocationNative.scala ;;
  *) echo 'Usage: benchmark-scans.sh [jvm|native]' >&2; exit 1 ;;
esac
cp tests/scan-benchmark/ScanBenchmark.scala "$scratch/"
sed 's/package io.github.adrielc.kyopdf/package io.github.adrielc.scan/' "tests/parser-benchmark/$allocation" > "$scratch/Allocation.scala"
sbt --server -J-Xms128m -J-Xmx3g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false -batch \
  ";set $project / Test / unmanagedSources ++= (file(\"$scratch\") ** \"*.scala\").get();$project/testOnly *ScanBenchmark"
