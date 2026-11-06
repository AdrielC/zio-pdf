#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
platform="${1:-jvm}"
case "$platform" in
  jvm|native) ;;
  *) echo 'Use jvm or native' >&2; exit 2 ;;
esac
proof="$(mktemp -d)"
trap 'rm -rf "$proof"' EXIT
export KYO_PDF_LARGE_PACKET_OUTPUT="$proof/packet.pdf"
runner=(sbt --server -J-Xms128m -J-Xmx2g -J-XX:ActiveProcessorCount=4 -Dsbt.server.autostart=false -batch)
if [[ "$platform" == jvm ]]; then
  export KYO_PDF_LARGE_ZIO_OUTPUT="$proof/staged.pdf"
  "${runner[@]}" \
    'set kyoPdf / Test / fork := true' \
    'set kyoPdf / Test / javaOptions ++= Seq("-Xms16m", "-Xmx64m", "-XX:ActiveProcessorCount=4")' \
    'kyoPdf/testOnly *PdfPacketSpec *PdfPacketLargeSpec' \
    'kyoPdfZio/testOnly *PdfPacketZIOSpec'
else
  python3 scripts/verify-native-provenance.py
  "${runner[@]}" 'kyoPdfNative/testOnly *PdfPacketSpec *PdfPacketLargeSpec'
fi
# Requiring the exported file also rejects sbt's successful "No tests to run".
test -s "$proof/packet.pdf"
qpdf --check "$proof/packet.pdf"
test "$(qpdf --show-npages "$proof/packet.pdf")" = 3
if [[ "$platform" == jvm ]]; then
  test -s "$proof/staged.pdf"
  qpdf --check "$proof/staged.pdf"
  test "$(qpdf --show-npages "$proof/staged.pdf")" = 2
fi
python3 - "$proof" "$platform" <<'PY'
from hashlib import sha256
from pathlib import Path
import sys
root, platform = Path(sys.argv[1]), sys.argv[2]
outputs = [('packet.pdf', 192 * 1024 * 1024)]
if platform == 'jvm':
    outputs.append(('staged.pdf', 80 * 1024 * 1024))
for name, minimum in outputs:
    path = root / name
    size = path.stat().st_size
    if size <= minimum:
        raise AssertionError(f'{name} did not exercise the large-file workload')
    digest = sha256()
    with path.open('rb') as stream:
        while block := stream.read(65536):
            digest.update(block)
    print(f'{platform} {name}: {size} bytes, SHA-256 {digest.hexdigest()}')
PY
