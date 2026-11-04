#!/usr/bin/env python3
"""Verify the Native runtime's public upstream source and local adaptations."""
import hashlib
import io
import json
from pathlib import Path
import tarfile
import urllib.request

root = Path(__file__).resolve().parents[1] / "modules" / "zio-blocks-native"
manifest = json.loads((root / "UPSTREAM.json").read_text())
assert manifest["repository"] == "https://github.com/zio/zio-blocks"
assert manifest["tag"] == "v0.0.56"
assert manifest["commit"] == "b4523963f2981c15f15d0afa05b33a1ff0cf38a9"
url = "https://codeload.github.com/zio/zio-blocks/tar.gz/" + manifest["commit"]
with urllib.request.urlopen(url, timeout=60) as response:
    archive = response.read(32 * 1024 * 1024 + 1)
assert len(archive) <= 32 * 1024 * 1024, "Upstream archive exceeds the verification limit"
with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as upstream:
    prefix = upstream.getnames()[0].split("/")[0] + "/"
    for entry in manifest["files"]:
        original = upstream.extractfile(prefix + entry["upstream"]).read()
        assert hashlib.sha256(original).hexdigest() == entry["sha256"], entry["upstream"]
        local = (root / entry["path"]).read_bytes()
        assert hashlib.sha256(local).hexdigest() == entry.get("adaptedSha256", entry["sha256"]), entry["path"]
actual = {str(path.relative_to(root)) for module in ("chunk", "typeid", "maybe", "markdown", "schema")
          for platform in ("shared", "native")
          for path in (root / module / platform / "src").rglob("*.scala")}
assert actual == {entry["path"] for entry in manifest["files"]}, "Unrecorded or missing upstream sources"
print("Native source provenance verified against public ZIO Blocks v0.0.56 at " + manifest["commit"])
