"""Live integration checks against the loopback studio; no external PDF fixtures."""

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path

BASE = sys.argv.pop(1).rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8087"


def request(path, data=None, headers=None):
    req = urllib.request.Request(BASE + path, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            return response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers


def synthetic_pdf(objects, root):
    data = b"%PDF-1.7\n"
    offsets = [0]
    for number, value in enumerate(objects, 1):
        offsets.append(len(data))
        data += f"{number} 0 obj\n{value}\nendobj\n".encode("ascii")
    xref = len(data)
    data += f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode("ascii")
    data += b"".join(f"{offset:010d} 00000 n \n".encode("ascii") for offset in offsets[1:])
    data += f"trailer\n<< /Size {len(offsets)} /Root {root} 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode("ascii")
    return data


class StudioTest(unittest.TestCase):
    def setUp(self):
        status, body, _ = request("/api/sample", b"")
        self.assertEqual(status, 200, body)
        self.doc = json.loads(body)
        self.path = "/api/documents/" + self.doc["id"]

    def test_sample_is_a_real_pdf_and_preview(self):
        self.assertEqual(len(self.doc["pages"]), 4)
        self.assertEqual(self.doc["objectCount"], 11)
        status, body, headers = request(self.path + "/file")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get_content_type(), "application/pdf")
        self.assertTrue(body.startswith(b"%PDF-1.7"))
        status, body, headers = request(self.path + "/preview?page=1")
        self.assertEqual(status, 200, body[:200])
        self.assertEqual(headers.get_content_type(), "image/png")
        self.assertTrue(body.startswith(b"\x89PNG\r\n\x1a\n"))

    def test_reordered_output_has_matching_graph_and_page_content(self):
        status, body, _ = request(self.path + "/remix", b"4,1,3")
        self.assertEqual(status, 200, body)
        remix = json.loads(body)
        self.assertEqual(len(remix["pages"]), 3)
        self.assertEqual(remix["objectCount"], 9)
        self.assertIn("/Size 10", remix["trailer"])
        ids = {obj["id"] for obj in remix["objects"]}
        self.assertTrue(all(ref in ids for obj in remix["objects"] for ref in obj["refs"]))
        status, pdf, _ = request("/api/documents/" + remix["id"] + "/file")
        self.assertEqual(status, 200)
        status, body, _ = request("/api/documents", pdf, {"X-File-Name": "round-trip.pdf"})
        self.assertEqual(status, 200, body)
        self.assertEqual(len(json.loads(body)["pages"]), 3)
        if not shutil.which("qpdf") or not shutil.which("pdftotext"):
            self.skipTest("Independent checks require qpdf and pdftotext; API round-trip passed")
        with tempfile.TemporaryDirectory(prefix="paper-machine-test-") as directory:
            path = Path(directory) / "remix.pdf"
            path.write_bytes(pdf)
            checked = subprocess.run(["qpdf", "--check", str(path)], capture_output=True, text=True)
            self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)
            text = subprocess.check_output(["pdftotext", str(path), "-"], text=True)
            pages = text.split("\f")
            for page, title in zip(pages, ["FORM", "ORBIT", "SIGNAL"]):
                self.assertIn(title, page)
            self.assertEqual(len(pages), 4)

    def test_invalid_requests_keep_the_original_usable(self):
        for selection in [b"", b"1,1", b"0", b"5", b"-1", b"a", b"1,", b"1" * 1025]:
            with self.subTest(selection=selection[:20]):
                status, body, _ = request(self.path + "/remix", selection)
                self.assertEqual(status, 400, body)
                self.assertIn("error", json.loads(body))
        self.assertEqual(request(self.path + "/preview?page=5")[0], 400)
        self.assertEqual(request(self.path + "/file")[0], 200)

    def test_upload_bounds_and_origin(self):
        self.assertEqual(request("/api/documents", b"not a PDF")[0], 400)
        status, body, _ = request("/api/documents", b"x" * (12 * 1024 * 1024 + 1))
        self.assertEqual(status, 400, body)
        self.assertIn("12 MiB", json.loads(body)["error"])
        self.assertEqual(request("/api/sample", b"", {"Origin": "https://example.org"})[0], 403)
        self.assertEqual(request("/api/sample", b"", {"Host": "example.org"})[0], 403)

    def test_old_documents_expire(self):
        for _ in range(3):
            self.assertEqual(request("/api/sample", b"")[0], 200)
        self.assertEqual(request(self.path + "/file")[0], 404)

    def test_graph_cap_keeps_high_numbered_pages_inspectable(self):
        pdf = synthetic_pdf(["null"] * 180 + [
            "<< /Type /Catalog /Pages 182 0 R >>",
            "<< /Type /Pages /Count 1 /Kids [183 0 R] >>",
            "<< /Type /Page /Parent 182 0 R /MediaBox [0 0 200 200] /Resources << >> >>",
        ], 181)
        status, body, _ = request("/api/documents", pdf)
        self.assertEqual(status, 200, body)
        doc = json.loads(body)
        self.assertEqual(doc["objectCount"], 183)
        self.assertEqual(len(doc["objects"]), 180)
        page = next(obj for obj in doc["objects"] if obj["id"] == doc["pages"][0]["ref"])
        self.assertEqual(page["kind"], "Page")
        self.assertIn("/Type /Page", page["syntax"])

    def test_rewrite_parent_cannot_collide_with_source_object_two(self):
        content = "BT /F1 12 Tf (Synthetic page) Tj ET"
        pdf = synthetic_pdf([
            "<< /Type /Catalog /Pages 3 0 R >>",
            "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
            "<< /Type /Pages /Count 1 /Kids [4 0 R] >>",
            "<< /Type /Page /Parent 3 0 R /MediaBox [0 0 200 200] /Resources << /Font << /F1 2 0 R >> >> /Contents 5 0 R >>",
            f"<< /Length {len(content)} >>\nstream\n{content}\nendstream",
        ], 1)
        status, body, _ = request("/api/documents", pdf)
        self.assertEqual(status, 200, body)
        doc = json.loads(body)
        status, body, _ = request("/api/documents/" + doc["id"] + "/remix", b"1")
        self.assertEqual(status, 200, body)
        remix = json.loads(body)
        page = next(obj for obj in remix["objects"] if obj["id"] == remix["pages"][0]["ref"])
        self.assertIn("/Parent 2 0 R", page["syntax"])
        if sys.platform == "darwin" and shutil.which("swift"):
            status, written, _ = request("/api/documents/" + remix["id"] + "/file")
            self.assertEqual(status, 200)
            with tempfile.TemporaryDirectory(prefix="paper-machine-pdfkit-") as directory:
                path = Path(directory) / "synthetic-parent-collision.pdf"
                path.write_bytes(written)
                checked = subprocess.run(
                    ["swift", str(Path(__file__).with_name("check_pdfkit.swift")), str(path), "1"],
                    capture_output=True, text=True, timeout=60,
                )
                self.assertEqual(checked.returncode, 0, checked.stdout + checked.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
