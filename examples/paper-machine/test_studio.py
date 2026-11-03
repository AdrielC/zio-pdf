"""Live integration checks against the loopback studio; no external PDF fixtures."""

import json
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
import urllib.parse
from pathlib import Path

BASE = sys.argv.pop(1).rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8087"


def request(path, data=None, headers=None):
    req = urllib.request.Request(BASE + path, data=data, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=65) as response:
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


class ValidationTest(unittest.TestCase):
    def sample(self, kind):
        status, body, _ = request("/api/validation-samples/" + kind)
        self.assertEqual(status, 200, body)
        return body

    def validate(self, pdf, profiles="3b"):
        status, body, _ = request("/api/validate?profiles=" + profiles, pdf)
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def test_explicit_profiles_share_a_file_and_produce_independent_verdicts(self):
        pdf = self.sample("valid")
        report = self.validate(pdf, "3b,2b,3u")
        self.assertEqual(report["sha256"], hashlib.sha256(pdf).hexdigest())
        self.assertEqual(report["engine"], "veraPDF")
        self.assertEqual(report["engineVersion"], "1.30.3")
        self.assertTrue(report["identification"]["declares3b"])
        results = {value["profile"]: value for value in report["validation"]["results"]}
        self.assertEqual(set(results), {"3b", "2b", "3u"})
        self.assertEqual(results["3b"]["status"], "passed")
        self.assertEqual(results["2b"]["status"], "failed")
        self.assertEqual(results["3u"]["status"], "failed")
        self.assertGreater(results["3b"]["checks"], 0)
        self.assertEqual(results["3b"]["failedRules"], 0)

    def test_false_pdfa_claim_fails_with_rule_evidence(self):
        report = self.validate(self.sample("false-claim"))
        self.assertTrue(report["identification"]["declares3b"])
        check = report["validation"]["results"][0]
        self.assertEqual(check["status"], "failed")
        self.assertGreater(check["failedRules"], 0)
        self.assertTrue(any(item["clause"] and item["message"] for item in check["failures"]))
        self.assertLessEqual(len(check["failures"]), 24)

    def test_multiple_documents_have_distinct_receipts_and_do_not_stop_after_failure(self):
        reports = [self.validate(self.sample(kind)) for kind in ["false-claim", "ordinary", "valid"]]
        self.assertEqual([r["validation"]["results"][0]["status"] for r in reports], ["failed", "failed", "passed"])
        self.assertEqual(len({r["sha256"] for r in reports}), 3)
        self.assertEqual(reports[1]["identification"]["state"], "NotDeclared")

    def test_rewrite_requires_a_fresh_fingerprint_and_validation(self):
        pdf = self.sample("valid")
        before = self.validate(pdf)
        status, body, _ = request("/api/documents", pdf)
        self.assertEqual(status, 200, body)
        doc = json.loads(body)
        self.assertTrue(doc["identification"]["declares3b"])
        status, body, _ = request("/api/documents/" + doc["id"] + "/remix", b"1")
        self.assertEqual(status, 200, body)
        rewritten = json.loads(body)
        self.assertNotEqual(rewritten["sha256"], before["sha256"])
        status, output, _ = request("/api/documents/" + rewritten["id"] + "/file")
        self.assertEqual(status, 200)
        after = self.validate(output)
        self.assertEqual(after["sha256"], rewritten["sha256"])
        self.assertEqual(after["validation"]["results"][0]["status"], "passed")

    def test_malformed_input_is_unable_to_check(self):
        report = self.validate(b"This is not a PDF")
        self.assertEqual(report["identification"]["state"], "Unreadable")
        self.assertIn("error", report["validation"])
        self.assertNotIn("results", report["validation"])

    def test_profile_bounds_input_bounds_and_origin(self):
        for query in ["", "profiles=", "profiles=3b,3b", "profiles=unknown", "profiles=1b"]:
            self.assertEqual(request("/api/validate?" + query, b"pdf")[0], 400)
        self.assertEqual(request("/api/validate?profiles=3b", b"")[0], 400)
        self.assertEqual(request("/api/validate?profiles=3b", b"x" * (12 * 1024 * 1024 + 1))[0], 400)
        self.assertEqual(request("/api/validate?profiles=3b", b"x", {"Origin": "https://example.org"})[0], 403)

    def test_temporary_validation_files_are_removed(self):
        root = Path(tempfile.gettempdir())
        before = set(root.glob("paper-machine-validation-*"))
        self.validate(self.sample("valid"))
        self.assertEqual(set(root.glob("paper-machine-validation-*")) - before, set())

    @unittest.skipUnless(os.environ.get("PAPER_MACHINE_SLOW_TESTS") == "1", "Set PAPER_MACHINE_SLOW_TESTS=1 for the 45-second process timeout test")
    def test_busy_timeout_and_cleanup(self):
        import concurrent.futures
        import signal
        import time
        pdf = self.sample("valid")
        root = Path(tempfile.gettempdir())
        before = set(root.glob("paper-machine-validation-*"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.validate, pdf)
            pid = None
            for _ in range(100):
                lines = subprocess.check_output(["ps", "-axo", "pid,args"], text=True).splitlines()
                line = next((line for line in lines if "io.github.adrielc.kyopdf.demo.ValidationWorker " in line and "paper-machine-validation-" in line), None)
                if line:
                    pid = int(line.split()[0])
                    os.kill(pid, signal.SIGSTOP)
                    break
                time.sleep(0.05)
            self.assertIsNotNone(pid, "No validation worker started")
            self.assertEqual(request("/api/validate?profiles=3b", pdf)[0], 429)
            report = pending.result(timeout=55)
            self.assertIn("45 seconds", report["validation"]["error"])
        self.assertEqual(set(root.glob("paper-machine-validation-*")) - before, set())


class PacketTest(unittest.TestCase):
    def setUp(self):
        status, body, _ = request("/api/sample", b"")
        self.assertEqual(status, 200)
        self.doc = json.loads(body)

    def build(self, options, doc=None):
        doc = doc or self.doc
        return request(f"/api/documents/{doc['id']}/compose", urllib.parse.urlencode(options).encode())

    def bookend(self, slot, pdf, name):
        status, body, _ = request("/api/bookends/" + slot, pdf, {"X-File-Name": name})
        self.assertEqual(status, 200, body)
        return json.loads(body)

    def output_text(self, report):
        status, pdf, _ = request(f"/api/documents/{report['id']}/file")
        self.assertEqual(status, 200)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "packet.pdf"
            path.write_bytes(pdf)
            if shutil.which("qpdf"):
                subprocess.run(["qpdf", "--check", str(path)], check=True, capture_output=True)
            if not shutil.which("pdftotext"):
                self.skipTest("Install Poppler to verify searchable stamp and number text")
            return subprocess.check_output(["pdftotext", "-layout", str(path), "-"], text=True).split("\f")[:-1]

    def test_bookends_stamps_and_bates_numbers_share_final_page_order(self):
        sample = request("/api/validation-samples/valid")[1]
        head = self.bookend("head", sample, "Front.pdf")
        tail = self.bookend("tail", sample, "Back.pdf")
        status, raw, _ = self.build({"pages": "4,1", "head": head["id"], "tail": tail["id"], "stamp": "REVIEW (copy) \\ approved", "numbered": "true", "start": "7", "style": "bates", "prefix": "DOC-"})
        self.assertEqual(status, 200, raw)
        report = json.loads(raw)
        self.assertEqual(len(report["pages"]), 4)
        pages = self.output_text(report)
        self.assertEqual(len(pages), 4)
        self.assertIn("FORM", pages[1])
        self.assertIn("ORBIT", pages[2])
        for index, text in enumerate(pages):
            self.assertIn("REVIEW (copy)", text)
            self.assertIn("DOC-" + str(index + 7).zfill(6), text)
        self.assertNotIn("Prev", report["trailer"])
        self.assertNotEqual(report["sha256"], self.doc["sha256"])

    def test_embedded_font_and_profiled_stamp_preserve_valid_3b_specimen(self):
        sample = request("/api/validation-samples/valid")[1]
        status, raw, _ = request("/api/documents", sample)
        self.assertEqual(status, 200, raw)
        status, raw, _ = self.build({"pages": "1", "stamp": "DRAFT", "numbered": "true"}, json.loads(raw))
        self.assertEqual(status, 200, raw)
        report = json.loads(raw)
        self.assertIn("DRAFT", self.output_text(report)[0])
        pdf = request(f"/api/documents/{report['id']}/file")[1]
        status, raw, _ = request("/api/validate?profiles=3b", pdf)
        self.assertEqual(status, 200, raw)
        receipt = json.loads(raw)
        self.assertEqual(receipt["validation"]["results"][0]["status"], "passed", receipt)
        self.assertEqual(receipt["sha256"], report["sha256"])

    def test_crop_rotation_unicode_and_numbering_survive_preview_and_write(self):
        pdf = synthetic_pdf([
            "<< /Type /Catalog /Pages 2 0 R >>",
            "<< /Type /Pages /Count 1 /Kids [3 0 R] /MediaBox [20 30 320 430] /Rotate 90 >>",
            "<< /Type /Page /Parent 2 0 R /CropBox [-20 -30 500 600] /Resources << >> >>",
        ], 1)
        status, raw, _ = request("/api/documents", pdf)
        self.assertEqual(status, 200, raw)
        status, raw, _ = self.build({"pages": "1", "stamp": "Révisé", "placement": "center", "numbered": "true", "start": "9", "style": "number"}, json.loads(raw))
        self.assertEqual(status, 200, raw)
        report = json.loads(raw)
        text = self.output_text(report)[0]
        self.assertIn("Révisé", text)
        self.assertIn("9", text)
        self.assertEqual(request(f"/api/documents/{report['id']}/preview?page=1")[0], 200)

    def test_packet_option_bounds_and_stale_bookends_fail_without_losing_source(self):
        sample = request("/api/validation-samples/valid")[1]
        stale = self.bookend("head", sample, "First.pdf")
        self.bookend("head", sample, "Replacement.pdf")
        invalid = [
            {"pages": "1", "stamp": "x" * 81}, {"pages": "1", "prefix": "x" * 25},
            {"pages": "1", "start": "0"}, {"pages": "1", "start": "1000000"},
            {"pages": "1", "placement": "outside"}, {"pages": "1", "style": "unknown"},
            {"pages": "1,1"}, {"pages": "1", "head": stale["id"]},
            {"pages": "1", "stamp": "line\nbreak"}, {"pages": "1", "numbered": "yes"},
        ]
        for options in invalid:
            self.assertEqual(self.build(options)[0], 400, options)
        self.assertEqual(request(f"/api/documents/{self.doc['id']}/file")[0], 200)
        self.assertEqual(request(f"/api/documents/{self.doc['id']}/compose", b"pages=1&pages=2")[0], 400)
        self.assertEqual(request(f"/api/documents/{self.doc['id']}/compose", b"x" * 4097)[0], 400)
        self.assertEqual(request(f"/api/documents/{self.doc['id']}/compose", b"pages=1", {"Origin": "https://example.com"})[0], 403)


if __name__ == "__main__":
    unittest.main(verbosity=2)
