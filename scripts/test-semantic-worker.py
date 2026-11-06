#!/usr/bin/env python3
"""Synthetic fixtures and independent checks; no external documents required."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

import pymupdf as pdf
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parent.parent
DEST = ROOT / "target" / "semantic-fixtures"
DEST.mkdir(parents=True, exist_ok=True)
SOURCE = DEST / "source.pdf"


def fixture():
    with pdf.open() as document:
        page = document.new_page(width=300, height=200)
        page.insert_text((20, 40), "SECRET42", fontsize=18)
        page.insert_text((20, 110), "KEEP VISIBLE", fontsize=18)
        with pdf.open() as form:
            fp = form.new_page(width=200, height=40)
            fp.insert_text((2, 20), "SECRET42", fontsize=12)
            page.show_pdf_page(pdf.Rect(120, 18, 235, 60), form, 0)
        with pdf.open() as image_document:
            image_page = image_document.new_page(width=220, height=47)
            image_page.insert_text((2, 25), "SECRET42", fontsize=18)
            page.insert_image(pdf.Rect(18, 18, 238, 65), pixmap=image_page.get_pixmap(alpha=False))
        widget = pdf.Widget()
        widget.field_name = "hidden-field"
        widget.field_type = pdf.PDF_WIDGET_TYPE_TEXT
        widget.field_value = "SECRET42"
        widget.rect = pdf.Rect(20, 25, 100, 50)
        page.add_widget(widget)
        page.add_text_annot((25, 25), "SECRET42 annotation")
        document.set_metadata(dict(title="SECRET42", author="Synthetic fixture"))
        document.embfile_add("secret.txt", b"SECRET42 attachment")
        unused = document.get_new_xref()
        document.update_object(unused, "<< /Unused (SECRET42) >>")
        document.save(SOURCE)
    with pdf.open() as document:
        page = document.new_page(width=300, height=200)
        page.insert_text((20, 40), "SECRET42", fontsize=18)
        page.insert_text((20, 110), "KEEP VISIBLE", fontsize=18)
        document.save(DEST / "replace-source.pdf")


def invoke(operation, settings, source=SOURCE):
    with tempfile.TemporaryDirectory(prefix="semantic-test-") as temporary:
        workspace = Path(temporary)
        shutil.copyfile(source, workspace / "input.pdf")
        request = dict(operation=operation, settings=settings,
                       inputSha256=hashlib.sha256(source.read_bytes()).hexdigest())
        (workspace / "request.json").write_text(json.dumps(request))
        process = subprocess.run([sys.executable, str(ROOT / "scripts/semantic-worker.py"), str(workspace)],
                                 timeout=130, capture_output=True)
        report = json.loads((workspace / "report.json").read_text())
        output = (workspace / "output.pdf").read_bytes() if (workspace / "output.pdf").exists() else None
        assert process.returncode == (1 if "error" in report else 0)
        return report, output


def check(path, secret_absent=True, searchable=False):
    subprocess.run(["qpdf", "--check", str(path)], check=True, capture_output=True, timeout=20)
    reader = PdfReader(path)
    extracted = "".join(page.extract_text() or "" for page in reader.pages)
    # A second parser/extractor, independent of the renderer's text extractor.
    text = subprocess.run(["pdftotext", str(path), "-"], check=True, capture_output=True, timeout=20).stdout.decode()
    if secret_absent:
        assert "SECRET42" not in extracted and "SECRET42" not in text
        assert "SECRET42" not in str(reader.metadata)
        assert not reader.attachments
        for number in range(1, reader.trailer["/Size"]):
            from pypdf.generic import IndirectObject
            obj = reader.get_object(IndirectObject(number, 0, reader))
            if hasattr(obj, "get_data"):
                assert b"SECRET42" not in obj.get_data()
            assert "SECRET42" not in str(obj)
    if searchable:
        assert "KEEP VISIBLE" in text
    return text


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "check-graph":
        check(DEST / "graph-output.pdf", searchable=True)
        print("Independent qpdf, Poppler, and pypdf graph-output checks passed")
        sys.exit(0)
    fixture()
    sha = hashlib.sha256(SOURCE.read_bytes()).hexdigest()
    region = dict(page=1, x=18, y=18, width=220, height=47)
    settings = dict(dpi=144, plan=dict(sha256=sha, regions=[region], reason="Synthetic regression"))
    report, output = invoke("redact", settings)
    assert report.get("verified"), report
    redacted = DEST / "redacted.pdf"
    redacted.write_bytes(output)
    check(redacted)
    with pdf.open(redacted) as document:
        pixmap = document[0].get_pixmap(clip=pdf.Rect(18, 18, 238, 65), alpha=False)
        assert all(value == 255 for value in pixmap.samples)
    report, output = invoke("ocr", dict(dpi=144, language="eng"), redacted)
    assert report.get("verified"), report
    searchable = DEST / "searchable.pdf"
    searchable.write_bytes(output)
    check(searchable, searchable=True)
    report, output = invoke("replace", dict(dpi=144, find="SECRET42", replacement="PUBLIC", font="helv", fontSize=11), DEST / "replace-source.pdf")
    assert report.get("verified"), report
    replaced = DEST / "replaced.pdf"
    replaced.write_bytes(output)
    check(replaced)
    report, output = invoke("ocr", dict(dpi=144, language="eng"), replaced)
    assert report.get("verified"), report
    (DEST / "replaced-searchable.pdf").write_bytes(output)
    assert "PUBLIC" in check(DEST / "replaced-searchable.pdf", searchable=True)
    report, _ = invoke("replace", dict(dpi=144, find="SECRET42", replacement="This text is much too long to fit", font="helv", fontSize=72))
    assert report.get("error") == "Rejected", report
    report, _ = invoke("redact", dict(dpi=144, plan=dict(sha256="wrong", regions=[region], reason="test")))
    assert report.get("error") == "Verification", report
    report, _ = invoke("redact", dict(dpi=144, plan=dict(sha256=sha, regions=[dict(region, page=2)], reason="test")))
    assert report.get("error") == "InvalidInput", report
    report, output = invoke("cleanup", dict(dpi=144))
    assert report.get("verified"), report
    (DEST / "cleaned.pdf").write_bytes(output)
    check(DEST / "cleaned.pdf")
    print("Synthetic redaction, final pixels, cleanup, font fitting, replacement, OCR, stale-plan, and independent-parser checks passed")
