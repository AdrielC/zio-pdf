#!/usr/bin/env python3
"""Optional bounded local raster-edit worker. Install PyMuPDF 1.26.7 separately.

Every output is a fresh document built from rendered pages. Original text,
images, forms, attachments, metadata, and incremental revisions are never copied.
This sacrifices vector content, original text searchability, tags, and signatures.
OCR is an explicit subsequent operation. Do not use on untrusted files without
an OS sandbox: limits here are application budgets, not a renderer sandbox.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import sys

import pymupdf as pdf

MAX_BYTES = 12 * 1024 * 1024
MAX_PAGE_PIXELS = 16_000_000
MAX_TOTAL_PIXELS = 128_000_000


class Rejected(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message


def require(test, message, code="InvalidInput"):
    if not test:
        raise Rejected(code, message)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def region(value, doc):
    number = value["page"]
    require(isinstance(number, int) and 1 <= number <= len(doc), "Region page is outside document")
    numbers = [value[k] for k in ("x", "y", "width", "height")]
    require(all(math.isfinite(v) for v in numbers), "Nonfinite region")
    x, y, width, height = numbers
    rect = pdf.Rect(x, y, x + width, y + height)
    page = doc[number - 1]
    require(page.rotation == 0, "Rotated region coordinates require normalization", "Unsupported")
    require(width > 0 and height > 0 and page.rect.contains(rect), "Region is outside page")
    return number, rect


def run(root):
    input_path, output_path = root / "input.pdf", root / "output.pdf"
    require((root / "request.json").stat().st_size <= 65536, "Request is too large", "Limit")
    request = json.loads((root / "request.json").read_text())
    operation, settings = request["operation"], request["settings"]
    require(operation in {"locate", "redact", "replace", "ocr", "cleanup"}, "Unknown operation")
    require(0 < input_path.stat().st_size <= MAX_BYTES, "PDF byte limit exceeded", "Limit")
    input_hash = digest(input_path)
    require(input_hash == request["inputSha256"], "Input hash mismatch", "Verification")
    with pdf.open(input_path) as source:
        require(not source.is_encrypted, "Encrypted input requires an explicit decrypt step", "Unsupported")
        require(not source.is_repaired, "Renderer repaired malformed input", "Unsupported")
        require(1 <= len(source) <= 64, "Page budget exceeded", "Limit")
        boxes = []
        if operation == "locate":
            boxes = settings.get("regions", [])[:]
            for value in boxes:
                region(value, source)
            for term in settings.get("terms", []):
                require(isinstance(term, str) and 0 < len(term) <= 256, "Invalid search term")
                matches = 0
                for page in source:
                    require(page.rotation == 0, "Rotated search coordinates require normalization", "Unsupported")
                    for rect in page.search_for(term):
                        boxes.append(dict(page=page.number + 1, x=rect.x0, y=rect.y0, width=rect.width, height=rect.height))
                        matches += 1
                        require(len(boxes) <= 256, "Selection budget exceeded", "Limit")
                require(matches > 0, "Search term was not found; scanned text needs OCR or explicit regions", "Rejected")
            require(0 < len(boxes) <= 256, "Selection must not be empty")
            return {"selection": {"sha256": input_hash, "regions": boxes}}

        dpi = settings["dpi"]
        require(isinstance(dpi, int) and 72 <= dpi <= 300, "Invalid DPI")
        matrix = pdf.Matrix(dpi / 72, dpi / 72)
        redactions = {}
        if operation == "redact":
            plan = settings["plan"]
            require(plan["sha256"] == input_hash, "Redaction plan hash mismatch", "Verification")
            require(0 < len(plan["regions"]) <= 256, "Invalid region count")
            for value in plan["regions"]:
                number, rect = region(value, source)
                redactions.setdefault(number - 1, []).append(rect)

        replacements = 0
        total_pixels = 0
        with pdf.open() as output:
            for page in source:
                require(page.rect.width <= 14400 and page.rect.height <= 14400, "Page dimensions exceed budget", "Limit")
                pixels = math.ceil(page.rect.width * dpi / 72) * math.ceil(page.rect.height * dpi / 72)
                total_pixels += pixels
                require(pixels <= MAX_PAGE_PIXELS and total_pixels <= MAX_TOTAL_PIXELS, "Raster pixel budget exceeded", "Limit")
                if operation == "replace":
                    require(page.rotation == 0, "Rotated replacement requires normalization", "Unsupported")
                    find, replacement = settings["find"], settings["replacement"]
                    require(0 < len(find) <= 256 and len(replacement) <= 256, "Invalid replacement strings")
                    require(all(ord(c) <= 255 for c in replacement), "Replacement needs a Unicode font backend", "Unsupported")
                    font, size = settings["font"], settings["fontSize"]
                    require(font in {"helv", "cour", "tiro"} and math.isfinite(size) and 4 <= size <= 72, "Invalid replacement font")
                    hits = page.search_for(find)
                    replacements += len(hits)
                    require(replacements <= 256, "Replacement budget exceeded", "Limit")
                    # Measure the selected font before any output can escape.
                    font_metrics = pdf.Font(fontname=font)
                    for rect in hits:
                        require(font_metrics.text_length(replacement, fontsize=size) <= rect.width and
                                (font_metrics.ascender - font_metrics.descender) * size <= rect.height,
                                "Replacement does not fit selected text width", "Rejected")
                        page.add_redact_annot(rect, fill=(1, 1, 1))
                    if hits:
                        page.apply_redactions(images=2, graphics=1, text=0)
                        for rect in hits:
                            point = pdf.Point(rect.x0, rect.y0 + font_metrics.ascender * size)
                            page.insert_text(point, replacement, fontname=font, fontsize=size)
                # Rasterize without annotations/widgets, then explicitly overwrite
                # selected pixels. The output has none of the source object graph.
                pixmap = page.get_pixmap(matrix=matrix, colorspace=pdf.csRGB, alpha=False, annots=False)
                for rect in redactions.get(page.number, []):
                    clip = (rect * matrix).irect
                    clip = pdf.IRect(clip.x0 - 1, clip.y0 - 1, clip.x1 + 1, clip.y1 + 1) & pixmap.irect
                    pixmap.set_rect(clip, (255, 255, 255))
                if operation == "ocr":
                    language = settings["language"]
                    import re
                    require(re.fullmatch(r"[a-z]{3}(\+[a-z]{3}){0,3}", language), "Invalid OCR language")
                    pixmap.set_dpi(dpi, dpi)
                    ocr_bytes = pixmap.pdfocr_tobytes(language=language, tessdata=os.environ.get("TESSDATA_PREFIX"))
                    require(len(ocr_bytes) <= MAX_BYTES, "OCR page exceeds byte limit", "Limit")
                    with pdf.open("pdf", ocr_bytes) as ocr_page:
                        output.insert_pdf(ocr_page)
                else:
                    target = output.new_page(width=page.rect.width, height=page.rect.height)
                    target.insert_image(target.rect, pixmap=pixmap)
            require(operation != "replace" or replacements > 0, "Replacement text was not found", "Rejected")
            output.set_metadata({})
            output.save(output_path, garbage=4, deflate=True, clean=True)

        require(output_path.stat().st_size <= MAX_BYTES, "Output PDF exceeds byte limit", "Limit")
        with pdf.open(output_path) as final:
            require(len(final) == len(source) and not final.is_repaired, "Invalid final PDF", "Verification")
            require(final.embfile_count() == 0, "Unexpected embedded files", "Verification")
            for page in final:
                require(not list(page.annots() or []) and not list(page.widgets() or []), "Unexpected interactive content", "Verification")
                if operation != "ocr":
                    require(not page.get_text().strip(), "Unexpected retained source text", "Verification")
                for rect in redactions.get(page.number, []):
                    check = page.get_pixmap(matrix=matrix, clip=rect, colorspace=pdf.csRGB, alpha=False, annots=False)
                    require(all(value == 255 for value in check.samples), "Redacted pixels failed final render check", "Verification")
            require(all(not final.metadata.get(k) for k in ("title", "author", "subject", "keywords")), "Retained metadata", "Verification")
        return dict(operation=operation, inputSha256=input_hash, outputSha256=digest(output_path), verified=True)


if __name__ == "__main__":
    root = Path(sys.argv[1])
    try:
        result = run(root)
    except Rejected as error:
        result = dict(error=error.code, message=error.message)
    except Exception:
        # Never return source text, paths, or unrestricted renderer diagnostics.
        result = dict(error="Backend", message="Renderer or OCR failed; no artifact was committed")
    (root / "report.json").write_text(json.dumps(result))
    sys.exit(1 if "error" in result else 0)
