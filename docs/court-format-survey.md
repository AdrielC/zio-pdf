# Court formats and document preparation workflows

Checked 2026-11-06 against public court sources. This is a representative US
survey, not an exhaustive inventory of every court, judge, case type, or standing
order. The workflow model should store jurisdiction, document role, submission
channel, source URL, rule version/effective date when known, and last-checked
date. Format acceptance is a rule for a particular submission, not an intrinsic
property of all court documents. Refresh the applicable source before filing.

## Editable orders: RTF and Word are real requirements

Utah's attorney FAQ, updated April 2026, requires searchable PDF for documents
bearing the filer's signature and RTF for drafts such as orders awaiting the
court's signature. It also warns against producing RTF by scanning and saving
under a different extension. Useful steps are document-role routing, native
word-processing export, checking genuine file syntax, a rendered review copy,
and preserving the editable original. A PDF-to-RTF rename is never a conversion.
[Utah State Courts attorney FAQ](https://www.utcourts.gov/content/dam/howto/filing/efiling/district/docs/eFiling_FAQ-Attorneys.pdf).

Northern District of Florida Bankruptcy's proposed-order instructions accept
Word DOCX or RTF through the proposed-order submission workflow. Its guidelines
exclude PDF for the order itself while allowing PDF exhibits. Produce a bundle
with distinct order and exhibit roles rather than forcing everything into a
single PDF. [Proposed-order instructions](https://www.flnb.uscourts.gov/faqs/submitting-proposed-order),
[preparation and service guidelines](https://www.flnb.uscourts.gov/sites/flnb/files/forms/styleguide.pdf).

The District of New Mexico Bankruptcy manual gives a contrasting example:
when its specified email procedure applies, proposed orders must be MS Word;
PDF, RTF, and DAT are rejected. Routing must include the submission channel and
the circumstances under which that channel applies. This source is not a claim
about every proposed-order procedure in that court.
[Formatting orders for email submission](https://nmb.uscourts.gov/online-manual/formatting-orders-submit-e-mail).

California rule 3.1312(c) describes both a PDF proposed order attached to EFS-020
and an editable word-processing version, with local requirements and exemptions
to account for. This suggests generating sibling artifacts from an editable
source, each carrying its own role, hash, required checks, and delivery target.
[California rule 3.1312](https://courts.ca.gov/cms/rules/index/three/rule3_1312).

## Searchable PDF, exhibits, images, and media

California rule 2.256 requires text searchability when technologically feasible
without impairing the image. Useful steps are detecting image-only pages,
explicitly routing unknown text evidence, OCR, orientation checks, and verifying
searchability in the final bytes after redaction or assembly. PDF/A alone does
not prove searchability, accessibility, correct bookmarks, or privacy.
[California rule 2.256](https://courts.ca.gov/cms/rules/index/two/rule2_256).

The District of Oregon's electronic trial-exhibit page lists PDF documents;
JPG/GIF/PNG/PDF images; and AVI/MPG/MP3/MP4 recordings. It specifies a 500 MB limit,
splitting larger exhibits, correct orientation, and fast-web-view PDFs. Useful
steps include MIME/signature identification, format-specific inspection,
bounded compression, page-aware splitting, and linearization of the final PDF.
This source concerns trial exhibits, not a universal filing policy.
[Electronic trial-exhibit requirements](https://www.ord.uscourts.gov/index.php/filing-and-forms/1740-submission-requirements-for-electronic-trial-exhibits).

Clackamas County Circuit Court's remote-exhibit instructions describe a
text-searchable unified PDF, a 25 MB limit, an exhibit index linked to each
exhibit, bookmarks, and an alternative SFTP process for material that cannot be
converted or exceeds the stated size. Helpful operations are a linked index,
stable exhibit IDs, bookmark/link verification after splitting and assembly, and
a route decision for large or non-PDF material.
[Clackamas exhibit instructions](https://www.courts.oregon.gov/courts/clackamas/go/Pages/Exhibits-Instructions.aspx).

Northern District of Illinois's digital-media portal accepts audio/video such as
MP4, MOV, MP3, and WAV and directs PDF/text exhibits to CM/ECF instead. Vermont's
digital-evidence portal similarly distinguishes media/image evidence from
documents such as DOC/DOCX/PDF/RTF/TXT/XLS/XLSX/WPD, which use other filing
channels. Preserve original evidence and select the correct channel; do not
automatically convert every file to PDF or submit a filing as a side effect of
format preparation. [Illinois digital-media submissions](https://www.ilnd.uscourts.gov/LandingPage.php?page=digital_media_submissions),
[Vermont digital-evidence portal](https://www.vtcourts.gov/about-vermont-judiciary/vermont-digital-evidence-portal).

## What to stream, spool, and verify

Generic streaming steps can identify signatures/MIME, count bytes, hash originals,
reject size overflow, copy unchanged evidence, and apply bounded metadata checks.
A native RTF writer can emit escaped text/control words incrementally from a
structured editable document. TXT and CSV exports can likewise stream from
structured source data. These are extension targets, not converters implemented
by the registered PDF operations in this change.

DOCX/ODT and other office inputs need an appropriate document engine and usually
seekable ZIP/package input. PDF-to-editable conversion needs layout recovery;
scans additionally need OCR and review. Image re-encoding, media transcoding,
OCR, font-aware PDF edits, archival conversion, and linearization should declare
their renderer, memory, seekability, scratch-space, and timeout requirements.
The pipeline can stream their input/output transports while the engine spools.
This does not make the underlying conversion forward-only.

Keep originals immutable. Produce derivatives as new artifacts with content
hashes and provenance. Run required privacy, format, size, searchability,
navigation, and PDF/A checks on the exact finished artifact. A failed required
check withholds the result; missing evidence creates a review outcome. Delivery
and service are separate authorized workflows with their own receipts.

The current registered API supplies the generic operation/schema boundary,
predicates, filters, gates, planner staging, and PDF editing capabilities needed
for these extensions. It does not yet supply RTF/DOCX writers, office imports,
media transcoders, court-specific submission adapters, or a global court-rules
database. Add those as versioned host-registered steps with typed artifact
references rather than adding legal concepts to the core graph.
