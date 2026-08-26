# Public court PDF corpus

These are immutable copies of publicly released, federal-court documents. They
are test fixtures, not legal advice and not a source of case records. The suite
does not download them during a test run: provenance and SHA-256 digests below
make each checked-in byte sequence auditable.

The corpus deliberately spans the documents that show up in a legal workflow:
slip opinions, a Supreme Court order list, appellate opinions from two producer
stacks, an ECF-stamped district-court order with an AcroForm, and a large
district-court general order. It contains no sealed filings or documents sourced
from PACER.

| Fixture | Court / document kind | Original public source | PDF facts | SHA-256 |
| --- | --- | --- | --- | --- |
| `scotus-atlantic-richfield-slip-opinion.pdf` | Supreme Court, 2020 slip opinion | https://www.supremecourt.gov/opinions/19pdf/17-1498_8mjp.pdf | PDF 1.6, 46 pages, tagged | `00d21eed19fa61629ef0ff0cc3837cf529c0f26fb3501be8b4afca3d4aa7b3b7` |
| `scotus-order-list-2025-05-19.pdf` | Supreme Court, order list | https://www.supremecourt.gov/orders/courtorders/051925zor_0pm1.pdf | PDF 1.6, 8 pages, tagged | `2fd20eee0560d55307e3e96343dd737affd4c3b64ffaf526654ad2ad48c1633b` |
| `ca4-bayramov-v-american-credit-acceptance.pdf` | Fourth Circuit, published opinion | https://www.ca4.uscourts.gov/opinions/251490.P.pdf | PDF 1.6, 25 pages, tagged, Adobe PDF Library | `2a3238423df474c05a4ad0f4c978548545e9cbe0fac201c62689fce7a6cc4def` |
| `cafc-janich-v-collins.pdf` | Federal Circuit, precedential opinion | https://www.cafc.uscourts.gov/opinions-orders/24-1944.OPINION.3-5-2026_2656739.pdf | PDF 1.6, 11 pages, tagged, iText 7 | `66b0ebaad01de04bc5a7c331e4e851d6e300a29af197745a1002a4fbca794c24` |
| `govinfo-district-court-order.pdf` | Middle District of Pennsylvania, ECF-stamped order | https://www.govinfo.gov/content/pkg/USCOURTS-pamd-4_06-cv-01556/pdf/USCOURTS-pamd-4_06-cv-01556-8.pdf | PDF 1.5, 11 pages, AcroForm, Distiller + iText | `1a9d7090501e370779069a85bad351b7c933b5f22a105373e63abb4bef071090` |
| `oknd-general-order-2024-09.pdf` | Northern District of Oklahoma, general order | https://oknd.uscourts.gov/sites/default/files/2024go09.pdf | PDF 1.6, 92 pages, iText 7 | `f14ebb8d301dd5f5e4e0d589bb172bc36e6861a17bc7ebf1f1bbbeafbe04e3a8` |

`CourtCorpusSpec` verifies byte length, SHA-256, PDF version, the exact
data/content-object baselines after expansion, parsed annotation/form dictionary
markers when visible to the top-level parser, fused-versus-streaming parity, and
a no-collection stream/sink pass for the largest fixture.

`PublicPdfCorpusBench` measures the same fused and stream APIs over every
fixture without changing the historical `RealPdfBench` matrix. Run it with
`sbt "bench/Jmh/run -i 10 -wi 5 .*PublicPdfCorpusBench.*"`.

## Fixture admission and updates

This is a PDF parser corpus, not a court-specific compatibility layer. Court
documents are useful because public institutions publish a durable variety of
producer stacks, layouts, metadata, forms, and file sizes. A future fixture may
come from any stable, publicly redistributable source when it covers a parsing
shape not already represented here.

When adding or replacing a fixture, keep the corpus deterministic:

1. Record the canonical public source, document kind, producer-relevant facts,
   byte length, and SHA-256 in this manifest.
2. Vendor the exact original bytes. Tests must never retrieve a document over
   the network.
3. Add exact structural expectations to `CourtCorpusSpec`, including
   fused-versus-streaming parity under rechunking.
4. Explain the added coverage in the pull request. Do not merely update an
   expectation to make a parser regression pass.

Review the source and hashes independently before changing a fixture. Removing
or replacing an existing fixture requires the same provenance record and a
reason that the coverage remains represented elsewhere in the corpus.
