# Evidence fixture matrix

`EvidenceCorpusSpec` keeps the small evidence-specific fixture matrix
deterministic and independent of a network download:

| Shape | Source | Assertion |
| --- | --- | --- |
| Native text | Generated minimal PDF | Digest-scoped citation with page and object provenance |
| Visual-only page | Generated minimal PDF | One explicit page-local text-recovery request |
| PDF/A-3b declaration | Generated XMP metadata stream | Structural declaration, not a full PDF/A conformance claim |
| Linearization marker | Generated `/Linearized` dictionary | Structural marker remains visible after decode |
| Image XObject | Vendored `test-image.pdf` | Structural image count on a real producer PDF |
| Malformed input | Deterministic byte string | Decode failure or a failed validation record |

The public court corpus remains the large, provenance-recorded integration
suite. This matrix adds parser shapes, not court-specific behavior.
