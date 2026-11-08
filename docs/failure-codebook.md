# Failure annotation codebook

Two raters label every failed cell in the main grid independently, using the artifact, the error text, and the sink evidence. Disagreements are resolved by discussion, and agreement is reported as Cohen's kappa before resolution.

Each failed cell gets one **root-cause label** (the first row that applies, top to bottom):

| Label | Use when |
|---|---|
| `platform_quirk` | The artifact follows the platform's documented API, and the platform fails for a reason the artifact cannot control (for example a runtime that rejects a valid definition, or an HTTP behaviour that the platform does not document) |
| `harness_fault` | The failure is caused by the harness (recorder, preflight, runner, scorer), confirmed by reproducing it with the reference artifact |
| `api_misuse` | The artifact uses the platform's API incorrectly: wrong node type, wrong task field, wrong sink call, a response that is never read |
| `syntax_or_schema` | The artifact does not parse, or does not match the platform's schema (YAML structure, JSON node shape, DAG import errors) |
| `logic_error` | The artifact runs and delivers records, but the transformation is wrong (wrong field, wrong rule, wrong join key, wrong grouping) |
| `no_delivery` | The artifact runs without an error but delivers no records to the sink, and the cause is not an API misuse |
| `ambiguous` | The raters cannot agree from the evidence; record the reason |

Each failed cell also gets a **secondary note** for the failure category assigned by the harness, and whether the error text is informative enough (`yes`/`no`).

The annotation sheet is produced by `scripts/make_annotation_sheet.py` from the main-grid run files. It lists only failed cells.
