---
icon: lucide/shield-check
---

# API Audit Tool

The `api_audit` module compares the data models defined in `nadag_python` against the live NADAG API to detect field mismatches, typos, and schema changes.

This is a **developer-only** tool. It requires the `rich` package for formatted terminal output (included in the `dev` extras). If `rich` is not installed, the tool falls back to plain-text output.

## Installation

The audit tool is part of the package but its extra dependency (`rich`) is only installed with the `dev` group:

```bash
pip install nadag_python[dev]
# or with uv
uv pip install -e ".[dev]"
```

## Usage

Run from the command line:

```bash
python -m nadag_python.api_audit
```

To also save a JSON report:

```bash
python -m nadag_python.api_audit --output report.json
```

## What it does

The tool performs the following steps:

1. **Fetches real fields from the API** — For each collection used by the codebase, it requests a sample of items from the live API and extracts all property keys from the JSON responses.

2. **Extracts expected fields from the models** — It reads the field mappings defined in `MethodDataFrame`, `MethodDataDataFrame`, `SampleDataFrame`, and `ApiSchemaConfig`.

3. **Compares fields bidirectionally**:
    - **Exact match** — field exists in the API with the same name.
    - **Case-insensitive match** — field exists but with different casing (handled by `normalize_columns` at runtime, so flagged as OK).
    - **Fuzzy match** — field not found exactly, but a similar field exists in the API (probable typo or rename). Uses `difflib.SequenceMatcher` with a similarity cutoff of 0.75.
    - **Missing from API** — field referenced in the model but not found anywhere in the API.
    - **New in API** — field present in the API but not referenced by any model (potential new data to capture).

4. **Generates a report** with severity levels:
    - `[CRITICAL]` — Model field not found in API, but a fuzzy match exists (likely a typo or renamed field).
    - `[WARNING]` — Model field not found in API, no fuzzy match (possibly removed or only present in some features).
    - `[OK]` — Field matches (exact or case-insensitive).
    - `[INFO]` — New API fields not captured by the current models.

## Collections audited

The tool checks all collections actively used by the codebase:

| Collection | Category |
|---|---|
| `geotekniskborehull` | Geometry (locations) |
| `geotekniskborehullunders` | Geometry (investigations) |
| `kombinasjonsondering` | Method info (TOT) |
| `trykksondering` | Method info (CPT) |
| `statisksondering` | Method info (RP) |
| `kombinasjonsonderingdata` | Observation data (TOT) |
| `trykksonderingdata` | Observation data (CPT) |
| `statisksonderingdata` | Observation data (RP) |
| `geotekniskproveserie` | Sample series |
| `geotekniskproveseriedel` | Sample series parts |
| `geotekniskproveseriedeldata` | Sample lab data |
| `geotekniskdokument` | Documents |

## Example output

```text
──────────────────── NADAG API Audit Report ────────────────────
  Timestamp : 2026-04-28T12:08:21+00:00
  API       : https://geo.ngu.no/api/features/grunnundersokelser_utvidet/collections

  OK   geotekniskborehull (30 fields)
  OK   geotekniskborehullunders (41 fields)
  OK   kombinasjonsonderingdata (7 fields)
  ...

──────────── CRITICAL — Probable typos / renamed fields ────────

  [CRITICAL] harPrøveseriedel.href  (from ApiSchemaConfig.sample.serie_href)
    Not found in any collection.
    Fuzzy match: "harPrøverseriedel.href" in geotekniskproveserie (0.977)

──────────── WARNING — Fields not found in API ─────────────────
  [WARNING] spyleTrykk  (from MethodDataDataFrame)

──────────── INFO — New API fields not in model ────────────────
┌──────────────────┬──────────────────────────────┐
│ Collection       │ New Fields                   │
├──────────────────┼──────────────────────────────┤
│ trykksondering   │ alpha, resistivitet, ...      │
└──────────────────┴──────────────────────────────┘

──────────────────── Summary ───────────────────────────────────
  3 critical  12 warnings  59 ok  88 new API fields
```

## JSON report structure

When using `--output`, the report is saved as a JSON file with this structure:

```json
{
  "timestamp": "2026-04-28T12:08:21+00:00",
  "api_base_url": "https://geo.ngu.no/...",
  "collections_fetched": [
    {"collection": "geotekniskborehull", "fields": [...], "error": null}
  ],
  "model_field_results": [
    {
      "model_field": "harPrøveseriedel.href",
      "source": "ApiSchemaConfig.sample.serie_href",
      "found_in": [],
      "case_match_in": {},
      "fuzzy_matches": {
        "geotekniskproveserie": [["harPrøverseriedel.href", 0.977]]
      },
      "severity": "critical"
    }
  ],
  "new_api_fields": {
    "trykksondering": ["alpha", "resistivitet", "..."]
  },
  "summary": {"critical": 3, "warning": 12, "ok": 59, "new_api_fields": 88}
}
```

## Notes

- The tool samples 20 items per collection to cover nullable fields that may be absent in a single item.
- Fields flagged as `[WARNING]` may be valid if they only appear in responses with specific data (e.g., sounding observation fields only appear in observation data responses fetched via hrefs, not in the top-level collection items).
- The `metode-*` keys (e.g., `metode-KombinasjonSondering`) appear with indexed notation (`.1.href`, `.1.title`) in the raw API response and are normalized into lists by `PaginatedResponse._normalize_properties`. The audit tool sees the raw format, so these show up as fuzzy matches rather than exact matches.
