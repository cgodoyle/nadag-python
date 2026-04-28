"""
NADAG API Audit Tool

Compares the data models defined in nadag_python against the live API schema
to detect field mismatches, typos, and new/removed fields.

Usage:
    python -m nadag_python.api_audit [--output report.json]

Requires 'rich' for formatted terminal output (optional, falls back to plain print).
Install with: pip install nadag_python[dev]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher, get_close_matches

import httpx

from .config import Settings
from .data_models import FIELD, ApiSchemaConfig, MethodDataDataFrame, MethodDataFrame, SampleDataFrame

# ---------------------------------------------------------------------------
# Lazy import of rich — graceful fallback to plain print
# ---------------------------------------------------------------------------
try:
    from rich.console import Console
    from rich.table import Table

    _console = Console()
    _HAS_RICH = True
except ImportError:
    _console = None  # type: ignore[assignment]
    _HAS_RICH = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_settings = Settings()
API_BASE_URL = _settings.API_BASE_URL

# Collections that the codebase actively uses
USED_COLLECTIONS: list[str] = [
    # Geometry / spatial
    "geotekniskborehull",
    "geotekniskborehullunders",
    # Method info
    "kombinasjonsondering",
    "trykksondering",
    "statisksondering",
    # Method observation data
    "kombinasjonsonderingdata",
    "trykksonderingdata",
    "statisksonderingdata",
    # Samples
    "geotekniskproveserie",
    "geotekniskproveseriedel",
    "geotekniskproveseriedeldata",
    # Documents
    "geotekniskdokument",
]

FUZZY_CUTOFF = 0.75
FUZZY_MAX_RESULTS = 3


# ---------------------------------------------------------------------------
# Data classes for the report
# ---------------------------------------------------------------------------


@dataclass
class FieldMatch:
    """A single field comparison result."""

    model_field: str
    source: str  # which model enum / config it comes from
    found_in: list[str] = field(default_factory=list)  # collections where exact match found
    case_match_in: dict[str, str] = field(default_factory=dict)  # collection -> api_field (case-insensitive)
    fuzzy_matches: dict[str, list[tuple[str, float]]] = field(
        default_factory=dict
    )  # collection -> [(api_field, score)]
    severity: str = "ok"  # ok, warning, critical


@dataclass
class CollectionFields:
    """Fields fetched from one API collection."""

    collection: str
    fields: set[str] = field(default_factory=set)
    error: str | None = None


@dataclass
class AuditReport:
    timestamp: str
    api_base_url: str
    collections_fetched: list[CollectionFields]
    model_field_results: list[FieldMatch]
    new_api_fields: dict[str, list[str]]  # collection -> fields not referenced by any model
    summary: dict[str, int] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 1. Fetch real fields from the API
# ---------------------------------------------------------------------------


async def fetch_collection_fields(
    client: httpx.AsyncClient, collection: str, sample_size: int = 20
) -> CollectionFields:
    """Fetch several items from a collection and extract the union of all property keys.

    A single item may omit nullable fields, so we sample multiple items to get
    better coverage of the full schema.
    """
    url = f"{API_BASE_URL}/{collection}/items"
    params = {"limit": sample_size, "f": "json", "crs": "http://www.opengis.net/def/crs/EPSG/0/25833"}
    try:
        resp = await client.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            return CollectionFields(collection=collection, error="No features returned")
        all_keys: set[str] = set()
        for feat in features:
            all_keys.update(feat.get("properties", {}).keys())
        return CollectionFields(collection=collection, fields=all_keys)
    except Exception as exc:
        return CollectionFields(collection=collection, error=str(exc))


async def fetch_all_collections() -> list[CollectionFields]:
    """Fetch fields for all used collections concurrently."""
    async with httpx.AsyncClient() as client:
        tasks = [fetch_collection_fields(client, col) for col in USED_COLLECTIONS]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------------------------
# 2. Extract model fields
# ---------------------------------------------------------------------------


def _enum_fields(enum_cls, source_name: str) -> list[tuple[str, str]]:
    """Extract (value, source_name) from a ModelEnum class."""
    return [(member.value, source_name) for member in enum_cls]


def _extra_mapper_keys(source_name: str) -> list[tuple[str, str]]:
    """Extract the extra keys from MethodDataDataFrame.column_mapper (method id foreign keys)."""
    extras = ["kombinasjonsondering", "trykksondering", "statisksondering"]
    return [(k, source_name) for k in extras]


def get_model_fields() -> dict[str, str]:
    """
    Collect all field names referenced by the data models.
    Returns dict: field_name -> source description.
    """
    fields: dict[str, str] = {}

    # MethodDataFrame — fields from geotekniskborehull + geotekniskborehullunders
    for value, source in _enum_fields(MethodDataFrame, "MethodDataFrame"):
        fields[value] = source

    # MethodDataDataFrame — fields from sounding data collections
    for value, source in _enum_fields(MethodDataDataFrame, "MethodDataDataFrame"):
        fields[value] = source

    # Extra column mapper keys (foreign key fields in sounding data)
    for value, source in _extra_mapper_keys("MethodDataDataFrame.column_mapper"):
        fields[value] = source

    # SampleDataFrame — fields from sample collections
    for value, source in _enum_fields(SampleDataFrame, "SampleDataFrame"):
        fields[value] = source

    # ApiSchemaConfig — navigation hrefs, id fields, metode keys
    schema_fields = {
        FIELD.id_field: "ApiSchemaConfig.id_field",
        FIELD.gbu_ref: "ApiSchemaConfig.gbu_ref",
        FIELD.gbu_id: "ApiSchemaConfig.gbu_id",
        FIELD.feature_id: "ApiSchemaConfig.feature_id",
    }
    fields.update(schema_fields)

    # Method-specific fields from MethodFields
    for method in FIELD.methods:
        fields[method.metode_key] = f"ApiSchemaConfig.{method.name}.metode_key"
        fields[method.observasjon] = f"ApiSchemaConfig.{method.name}.observasjon"
        fields[method.id_ref] = f"ApiSchemaConfig.{method.name}.id_ref"
        fields[method.parent_ref] = f"ApiSchemaConfig.{method.name}.parent_ref"

    # Sample-specific fields from SampleFields
    sample = FIELD.sample
    fields[sample.metode_key] = "ApiSchemaConfig.sample.metode_key"
    fields[sample.serie_href] = "ApiSchemaConfig.sample.serie_href"
    fields[sample.data_href] = "ApiSchemaConfig.sample.data_href"

    # Filter out purely internal/computed fields that are not API property names
    internal_fields = {
        "geometry",
        "method_id",
        "method_type",
        "data",
        "cpt_info",
        "depth",
        "layer_composition",
        "gbhu_id",
        "hammering",
        "increased_rotation_rate",
        "flushing",
    }
    return {k: v for k, v in fields.items() if k not in internal_fields}


# ---------------------------------------------------------------------------
# 3. Comparison engine
# ---------------------------------------------------------------------------


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def compare_all(
    api_collections: list[CollectionFields],
    model_fields: dict[str, str],
) -> AuditReport:
    """Run the full comparison: model fields vs API, and API vs model."""

    # Build a lookup: all api fields across all collections
    all_api_fields: dict[str, set[str]] = {}  # field -> set of collections
    for col in api_collections:
        for f in col.fields:
            all_api_fields.setdefault(f, set()).add(col.collection)

    # Also build lowercase lookup for case-insensitive matching
    api_lower_map: dict[str, list[tuple[str, str]]] = {}  # lowercase -> [(original, collection)]
    for col in api_collections:
        for f in col.fields:
            api_lower_map.setdefault(f.lower(), []).append((f, col.collection))

    # All api field names for fuzzy matching
    all_api_field_names = list(all_api_fields.keys())

    results: list[FieldMatch] = []
    referenced_api_fields: set[str] = set()

    for model_field, source in model_fields.items():
        match = FieldMatch(model_field=model_field, source=source)

        # Exact match
        if model_field in all_api_fields:
            match.found_in = list(all_api_fields[model_field])
            match.severity = "ok"
            referenced_api_fields.add(model_field)
        else:
            # Case-insensitive match
            lower_key = model_field.lower()
            if lower_key in api_lower_map:
                for api_name, col in api_lower_map[lower_key]:
                    match.case_match_in[col] = api_name
                    referenced_api_fields.add(api_name)
                match.severity = "ok"  # normalize_columns handles this
            else:
                # Fuzzy match
                close = get_close_matches(model_field, all_api_field_names, n=FUZZY_MAX_RESULTS, cutoff=FUZZY_CUTOFF)
                if close:
                    for candidate in close:
                        score = _similarity(model_field, candidate)
                        for col in all_api_fields[candidate]:
                            match.fuzzy_matches.setdefault(col, []).append((candidate, round(score, 3)))
                    match.severity = "critical"
                else:
                    match.severity = "warning"

        results.append(match)

    # New API fields not referenced by any model field
    new_api: dict[str, list[str]] = {}
    for col in api_collections:
        new_fields = col.fields - referenced_api_fields
        # Also exclude fields matched case-insensitively
        referenced_lower = {f.lower() for f in referenced_api_fields}
        new_fields = {f for f in new_fields if f.lower() not in referenced_lower}
        if new_fields:
            new_api[col.collection] = sorted(new_fields)

    # Summary counts
    counts = {"critical": 0, "warning": 0, "ok": 0}
    for r in results:
        counts[r.severity] = counts.get(r.severity, 0) + 1
    counts["new_api_fields"] = sum(len(v) for v in new_api.values())

    return AuditReport(
        timestamp=datetime.now(timezone.utc).isoformat(),
        api_base_url=API_BASE_URL,
        collections_fetched=api_collections,
        model_field_results=results,
        new_api_fields=new_api,
        summary=counts,
    )


# ---------------------------------------------------------------------------
# 4. Report output
# ---------------------------------------------------------------------------


def _print_rich(report: AuditReport) -> None:
    """Pretty-print with rich tables and colors."""
    console = _console
    assert console is not None

    console.print()
    console.rule("[bold]NADAG API Audit Report[/bold]")
    console.print(f"  Timestamp : {report.timestamp}")
    console.print(f"  API       : {report.api_base_url}")
    console.print()

    # Collection fetch status
    for col in report.collections_fetched:
        if col.error:
            console.print(f"  [red]FAIL[/red] {col.collection}: {col.error}")
        else:
            console.print(f"  [green]OK[/green]   {col.collection} ({len(col.fields)} fields)")
    console.print()

    # Critical issues
    criticals = [r for r in report.model_field_results if r.severity == "critical"]
    if criticals:
        console.rule("[bold red]CRITICAL — Probable typos / renamed fields[/bold red]")
        for r in criticals:
            console.print(f"\n  [bold red]\\[CRITICAL][/bold red] [bold]{r.model_field}[/bold]  (from {r.source})")
            console.print("    Not found in any collection.")
            for col, matches in r.fuzzy_matches.items():
                for api_field, score in matches:
                    console.print(f'    [yellow]Fuzzy match:[/yellow] "{api_field}" in {col} (similarity: {score})')
        console.print()

    # Warnings
    warnings = [r for r in report.model_field_results if r.severity == "warning"]
    if warnings:
        console.rule("[bold yellow]WARNING — Fields not found in API[/bold yellow]")
        for r in warnings:
            console.print(f"  [yellow]\\[WARNING][/yellow] {r.model_field}  (from {r.source})")
        console.print()

    # New API fields
    if report.new_api_fields:
        console.rule("[bold cyan]INFO — New API fields not in model[/bold cyan]")
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Collection")
        table.add_column("New Fields")
        for col, fields in sorted(report.new_api_fields.items()):
            table.add_row(col, ", ".join(fields))
        console.print(table)
        console.print()

    # Summary
    console.rule("[bold]Summary[/bold]")
    s = report.summary
    console.print(
        f"  [red]{s.get('critical', 0)} critical[/red]  "
        f"[yellow]{s.get('warning', 0)} warnings[/yellow]  "
        f"[green]{s.get('ok', 0)} ok[/green]  "
        f"[cyan]{s.get('new_api_fields', 0)} new API fields[/cyan]"
    )
    console.print()


def _print_plain(report: AuditReport) -> None:
    """Fallback plain-text output."""
    print()
    print("=" * 60)
    print("  NADAG API Audit Report")
    print("=" * 60)
    print(f"  Timestamp : {report.timestamp}")
    print(f"  API       : {report.api_base_url}")
    print()

    for col in report.collections_fetched:
        if col.error:
            print(f"  FAIL  {col.collection}: {col.error}")
        else:
            print(f"  OK    {col.collection} ({len(col.fields)} fields)")
    print()

    criticals = [r for r in report.model_field_results if r.severity == "critical"]
    if criticals:
        print("-" * 60)
        print("  CRITICAL — Probable typos / renamed fields")
        print("-" * 60)
        for r in criticals:
            print(f"\n  [CRITICAL] {r.model_field}  (from {r.source})")
            print("    Not found in any collection.")
            for col, matches in r.fuzzy_matches.items():
                for api_field, score in matches:
                    print(f'    Fuzzy match: "{api_field}" in {col} (similarity: {score})')
        print()

    warnings = [r for r in report.model_field_results if r.severity == "warning"]
    if warnings:
        print("-" * 60)
        print("  WARNING — Fields not found in API")
        print("-" * 60)
        for r in warnings:
            print(f"  [WARNING] {r.model_field}  (from {r.source})")
        print()

    if report.new_api_fields:
        print("-" * 60)
        print("  INFO — New API fields not in model")
        print("-" * 60)
        for col, fields in sorted(report.new_api_fields.items()):
            print(f"  {col}: {', '.join(fields)}")
        print()

    s = report.summary
    print("-" * 60)
    print(
        f"  Summary: {s.get('critical', 0)} critical, "
        f"{s.get('warning', 0)} warnings, "
        f"{s.get('ok', 0)} ok, "
        f"{s.get('new_api_fields', 0)} new API fields"
    )
    print()


def print_report(report: AuditReport) -> None:
    if _HAS_RICH:
        _print_rich(report)
    else:
        _print_plain(report)


def _make_serializable(obj: object) -> object:
    """Convert dataclass / set types to JSON-serializable structures."""
    if isinstance(obj, set):
        return sorted(obj)
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_serializable(i) for i in obj]
    if hasattr(obj, "__dataclass_fields__"):
        return {k: _make_serializable(v) for k, v in asdict(obj).items()}
    return obj


def save_report(report: AuditReport, path: str) -> None:
    data = _make_serializable(report)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)
    label = f"Report saved to {path}"
    if _HAS_RICH and _console:
        _console.print(f"[green]{label}[/green]")
    else:
        print(label)


# ---------------------------------------------------------------------------
# 5. Main orchestrator
# ---------------------------------------------------------------------------


async def run_audit(output_path: str | None = None) -> AuditReport:
    """Run the full audit pipeline."""
    api_collections = await fetch_all_collections()
    model_fields = get_model_fields()
    report = compare_all(api_collections, model_fields)
    print_report(report)
    if output_path:
        save_report(report, output_path)
    return report


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit NADAG API fields against local data models.")
    parser.add_argument("--output", "-o", type=str, default=None, help="Path to save JSON report.")
    args = parser.parse_args()
    asyncio.run(run_audit(output_path=args.output))


if __name__ == "__main__":
    main()
