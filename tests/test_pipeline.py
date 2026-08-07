"""Integration tests using real API fixtures and malformed data.

These tests verify that:
1. PaginatedResponse correctly parses real API responses
2. process_api_responses handles real and malformed data
3. The safe access helpers integrate correctly with real fixture shapes
4. Graceful degradation works end-to-end with malformed inputs
"""

import json
from pathlib import Path

import geopandas as gpd
import httpx
import pandas as pd
import pytest
from shapely.geometry import Point

import nadag_python.nadag_functions as nf
from nadag_python.data_models import FIELD, MethodDataFrame, NadagData, PaginatedResponse, SampleDataFrame
from nadag_python.http_client import NadagHTTPClient
from nadag_python.postprocessing import get_samples_dataframe
from nadag_python.utils import (
    safe_extract_feature_list,
    safe_extract_features,
    safe_extract_properties,
    safe_first,
)

FIXTURES_REAL = Path(__file__).parent / "fixtures" / "real"
FIXTURES_MALFORMED = Path(__file__).parent / "fixtures" / "malformed"


def load_fixture(name: str, malformed: bool = False) -> dict:
    base = FIXTURES_MALFORMED if malformed else FIXTURES_REAL
    with open(base / name) as f:
        return json.load(f)


def _timeout_tuple(timeout: httpx.Timeout) -> tuple[int | float | None, ...]:
    return timeout.connect, timeout.read, timeout.write, timeout.pool


class TestHTTPClientOptions:
    async def test_default_calls_use_settings_timeout_defaults(self):
        from nadag_python.config import settings

        client = NadagHTTPClient()
        http_client = client._get_client()
        try:
            assert _timeout_tuple(http_client.timeout) == (
                settings.API_TIMEOUT,
                settings.API_TIMEOUT,
                settings.API_WRITE_TIMEOUT,
                settings.API_POOL_TIMEOUT,
            )
        finally:
            await http_client.aclose()

    async def test_per_call_timeout_controls_httpx_timeout(self):
        client = NadagHTTPClient(
            timeout_seconds=20,
            connect_timeout_seconds=3,
            read_timeout_seconds=4,
            write_timeout_seconds=5,
            pool_timeout_seconds=6,
        )
        http_client = client._get_client()
        try:
            assert _timeout_tuple(http_client.timeout) == (3, 4, 5, 6)
        finally:
            await http_client.aclose()

    async def test_retry_attempts_controls_transient_failure_attempts(self):
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ConnectError("temporary failure", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = NadagHTTPClient(retry_attempts=2, retry_min_wait=0, retry_max_wait=0)
            with pytest.raises(httpx.ConnectError):
                await client._fetch_page(http_client, "https://example.test/items")

        assert attempts == 2

    async def test_http_503_is_retried(self):
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(200, json={"ok": True}, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = NadagHTTPClient(retry_attempts=2, retry_min_wait=0, retry_max_wait=0)
            data = await client._fetch_page(http_client, "https://example.test/items")

        assert data == {"ok": True}
        assert attempts == 2

    async def test_http_404_is_not_retried(self):
        attempts = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(404, request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http_client:
            client = NadagHTTPClient(retry_attempts=3, retry_min_wait=0, retry_max_wait=0)
            with pytest.raises(httpx.HTTPStatusError):
                await client._fetch_page(http_client, "https://example.test/items")

        assert attempts == 1

    async def test_fetch_from_bounds_forwards_request_options(self, monkeypatch):
        captured_kwargs = {}

        class StopAfterInit(Exception):
            pass

        class FakeNadagHTTPClient:
            def __init__(self, **kwargs):
                captured_kwargs.update(kwargs)

            async def __aenter__(self):
                raise StopAfterInit

            async def __aexit__(self, *exc):
                pass

        monkeypatch.setattr(nf, "NadagHTTPClient", FakeNadagHTTPClient)

        with pytest.raises(StopAfterInit):
            await nf.fetch_from_bounds([0, 0, 1, 1], timeout_seconds=20, retry_attempts=2)

        assert captured_kwargs["timeout_seconds"] == 20
        assert captured_kwargs["retry_attempts"] == 2

    async def test_fetch_from_location_ids_forwards_request_options(self, monkeypatch):
        captured_kwargs = {}

        class StopAfterInit(Exception):
            pass

        class FakeNadagHTTPClient:
            def __init__(self, **kwargs):
                captured_kwargs.update(kwargs)

            async def __aenter__(self):
                raise StopAfterInit

            async def __aexit__(self, *exc):
                pass

        monkeypatch.setattr(nf, "NadagHTTPClient", FakeNadagHTTPClient)

        with pytest.raises(StopAfterInit):
            await nf.fetch_from_location_ids(["location-1"], timeout_seconds=20, retry_attempts=2)

        assert captured_kwargs["timeout_seconds"] == 20
        assert captured_kwargs["retry_attempts"] == 2


class TestGetFeaturesInBbox:
    async def test_deduplicates_identical_features_from_overlapping_sub_bboxes(self, monkeypatch):
        shared_feature = {
            "type": "Feature",
            "id": "shared-id",
            "properties": {"dokumentID": "shared-id"},
        }
        responses = iter(
            [
                PaginatedResponse(
                    features=[
                        {"type": "Feature", "id": "first-id", "properties": {}},
                        shared_feature.copy(),
                    ],
                    numberReturned=2,
                ),
                PaginatedResponse(
                    features=[
                        shared_feature.copy(),
                        {"type": "Feature", "id": "last-id", "properties": {}},
                    ],
                    numberReturned=2,
                ),
            ]
        )

        async def fake_get_features_in_bbox_single(*args, **kwargs):
            return next(responses)

        monkeypatch.setattr(nf, "get_features_in_bbox_single", fake_get_features_in_bbox_single)

        response = await nf.get_features_in_bbox(
            NadagHTTPClient(),
            [211993.38433339744, 6648075.313127061, 217185.76397063793, 6649828.643472111],
            "geotekniskdokument",
            max_dist_query=2500,
        )

        assert [feature["id"] for feature in response.features] == ["first-id", "shared-id", "last-id"]
        assert response.numberReturned == 3

    async def test_preserves_conflicting_features_with_same_id_and_warns(self, monkeypatch):
        responses = iter(
            [
                PaginatedResponse(
                    features=[{"type": "Feature", "id": "shared-id", "properties": {"value": 1}}],
                    numberReturned=1,
                ),
                PaginatedResponse(
                    features=[{"type": "Feature", "id": "shared-id", "properties": {"value": 2}}],
                    numberReturned=1,
                ),
            ]
        )
        warnings = []

        async def fake_get_features_in_bbox_single(*args, **kwargs):
            return next(responses)

        monkeypatch.setattr(nf, "get_features_in_bbox_single", fake_get_features_in_bbox_single)
        monkeypatch.setattr(nf.logger, "warning", warnings.append)

        response = await nf.get_features_in_bbox(
            NadagHTTPClient(),
            [211993.38433339744, 6648075.313127061, 217185.76397063793, 6649828.643472111],
            "geotekniskdokument",
            max_dist_query=2500,
        )

        assert [feature["properties"]["value"] for feature in response.features] == [1, 2]
        assert response.numberReturned == 2
        assert warnings == [
            "Collection geotekniskdokument returned different features with the same top-level id 'shared-id'; "
            "preserving both."
        ]


# ---------------------------------------------------------------------------
# PaginatedResponse with real fixtures
# ---------------------------------------------------------------------------


class TestPaginatedResponseReal:
    def test_parse_geotekniskborehull(self):
        data = load_fixture("geotekniskborehull.json")
        pr = PaginatedResponse(**data)
        assert len(pr) == data["numberReturned"]
        assert pr.numberMatched is not None

    def test_parse_geotekniskborehullunders(self):
        data = load_fixture("geotekniskborehullunders.json")
        pr = PaginatedResponse(**data)
        assert len(pr) == data["numberReturned"]

    def test_to_gdf_geotekniskborehull(self):
        data = load_fixture("geotekniskborehull.json")
        pr = PaginatedResponse(**data)
        gdf = pr.to_gdf()
        assert len(gdf) == len(pr)
        assert "geometry" in gdf.columns
        assert FIELD.id_field in gdf.columns

    def test_to_gdf_geotekniskborehullunders(self):
        data = load_fixture("geotekniskborehullunders.json")
        pr = PaginatedResponse(**data)
        gdf = pr.to_gdf()
        assert len(gdf) == len(pr)
        assert FIELD.id_field in gdf.columns

    def test_geotekniskborehullunders_location_id_uses_underspkt_title(self):
        data = load_fixture("geotekniskborehullunders.json")
        pr = PaginatedResponse(**data)
        gdf = pr.to_gdf()

        assert MethodDataFrame.location_id.value == "undersPkt.title"
        assert SampleDataFrame.location_id.value == "undersPkt.title"
        assert MethodDataFrame.location_id.value in gdf.columns

    def test_indexed_keys_normalized_to_lists(self):
        """metode-KombinasjonSondering.1.href should become a list."""
        data = load_fixture("geotekniskborehullunders.json")
        pr = PaginatedResponse(**data)
        gdf = pr.to_gdf()
        method_key = FIELD.tot.metode_key
        if method_key in gdf.columns:
            non_null = gdf[method_key].dropna()
            if len(non_null) > 0:
                first_val = non_null.iloc[0]
                assert isinstance(first_val, list), f"Expected list, got {type(first_val)}"
                assert isinstance(first_val[0], dict)
                assert "href" in first_val[0]

    def test_merge_pages(self):
        data = load_fixture("geotekniskborehull.json")
        pr1 = PaginatedResponse(**data)
        pr2 = PaginatedResponse(**data)
        merged = PaginatedResponse.merge([pr1, pr2])
        assert len(merged) == len(pr1) + len(pr2)
        assert merged.numberReturned == (pr1.numberReturned or 0) + (pr2.numberReturned or 0)

    def test_merge_empty_list(self):
        merged = PaginatedResponse.merge([])
        assert len(merged) == 0
        assert merged.numberReturned == 0

    def test_parse_all_collections(self):
        """All real fixture files should parse without errors."""
        for fixture_file in FIXTURES_REAL.glob("*.json"):
            if fixture_file.name == "metadata.json":
                continue
            data = load_fixture(fixture_file.name)
            pr = PaginatedResponse(**data)
            assert len(pr) >= 0, f"Failed to parse {fixture_file.name}"


# ---------------------------------------------------------------------------
# process_api_responses with real fixtures
# ---------------------------------------------------------------------------


class TestProcessApiResponses:
    def test_with_real_fixture(self):
        data = load_fixture("kombinasjonsonderingdata.json")
        result = NadagHTTPClient.process_api_responses([data])
        assert len(result) == 1
        assert len(result[0]) > 0
        # Each item should have properties extracted + feature_id injected
        assert FIELD.feature_id in result[0][0]

    def test_with_multiple_responses(self):
        data1 = load_fixture("kombinasjonsonderingdata.json")
        data2 = load_fixture("statisksonderingdata.json")
        result = NadagHTTPClient.process_api_responses([data1, data2])
        assert len(result) == 2

    def test_skips_none_responses(self):
        data = load_fixture("kombinasjonsonderingdata.json")
        result = NadagHTTPClient.process_api_responses([None, data, None])
        assert len(result) == 1

    def test_skips_missing_features_key(self):
        malformed = load_fixture("missing_features_key.json", malformed=True)
        data = load_fixture("kombinasjonsonderingdata.json")
        result = NadagHTTPClient.process_api_responses([malformed, data])
        assert len(result) == 1

    def test_skips_features_without_properties(self):
        no_props = load_fixture("feature_no_properties.json", malformed=True)
        result = NadagHTTPClient.process_api_responses([no_props])
        # Should produce a list but features without properties are skipped
        assert isinstance(result, list)

    def test_empty_features_list(self):
        empty = load_fixture("empty_features.json", malformed=True)
        result = NadagHTTPClient.process_api_responses([empty])
        # Empty features = no properties extracted, so skipped
        assert len(result) == 0


class TestSamplesDataFrame:
    def test_z_alias_matches_location_elevation(self):
        locations = gpd.GeoDataFrame(
            {
                FIELD.id_field: ["loc-1"],
                SampleDataFrame.location_elevation.value: [42.5],
                SampleDataFrame.location_name.value: ["BH-1"],
            },
            geometry=[Point(10, 60)],
            crs="EPSG:4326",
        )
        test_series_data = pd.DataFrame(
            {
                SampleDataFrame.location_id.name: ["loc-1"],
                SampleDataFrame.method_id.value: ["sample-1"],
                SampleDataFrame.depth_top.value: [1.0],
                SampleDataFrame.depth_base.value: [2.0],
                SampleDataFrame.layer_composition_full.value: ["clay"],
            }
        )

        samples = get_samples_dataframe(
            NadagData(
                bounds=(10, 60, 11, 61),
                locations=locations,
                test_series_data=test_series_data,
            )
        )

        assert FIELD.z in samples.columns
        assert samples[FIELD.z].equals(samples[SampleDataFrame.location_elevation.name])


# ---------------------------------------------------------------------------
# Safe access helpers with real fixture shapes
# ---------------------------------------------------------------------------


class TestSafeAccessWithFixtures:
    def test_safe_extract_features_real(self):
        data = load_fixture("geotekniskborehull.json")
        features = safe_extract_features(data)
        assert len(features) == data["numberReturned"]

    def test_safe_extract_properties_real(self):
        data = load_fixture("geotekniskborehull.json")
        feature = data["features"][0]
        props = safe_extract_properties(feature)
        assert props is not None
        assert FIELD.id_field in props

    def test_safe_extract_feature_list_real(self):
        data = load_fixture("geotekniskborehull.json")
        props_list = safe_extract_feature_list(data)
        assert len(props_list) == data["numberReturned"]
        assert all(FIELD.id_field in p for p in props_list)

    def test_safe_first_on_method_list(self):
        """Test safe_first on the normalized method lists from GBHU."""
        data = load_fixture("geotekniskborehullunders.json")
        pr = PaginatedResponse(**data)
        gdf = pr.to_gdf()
        method_key = FIELD.tot.metode_key
        if method_key in gdf.columns:
            non_null = gdf[method_key].dropna()
            if len(non_null) > 0:
                val = non_null.iloc[0]
                first = safe_first(val, {})
                assert isinstance(first, dict)
                assert "href" in first


# ---------------------------------------------------------------------------
# Graceful degradation: malformed inputs don't crash
# ---------------------------------------------------------------------------


class TestGracefulDegradation:
    def test_missing_features_key(self):
        data = load_fixture("missing_features_key.json", malformed=True)
        assert safe_extract_features(data) == []

    def test_empty_features(self):
        data = load_fixture("empty_features.json", malformed=True)
        assert safe_extract_features(data) == []

    def test_feature_no_properties(self):
        data = load_fixture("feature_no_properties.json", malformed=True)
        features = safe_extract_features(data)
        # First feature has no properties, second does
        assert safe_extract_properties(features[0]) is None
        assert safe_extract_properties(features[1]) is not None

    def test_feature_no_id(self):
        data = load_fixture("feature_no_id.json", malformed=True)
        result = NadagHTTPClient.process_api_responses([data])
        # Should still extract properties, feature_id will be None
        if result and result[0]:
            assert result[0][0].get(FIELD.feature_id) is None

    def test_null_nested_fields(self):
        data = load_fixture("null_nested_fields.json", malformed=True)
        props_list = safe_extract_feature_list(data)
        # Should extract whatever is valid
        assert isinstance(props_list, list)

    def test_paginated_response_none_number_returned(self):
        """PaginatedResponse.merge should handle None numberReturned."""
        pr1 = PaginatedResponse(features=[], numberReturned=None)
        pr2 = PaginatedResponse(features=[], numberReturned=5)
        merged = PaginatedResponse.merge([pr1, pr2])
        assert merged.numberReturned == 5

    def test_process_api_responses_all_none(self):
        result = NadagHTTPClient.process_api_responses([None, None, None])
        assert result == []

    def test_process_api_responses_empty_list(self):
        result = NadagHTTPClient.process_api_responses([])
        assert result == []
