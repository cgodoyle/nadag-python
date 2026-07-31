import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from nadag_python.data_models import FIELD, MethodDataDataFrame, MethodDataFrame, NadagData
from nadag_python.nadag_functions import build_methods_info_from_metadata
from nadag_python.postprocessing import add_empty_soundings, get_boreholes_and_samples


class TestEmptySoundingsFlag:
    def _locations(self):
        return gpd.GeoDataFrame(
            {
                FIELD.id_field: ["loc-normal", "loc-empty"],
                MethodDataFrame.location_name.value: ["BH-1", "BH-2"],
                MethodDataFrame.depth.value: [10.0, 5.0],
                MethodDataFrame.elevation.value: [42.0, 43.0],
            },
            geometry=[Point(10, 60), Point(11, 61)],
            crs="EPSG:4326",
        )

    def _investigations(self):
        return gpd.GeoDataFrame(
            {
                FIELD.id_field: ["gbhu-normal", "gbhu-empty"],
                MethodDataFrame.location_id.value: ["loc-normal", "loc-empty"],
                MethodDataFrame.method_type_nadag.value: ["15", "15"],
                FIELD.tot.metode_key: [[{"href": "https://example.test/tot-1"}], pd.NA],
                MethodDataFrame.depth_to_rock.value: [8.0, 4.0],
                MethodDataFrame.depth_to_rock_quality.value: [1, 2],
            },
            geometry=[Point(10, 60), Point(11, 61)],
            crs="EPSG:4326",
        )

    def _methods_info(self):
        return pd.DataFrame(
            {
                MethodDataFrame.method_id.name: ["method-normal"],
                MethodDataFrame.gbhu_id.name: ["gbhu-normal"],
                MethodDataFrame.method_type.name: [FIELD.tot.name],
                "tilhørerGBU.title": ["gbhu-normal"],
            }
        )

    def _methods_data(self):
        return pd.DataFrame(
            {
                MethodDataFrame.method_id.name: ["method-normal"],
                MethodDataDataFrame.depth.name: [1.0],
                MethodDataDataFrame.penetration_force.name: [2.0],
            }
        )

    def test_add_empty_soundings_marks_only_synthesized_rows(self):
        methods_info = add_empty_soundings(self._investigations(), self._methods_info())

        assert len(methods_info) == 2
        assert MethodDataFrame.is_empty_sounding.name in methods_info.columns

        flags_by_id = methods_info.set_index(MethodDataFrame.method_id.name)[MethodDataFrame.is_empty_sounding.name]
        assert not flags_by_id["method-normal"]
        assert flags_by_id["gbhu-empty_15"]

    def test_get_boreholes_and_samples_preserves_empty_sounding_flag(self):
        methods_info = add_empty_soundings(self._investigations(), self._methods_info())
        nadag_data = NadagData(
            bounds=(10, 60, 11, 61),
            locations=self._locations(),
            investigations=self._investigations(),
            methods_info=methods_info,
            methods_data=self._methods_data(),
        )

        boreholes, samples = get_boreholes_and_samples(nadag_data)

        assert samples.empty
        assert len(boreholes) == 2
        assert MethodDataFrame.is_empty_sounding.name in boreholes.columns

        flags_by_id = boreholes.set_index(MethodDataFrame.method_id.name)[MethodDataFrame.is_empty_sounding.name]
        assert not flags_by_id["method-normal"]
        assert flags_by_id["gbhu-empty_15"]

    def test_get_boreholes_and_samples_defaults_flag_false_for_existing_data_without_column(self):
        nadag_data = NadagData(
            bounds=(10, 60, 11, 61),
            locations=self._locations().iloc[[0]].copy(),
            investigations=self._investigations().iloc[[0]].copy(),
            methods_info=self._methods_info(),
            methods_data=self._methods_data(),
        )

        boreholes, _ = get_boreholes_and_samples(nadag_data)

        assert len(boreholes) == 1
        assert MethodDataFrame.is_empty_sounding.name in boreholes.columns
        assert boreholes[MethodDataFrame.is_empty_sounding.name].tolist() == [False]

    def test_consumers_can_filter_empty_soundings_without_library_filtering(self):
        methods_info = add_empty_soundings(self._investigations(), self._methods_info())
        nadag_data = NadagData(
            bounds=(10, 60, 11, 61),
            locations=self._locations(),
            investigations=self._investigations(),
            methods_info=methods_info,
            methods_data=self._methods_data(),
        )

        boreholes, _ = get_boreholes_and_samples(nadag_data)
        non_empty_boreholes = boreholes[~boreholes[MethodDataFrame.is_empty_sounding.name]]

        assert len(boreholes) == 2
        assert non_empty_boreholes[MethodDataFrame.method_id.name].tolist() == ["method-normal"]

    def test_build_methods_info_from_metadata_uses_bbox_features_only(self):
        investigations = self._investigations()
        investigations.at[0, FIELD.tot.metode_key] = [{"title": "method-normal", "href": "https://example.test/tot-1"}]
        nadag_data = NadagData(
            bounds=(10, 60, 11, 61),
            locations=self._locations(),
            investigations=investigations,
        )

        methods_info = build_methods_info_from_metadata(nadag_data)

        assert MethodDataFrame.method_id.name in methods_info.columns
        assert MethodDataFrame.gbhu_id.name in methods_info.columns
        assert MethodDataFrame.location_id.name in methods_info.columns
        assert MethodDataFrame.location_name.value in methods_info.columns
        assert MethodDataFrame.depth.value in methods_info.columns
        assert MethodDataFrame.method_type_nadag.value in methods_info.columns
        assert MethodDataFrame.geometry.value in methods_info.columns

        rows_by_id = methods_info.set_index(MethodDataFrame.method_id.name)
        assert not rows_by_id.loc["method-normal", MethodDataFrame.is_empty_sounding.name]
        assert rows_by_id.loc["method-normal", MethodDataFrame.location_name.value] == "BH-1"
        assert rows_by_id.loc["gbhu-empty_15", MethodDataFrame.is_empty_sounding.name]
        assert rows_by_id.loc["gbhu-empty_15", MethodDataFrame.location_name.value] == "BH-2"
