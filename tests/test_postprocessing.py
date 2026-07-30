import pandas as pd

from nadag_python import postprocessing
from nadag_python.data_models import FIELD, MethodDataDataFrame, MethodDataFrame
from nadag_python.postprocessing import drop_duplicate_tot_method_data, postprocess_methods_data_and_info


def _method_info(method_type: str) -> pd.DataFrame:
    method = FIELD.get_method_by_type(method_type)
    return pd.DataFrame(
        {
            FIELD.id_field: ["method-1"],
            FIELD.gbu_id: ["gbhu-1"],
            FIELD.gbu_ref: ["https://example.test/gbhu-1"],
            method.observasjon: ["https://example.test/data"],
        }
    )


def _method_data(method_type: str, depths: list[float], forces: list[float], times: list[int]) -> pd.DataFrame:
    method = FIELD.get_method_by_type(method_type)
    return pd.DataFrame(
        {
            method.id_ref: ["https://example.test/method-1"] * len(depths),
            method.id_ref.replace("href", "title"): ["method-1"] * len(depths),
            method.api_name: ["method-1"] * len(depths),
            MethodDataDataFrame.depth.value: depths,
            MethodDataDataFrame.penetration_force.value: forces,
            MethodDataDataFrame.penetration_time.value: times,
            MethodDataDataFrame.comment_code.value: ["14"] * len(depths),
            MethodDataDataFrame.comment.value: ["same comment"] * len(depths),
        }
    )


def _investigations() -> pd.DataFrame:
    return pd.DataFrame(
        {
            FIELD.id_field: ["gbhu-1"],
            MethodDataFrame.location_id.value: ["location-1"],
        }
    )


def test_drop_duplicate_tot_method_data_removes_exact_duplicate_rows(monkeypatch):
    method_data = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1"] * 6,
            MethodDataFrame.method_type.name: [FIELD.tot.name] * 6,
            MethodDataDataFrame.depth.value: [0.01, 0.02, 0.03, 0.01, 0.02, 0.03],
            MethodDataDataFrame.penetration_force.value: [10.0, 11.0, 12.0, 10.0, 11.0, 12.0],
            MethodDataDataFrame.penetration_time.value: [100, 101, 102, 100, 101, 102],
            MethodDataDataFrame.comment_code.value: ["14"] * 6,
            MethodDataDataFrame.comment.value: ["same comment"] * 6,
        }
    )
    method_info = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1"],
            FIELD.model_gbhu_id: ["gbhu-1"],
        }
    )
    warnings = []
    monkeypatch.setattr(postprocessing.logger, "warning", warnings.append)

    cleaned = drop_duplicate_tot_method_data(method_data, method_info=method_info, investigations=_investigations())

    assert cleaned[MethodDataDataFrame.depth.value].tolist() == [0.01, 0.02, 0.03]
    warning_text = "\n".join(warnings)
    assert "Dropped duplicate TOT method data rows" in warning_text
    assert "location_id=location-1" in warning_text
    assert "method_id=method-1" in warning_text
    assert "removed_row_count=3" in warning_text


def test_drop_duplicate_tot_method_data_keeps_repeated_depths_with_different_measurements():
    method_data = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1"] * 4,
            MethodDataFrame.method_type.name: [FIELD.tot.name] * 4,
            MethodDataDataFrame.depth.value: [0.01, 0.02, 0.01, 0.02],
            MethodDataDataFrame.penetration_force.value: [10.0, 11.0, 20.0, 21.0],
            MethodDataDataFrame.penetration_time.value: [100, 101, 200, 201],
            MethodDataDataFrame.comment_code.value: ["14"] * 4,
            MethodDataDataFrame.comment.value: ["same comment"] * 4,
        }
    )

    cleaned = drop_duplicate_tot_method_data(method_data)

    assert len(cleaned) == 4
    assert cleaned.equals(method_data)


def test_postprocess_removes_tot_duplicates_before_nadag_data_is_created():
    _, methods_data = postprocess_methods_data_and_info(
        {FIELD.tot.name: _method_info(FIELD.tot.name)},
        {
            FIELD.tot.name: _method_data(
                FIELD.tot.name,
                depths=[0.01, 0.02, 0.03, 0.01, 0.02, 0.03],
                forces=[10.0, 11.0, 12.0, 10.0, 11.0, 12.0],
                times=[100, 101, 102, 100, 101, 102],
            )
        },
        investigations=_investigations(),
    )

    assert len(methods_data) == 3
    assert methods_data[MethodDataDataFrame.depth.value].tolist() == [0.01, 0.02, 0.03]


def test_postprocess_does_not_remove_exact_duplicates_for_non_tot_methods():
    _, methods_data = postprocess_methods_data_and_info(
        {FIELD.rp.name: _method_info(FIELD.rp.name)},
        {
            FIELD.rp.name: _method_data(
                FIELD.rp.name,
                depths=[0.01, 0.01],
                forces=[10.0, 10.0],
                times=[100, 100],
            )
        },
        investigations=_investigations(),
    )

    assert len(methods_data) == 2


def test_drop_duplicate_tot_method_data_does_not_cross_method_boundaries():
    method_data = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1", "method-2"],
            MethodDataFrame.method_type.name: [FIELD.tot.name, FIELD.tot.name],
            MethodDataDataFrame.depth.value: [0.01, 0.01],
            MethodDataDataFrame.penetration_force.value: [10.0, 10.0],
            MethodDataDataFrame.penetration_time.value: [100, 100],
            MethodDataDataFrame.comment_code.value: ["14", "14"],
            MethodDataDataFrame.comment.value: ["same comment", "same comment"],
        }
    )

    cleaned = drop_duplicate_tot_method_data(method_data)

    assert len(cleaned) == 2


def test_drop_duplicate_tot_method_data_uses_available_identity_columns():
    method_data = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1"] * 2,
            MethodDataFrame.method_type.name: [FIELD.tot.name] * 2,
            MethodDataDataFrame.depth.value: [0.01, 0.01],
            MethodDataDataFrame.penetration_force.value: [10.0, 10.0],
        }
    )

    cleaned = drop_duplicate_tot_method_data(method_data)

    assert len(cleaned) == 1


def test_drop_duplicate_tot_method_data_preserves_non_tot_rows_when_called_directly():
    method_data = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1"] * 2,
            MethodDataFrame.method_type.name: [FIELD.rp.name] * 2,
            MethodDataDataFrame.depth.value: [0.01, 0.01],
            MethodDataDataFrame.penetration_force.value: [10.0, 10.0],
            MethodDataDataFrame.penetration_time.value: [100, 100],
        }
    )

    cleaned = drop_duplicate_tot_method_data(method_data)

    assert cleaned.equals(method_data)


def test_drop_duplicate_tot_method_data_handles_duplicate_input_index_labels():
    method_data = pd.DataFrame(
        {
            MethodDataFrame.method_id.name: ["method-1"] * 4,
            MethodDataFrame.method_type.name: [FIELD.tot.name] * 4,
            MethodDataDataFrame.depth.value: [0.01, 0.02, 0.01, 0.02],
            MethodDataDataFrame.penetration_force.value: [10.0, 11.0, 10.0, 11.0],
            MethodDataDataFrame.penetration_time.value: [100, 101, 100, 101],
        },
        index=[0, 1, 0, 1],
    )

    cleaned = drop_duplicate_tot_method_data(method_data)

    assert cleaned[MethodDataDataFrame.depth.value].tolist() == [0.01, 0.02]
