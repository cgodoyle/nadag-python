from copy import deepcopy

import geopandas as gpd
import numpy as np
import pandas as pd
from matplotlib.axes import Axes

from .data_models import (
    FIELD,
    GeoDataFrameType,
    GrundigMethodDataFrame,
    GrundigSampleDataFrame,
    MethodDataFrame,
    MethodsConfig,
    NadagData,
    SampleDataFrame,
    SamplesConfig,
)
from .logging import get_module_logger

logger = get_module_logger(__name__)


########################################################################################################################
##### functions for methods (soundings)
########################################################################################################################


def add_empty_soundings(investigations, soundings_info) -> pd.DataFrame:
    """
    Adds empty soundings to the soundings_info DataFrame for investigations that do not have any method data.
    The function identifies investigations that do not have any method data by checking for the presence of method
    keys in the investigations DataFrame. For each investigation that does not have method data,
    it creates a new row in the soundings_info DataFrame with the appropriate fields filled in,
    including a dummy method_id and method_type to indicate that it is an empty sounding.
    The resulting DataFrame is then concatenated with the original soundings_info DataFrame and returned as output.

        Args:
        investigations (pd.DataFrame): The DataFrame containing investigation data, including method keys.
        soundings_info (pd.DataFrame): The DataFrame containing soundings information to which empty
                                        soundings will be added.
        Returns:
            pd.DataFrame: The updated soundings_info DataFrame with empty soundings added.
    """

    method_keys = [xx.metode_key for xx in FIELD.methods] + [FIELD.sample.metode_key]

    investigations_with_no_data = investigations[
        ~investigations.apply(
            lambda row: any([(method_key in row and pd.notna(row[method_key])) for method_key in method_keys]),
            axis=1,
        )
    ]

    valid_methods = tuple(MethodsConfig.SOUNDINGS_FILTER)
    investigation_method_field = MethodDataFrame.method_type_nadag.value
    investigations_with_no_data = investigations_with_no_data.query(f"{investigation_method_field} in {valid_methods}")

    if investigations_with_no_data.empty:
        return soundings_info
    empty_methods_df = pd.DataFrame(
        [
            {
                # MethodDataFrame.geometry.name: xx[MethodDataFrame.geometry.value],
                MethodDataFrame.method_id.value: xx[FIELD.id_field]
                + "_"
                + str(xx[MethodDataFrame.method_type_nadag.value]),
                MethodDataFrame.gbhu_id.value: xx[FIELD.id_field],
                MethodDataFrame.depth_to_rock.value: xx[MethodDataFrame.depth_to_rock.value],
                MethodDataFrame.depth_to_rock_quality.value: xx[MethodDataFrame.depth_to_rock_quality.value],
                MethodDataFrame.method_type.value: MethodsConfig.GEOTEKNISKMETODE_TO_METHOD_TYPE_MAPPER.get(
                    xx[MethodDataFrame.method_type_nadag.value]
                ),
            }
            for _, xx in investigations_with_no_data.iterrows()
        ]
    )

    if not empty_methods_df.empty:
        if soundings_info is None or soundings_info.empty:
            empty_methods_df = empty_methods_df.astype(
                {col: soundings_info[col].dtype for col in empty_methods_df.columns if col in soundings_info.columns},
                errors="ignore",
            )
        soundings_info_out = pd.concat([empty_methods_df, soundings_info], ignore_index=True).reset_index(drop=True)
        logger.debug(f"Added {len(empty_methods_df)} empty soundings to soundings_info.")
    else:
        soundings_info_out = soundings_info.copy()
    return soundings_info_out


def postprocess_methods_data_and_info(
    methods_info_in: dict[str, pd.DataFrame],
    methods_data_in: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Post-processes the soundings data and info DataFrames to ensure correct data types and structure.

    Args:
        methods_info_in (dict[str, pd.DataFrame]): The raw soundings info fetched from the API.
        methods_data_in (dict[str, pd.DataFrame]): The raw soundings data fetched from the API.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the post-processed soundings data and info DataFrames.
    """

    soundings_data = deepcopy(methods_data_in)
    soundings_info = deepcopy(methods_info_in)

    new_data = []
    new_info = []
    for method in FIELD.methods:
        method_type = method.name
        method_nadag_id = method.id_ref

        logger.debug(f"Processing method type: {method_type} with nadag_id: {method_nadag_id}")

        method_data = soundings_data.get(method_type)
        method_info = soundings_info.get(method_type)
        if method_info is None:
            logger.debug(f"No method info found for type: {method_type}")
            continue

        method_info[MethodDataFrame.method_id.name] = method_info[FIELD.id_field]
        method_info[MethodDataFrame.gbhu_id.name] = method_info[FIELD.gbu_id]
        method_info[MethodDataFrame.method_type.name] = method_type
        method_info = method_info.drop(
            columns=[
                FIELD.id_field,
                FIELD.gbu_ref,
                FIELD.get_method_by_type(method_type).observasjon,
            ]
        )

        if method_data is None:
            logger.debug(f"No method data found for type: {method_type}")
            continue
        method_data[MethodDataFrame.method_type.name] = method_type
        if method_nadag_id not in method_data.columns:
            logger.debug(f"Method data does not contain nadag_id: {method_nadag_id} for type: {method_type}")
            continue

        method_data[MethodDataFrame.method_id.name] = method_data[method_nadag_id.replace("href", "title")]
        method_data = method_data.drop(columns=[method_nadag_id, FIELD.get_method_by_type(method_type).api_name])

        if method_type == FIELD.cpt.name:
            cpt_info_dict = method_info[MethodsConfig.CPT_INFO_COLUMNS].to_dict(orient="records")
            method_info[MethodDataFrame.cpt_info.name] = cpt_info_dict
            method_info = method_info.drop(columns=MethodsConfig.CPT_INFO_COLUMNS)

        new_data.append(method_data)
        new_info.append(method_info)

    if len(new_data) == 0:
        logger.warning("No method data found for any method type. Returning empty DataFrames.")
        new_data_df = pd.DataFrame()
    else:
        new_data_df = pd.concat(new_data, ignore_index=True).reset_index(drop=True)

    if len(new_info) == 0:
        logger.warning("No method info found for any method type. Returning empty DataFrames.")
        new_info_df = pd.DataFrame()
    else:
        new_info_df = pd.concat(new_info, ignore_index=True).reset_index(drop=True)

    return new_info_df, new_data_df


def create_flagged_column(
    df: pd.DataFrame,
    col_start_bool_series: pd.Series,
    col_end_bool_series: pd.Series,
) -> pd.Series:
    """
    Creates a boolean column that indicates active intervals based on start and end boolean series.
    The function iterates through the DataFrame index and updates the active state based on the start and end boolean series.
    When a start flag is encountered, the active state is set to True, and when an end flag is encountered, the active state is set to False.
    The resulting active state is stored in a new column that indicates whether the interval is active at each index.

    Args:
        df (pd.DataFrame): The input DataFrame for which the flagged column is to be created.
        col_start_bool_series (pd.Series): A boolean Series indicating the start of intervals.
        col_end_bool_series (pd.Series): A boolean Series indicating the end of intervals.

    Returns:
        pd.Series: A boolean Series indicating the active state of intervals based on the start and end flags.
    """
    n = len(df)
    if n == 0:
        return pd.Series([False] * n, index=df.index, dtype=bool)

    active_state_col = pd.Series(False, index=df.index, dtype=bool)
    current_is_active = False

    for i in df.index:
        if col_end_bool_series.loc[i]:
            current_is_active = False
        elif col_start_bool_series.loc[i]:
            current_is_active = True

        active_state_col.loc[i] = current_is_active

    return active_state_col


def create_intervals_from_comments(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates boolean columns for hammering, increased rotation rate, and flushing based on the comment codes in the input DataFrame.
    The function checks for the presence of a "comment_code" column in the input DataFrame.
    If it exists, it processes the comment codes to identify specific events (hammering, increased rotation rate, flushing) based on predefined flag codes from the MethodsConfig.
    It then creates boolean columns for each event type and also creates interval columns that indicate when these events are active based on start and end flags.

    Args:
        input_df (pd.DataFrame): The input DataFrame containing a "comment_code" column and optional start/end flag columns for each event type.

    Returns:
        pd.DataFrame: A DataFrame containing boolean columns for each event type and their corresponding intervals.
    """
    df = input_df.copy()

    flag_codes_config = MethodsConfig.NADAG_FLAG_CODES

    interval_types = ["hammering", "increased_rotation_rate", "flushing"]

    if "comment_code" not in df.columns:
        logger.debug(
            f"Column 'comment_code' not found in DataFrame. Returning False '{'/'.join(interval_types)}' columns."
        )
        default_false_series = pd.Series(False, index=df.index, dtype=bool)
        return pd.DataFrame(dict.fromkeys(interval_types, default_false_series))

    def format_comment_value(value):
        if pd.isna(value):
            return ""
        if isinstance(value, (int, float)):
            return str(int(value))
        return str(value)

    df["comment_code_str"] = df["comment_code"].apply(format_comment_value)

    for event_col_name, target_codes in flag_codes_config.items():
        if not isinstance(target_codes, list):
            df[event_col_name] = pd.Series(False, index=df.index, dtype=bool)
            logger.debug(f"Column '{event_col_name}' in flag_codes_config is not a list. Setting to False.")
            continue

        str_target_codes = [str(tc) for tc in target_codes]
        df[event_col_name] = df["comment_code_str"].apply(
            lambda comment_str: any(target_code in comment_str for target_code in str_target_codes)
        )

    for base_col_name in interval_types:
        start_event_col_name = base_col_name + "_starts"
        end_event_col_name = base_col_name + "_ends"

        if start_event_col_name not in df.columns:
            df[start_event_col_name] = pd.Series(False, index=df.index, dtype=bool)
        if end_event_col_name not in df.columns:
            df[end_event_col_name] = pd.Series(False, index=df.index, dtype=bool)

        df[base_col_name] = create_flagged_column(df, df[start_event_col_name], df[end_event_col_name])

    return df[interval_types]


########################################################################################################################
##### functions for samples  (TODO: try to be consistent with the naming of the methods)
########################################################################################################################


def merge_sample_dataframes(
    sample_series: GeoDataFrameType,
    sample_data: GeoDataFrameType,
) -> GeoDataFrameType:
    """
    Merges sample series and sample data into a single DataFrame.
    The function takes two DataFrames as input: one containing sample series information and another containing sample data.
    It identifies the common identifier field between the two DataFrames and performs a left merge based on this identifier.
    After merging, it drops any duplicate columns that may have been created during the merge process and resets the index of the resulting DataFrame before returning it.

    Args:
        sample_series (GeoDataFrameType): A DataFrame containing sample series information, including identifiers and geometries.

    Returns:
        GeoDataFrameType: A merged DataFrame that combines the sample series and sample data based on the common identifier, with duplicate columns removed and index reset.        GeoDataFrameType: A merged DataFrame that combines the sample series and sample data based on the common identifier, with duplicate columns removed and index reset.

    """

    id_field = FIELD.sample.serie_id

    data_to_series_id = FIELD.sample.serie_id_ref

    sample_data[id_field] = sample_data[data_to_series_id]

    merged_samples = pd.merge(sample_series, sample_data, on=id_field, how="left", suffixes=("", "_drop"))
    columns_to_drop = [col for col in merged_samples.columns if col.endswith("_drop")]
    merged_samples = merged_samples.drop(columns=columns_to_drop).reset_index(drop=True)
    return merged_samples


def get_samples_dataframe(
    nadag_data: NadagData,
    aggregate: bool = True,
) -> GeoDataFrameType:
    """
    Post-processes the samples DataFrame to ensure correct data types and structure.
    The function takes the raw samples data from the NadagData object and performs several operations to prepare it for analysis and visualization.
    It first creates a copy of the test series data and the locations data from the NadagData object.
    It then merges these two DataFrames based on the location identifier to combine the sample data with the corresponding location information.
    After merging, it renames the columns according to a predefined column mapper and drops any columns that are not part of the expected fields for the sample data.
    The function also calculates the depth of each sample using a helper function and adds this information as a new column.
    If the aggregate flag is set to True, it further aggregates the samples based on a specified identifier field using a custom aggregation function.
    Finally, it converts the resulting DataFrame into a GeoDataFrame with the appropriate coordinate reference system and returns it.

    Args:
        nadag_data (NadagData): The input data containing raw samples and location information.
        aggregate (bool): A flag indicating whether to aggregate the samples based on a specified identifier field. Default is True.

    Returns:
        GeoDataFrameType: A post-processed GeoDataFrame containing the sample data with correct data types, structure, and optional aggregation applied.
    """
    df = nadag_data.test_series_data.copy()
    locs = nadag_data.locations.copy()
    locs[SampleDataFrame.location_id.name] = locs[FIELD.id_field]

    if SampleDataFrame.layer_composition_full.value in df.columns:
        df[SampleDataFrame.layer_composition.name] = df[SampleDataFrame.layer_composition_full.value]
    else:
        logger.warning(f"did not find {SampleDataFrame.layer_composition_full.value} in columns: {df.columns}")
        df[SampleDataFrame.layer_composition.name] = np.nan

    merged = pd.merge(
        df,
        locs,
        on=SampleDataFrame.location_id.name,
        how="left",
    )

    column_mapper = SampleDataFrame.column_mapper()
    cols_to_drop = [col for col in merged.columns if col not in SampleDataFrame.fields()]

    merged = merged.rename(columns=column_mapper).drop(columns=cols_to_drop, errors="ignore").reset_index(drop=True)
    merged[SampleDataFrame.depth.name] = merged.apply(get_sample_depth, axis=1)
    if aggregate:
        merged = aggregate_samples(merged, id_field=SampleDataFrame.method_id.name)
    merged = gpd.GeoDataFrame(merged, crs=locs.crs)
    return merged


def aggregate_samples(
    samples_gdf: gpd.GeoDataFrame | pd.DataFrame,
    id_field: str = SampleDataFrame.method_id.name,
) -> GeoDataFrameType:
    """
    Aggregates the samples based on a specified identifier field using custom aggregation functions for each column.
    The function takes a GeoDataFrame containing sample data and an identifier field to group by.
    It defines custom aggregation functions for specific columns, such as calculating the mean for numeric columns and applying a custom function for classification columns.
    For columns that do not have a specified aggregation function, it uses a default function that takes
    the first value from the group.
    The function then applies the aggregation functions to the grouped data and returns the resulting aggregated GeoDataFrame.

    Args:
        samples_gdf (gpd.GeoDataFrame | pd.DataFrame): The input GeoDataFrame containing sample data to be aggregated.
        id_field (str): The name of the column to group by for aggregation. Default is SampleDataFrame.method_id.name.

    Returns:
        GeoDataFrameType: An aggregated GeoDataFrame containing the sample data grouped by the specified identifier field, with custom aggregation applied to each column.
    """

    def take_any(x):
        return x.iloc[0]

    def join_texts(x):
        # Filter out None, "nan", "none", and empty strings
        filtered_x = [
            item
            for item in x
            if item is not None and str(item).lower() not in ("nan", "none", "") and str(item).strip() != ""
        ]
        # Join with ' | ' if there are any values left, otherwise return "-"
        return " | ".join(filtered_x) if filtered_x else SamplesConfig.nothing_name

    def _clf_aggr(x):
        values = x.astype(str).unique()
        if all(vv in ("nan", "none") for vv in values):
            return SamplesConfig.nothing_name
        else:
            for xx in values:
                if any(kwd in str(xx).lower() for kwd in SamplesConfig.BRITTLE_KEYWORDS):
                    return SamplesConfig.brittle_name
            return SamplesConfig.other_name

    default_agg_func = take_any

    agg_funcs = {
        SampleDataFrame.water_content.name: "mean",
        SampleDataFrame.layer_composition.name: _clf_aggr,
        SampleDataFrame.layer_composition_full.name: join_texts,
        SampleDataFrame.liquid_limit.name: "mean",
        SampleDataFrame.plastic_limit.name: "mean",
        SampleDataFrame.strength_undisturbed.name: "min",
        SampleDataFrame.strength_undrained.name: "min",
        SampleDataFrame.strength_remoulded.name: "min",
        SampleDataFrame.unit_weight.name: "mean",
        SampleDataFrame.organic_matter.name: "mean",
        SampleDataFrame.axial_deformation.name: "mean",
        SampleDataFrame.depth_base.name: "max",
        SampleDataFrame.depth_top.name: "min",
    }

    # Crear un diccionario de funciones de agregación que incluya la función por defecto
    agg_funcs_with_default = {col: agg_funcs.get(col, default_agg_func) for col in samples_gdf.columns}
    samples = samples_gdf.groupby(id_field, as_index=False).agg(agg_funcs_with_default)

    return samples


def get_sample_depth(sample: pd.Series) -> float:
    """
    Calculates the depth of a sample based on its top and base depth values. The function handles various cases to determine the appropriate depth value, including cases where one of the depth values is missing or when the top and base depths are equal. If both depth values are present and valid, it calculates the average of the two depths to determine the final depth value for the sample.

    Args:
        sample (pd.Series): A Series representing a sample, containing the depth_top and depth_base values.

    Returns:
        float: The calculated depth value for the sample based on the provided depth_top and depth_base values, following the defined logic for handling different cases of missing or equal depth values.

    """
    if (sample[SampleDataFrame.depth_top.name] > sample[SampleDataFrame.depth_base.name]) and (
        sample[SampleDataFrame.depth_base.name] == 0
    ):
        depth = sample[SampleDataFrame.depth_top.name]
    elif pd.isna(sample[SampleDataFrame.depth_base.name]) and not pd.isna(sample[SampleDataFrame.depth_top.name]):
        depth = sample[SampleDataFrame.depth_top.name]
    elif sample[SampleDataFrame.depth_top.name] == sample[SampleDataFrame.depth_base.name]:
        depth = sample[SampleDataFrame.depth_top.name]
    else:
        try:
            depth = (sample[SampleDataFrame.depth_top.name] + sample[SampleDataFrame.depth_base.name]) / 2
        except TypeError:
            depth = np.nan
    return depth


#########################################################################################################################
##### functions for export to a structured, simplified GeoDataFrame (NVE's Grundig / Field Manager-like structure)
#########################################################################################################################


def export_samples_to_gdf(nadag_data: NadagData) -> GeoDataFrameType:
    """
    Exports the samples data to a GeoDataFrame with the structure defined in GrundigSampleDataFrame.

    Args:
        nadag_data (NadagData): The input data containing raw samples and location information.

    Returns:
        GeoDataFrameType: A GeoDataFrame containing the sample data structured according to the fields

    """
    if nadag_data.test_series_aggregated is None or nadag_data.test_series_aggregated.empty:
        return gpd.GeoDataFrame(geometry=[])
    samples = nadag_data.test_series_aggregated.copy()
    samples = samples.rename(columns=GrundigSampleDataFrame.column_mapper())
    grundig_extra_fields = GrundigSampleDataFrame.extra_fields_values()
    for field, value in grundig_extra_fields.items():
        samples[field] = value

    samples[[FIELD.x, FIELD.y]] = samples.geometry.get_coordinates()
    samples[FIELD.z] = samples[SampleDataFrame.location_elevation.name]

    samples = samples.drop(columns=[col for col in samples.columns if col not in GrundigSampleDataFrame.fields()])
    samples = samples.set_crs(nadag_data.locations.crs)

    return samples


def export_methods_to_gdf(nadag_data: NadagData) -> GeoDataFrameType:
    """
    Exports the methods (soundings) data to a GeoDataFrame with the structure defined in GrundigMethodDataFrame.

    Args:
        nadag_data (NadagData): The input data containing raw soundings info and data.

    Returns:
        GeoDataFrameType: A GeoDataFrame containing the soundings data structured according to the fields defined in GrundigMethodDataFrame.

    """

    methods = gpd.GeoDataFrame(
        [
            nadag_data.query_method(method_id=mm)
            for mm in nadag_data.methods_info[MethodDataFrame.method_id.name].unique()
        ]
    )

    methods = methods.rename(columns=GrundigMethodDataFrame.column_mapper())

    grundig_extra_fields = GrundigMethodDataFrame.extra_fields_values()
    for field, value in grundig_extra_fields.items():
        methods[field] = value

    methods[[FIELD.x, FIELD.y]] = methods.geometry.get_coordinates()
    methods[FIELD.z] = methods[MethodDataFrame.elevation.name]

    # convert to method types ala Field Manager using the mapper from MethodsConfig
    methods[GrundigMethodDataFrame.method_type.name] = methods[MethodDataFrame.method_type_nadag.name].map(
        MethodsConfig.GEOTEKNISKMETODE_TO_METHOD_TYPE_MAPPER
    )

    # ----------------------------------------------------------------------------------------------
    # adding flagged columns for hammering, increased rotation rate and flushing based on comment codes for tot data
    def apply_intervals_if_tot(method_type, data):
        if method_type == FIELD.tot.name and data is not None and not data.empty:
            intervals = create_intervals_from_comments(data)
            return data.join(intervals.drop(columns=data.columns, errors="ignore"))
        return data

    methods[MethodDataFrame.data.name] = [
        apply_intervals_if_tot(row[MethodDataFrame.method_type.name], row[MethodDataFrame.data.name])
        for _, row in methods.iterrows()
    ]
    # ----------------------------------------------------------------------------------------------

    cols_to_drop = [col for col in methods.columns if col not in GrundigMethodDataFrame.fields()]
    methods = methods.drop(columns=cols_to_drop)
    methods = methods.fillna(np.nan)
    methods = methods.set_crs(nadag_data.locations.crs)

    return methods


def get_boreholes_and_samples(
    nadag_data: NadagData,
) -> tuple[GeoDataFrameType, GeoDataFrameType]:
    """
    Exports both methods (soundings) and samples data to GeoDataFrames with the structure defined in GrundigMethodDataFrame and GrundigSampleDataFrame, respectively.

    Args:
        nadag_data (NadagData): The input data containing raw soundings info, soundings data, samples and location information.

    Returns:
        tuple[GeoDataFrameType, GeoDataFrameType]: A tuple containing two GeoDataFrames: the first one with the soundings data structured according to GrundigMethodDataFrame, and the second one with the sample data structured according to GrundigSampleDataFrame.


    """
    boreholes_gdf = export_methods_to_gdf(nadag_data)
    samples_gdf = export_samples_to_gdf(nadag_data)
    return boreholes_gdf, samples_gdf


#########################################################################################################################
##### Plotting functions
#########################################################################################################################


def plot_nadag_data(
    nadag_data: NadagData,
    markersize_boreholes: int = 100,
    markersize_samples: int = 30,
    round_to: int = 100,
    percent_margin: float = 0.1,
) -> Axes:
    """
    Quick and simple plotting of boreholes and samples from nadag data. Mainly for quick visualization in notebooks.

    Args:
        nadag_data (NadagData): The input data containing raw soundings info, soundings data, samples and location information.
        markersize_boreholes (int): The marker size for plotting boreholes. Default is 100.
        markersize_samples (int): The marker size for plotting samples. Default is 30.
        round_to (int): The value to which the plot limits will be rounded. Default is 100.
        percent_margin (float): The percentage margin to add to the plot limits. Default is 0.1 (10%).

    Returns:
        Axes: The axes object containing the plot of boreholes and samples.

    """

    boreholes = nadag_data.investigations.copy()
    samples = nadag_data.test_series_aggregated.copy()

    xmin, xmax = (
        min(boreholes.geometry.x.min(), samples.geometry.x.min()),
        max(boreholes.geometry.x.max(), samples.geometry.x.max()),
    )
    ymin, ymax = (
        min(boreholes.geometry.y.min(), samples.geometry.y.min()),
        max(boreholes.geometry.y.max(), samples.geometry.y.max()),
    )

    x_margin = (xmax - xmin) * percent_margin
    y_margin = (ymax - ymin) * percent_margin
    xmin -= x_margin
    xmax += x_margin
    ymin -= y_margin
    ymax += y_margin

    xmin = (xmin // round_to) * round_to
    xmax = ((xmax // round_to) + 1) * round_to
    ymin = (ymin // round_to) * round_to
    ymax = ((ymax // round_to) + 1) * round_to

    boreholes["dummy"] = boreholes["geotekniskMetode"].map(MethodsConfig.GEOTEKNISKMETODE_TO_METHOD_TYPE_MAPPER)
    ax = boreholes.plot(column="dummy", markersize=markersize_boreholes, categorical=True, legend=True)
    samples.plot(ax=ax, color="black", markersize=markersize_samples, marker="x", label="Samples")
    ax.set_xlim(xmin, xmax)
    ax.set_ylim(ymin, ymax)
    # _ = ax.legend()
    return ax
