import re
from typing import Any

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from .data_models import BoundingBox
from .logging import get_module_logger

logger = get_module_logger(__name__)


def normalize_columns(df: pd.DataFrame, canonical_columns: list[str]) -> pd.DataFrame:
    """
    Normalize column names in a DataFrame to match a list of canonical column names, ignoring case.
    This function takes a DataFrame and a list of canonical column names, and renames the columns in the
    DataFrame to match the canonical names, ignoring case differences. If a column in the DataFrame does not
    match any of the canonical names (ignoring case), it will be left unchanged.

    Args:
        df (pd.DataFrame): The DataFrame whose columns are to be normalized.
        canonical_columns (list[str]): A list of canonical column names to match against.

    Returns:
        pd.DataFrame: A new DataFrame with columns renamed to match the canonical names, ignoring case.

    """
    # Crear un mapeo: nombre_lowercase -> nombre_canónico
    canonical_map = {col.lower(): col for col in canonical_columns}

    rename_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in canonical_map and col != canonical_map[col_lower]:
            rename_map[col] = canonical_map[col_lower]

    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def clean_url(url: str) -> str:
    """
    Clean a URL by removing duplicate slashes, except for the protocol part.

    Args:
        url (str): The URL to clean.

    Returns:
        str: The cleaned URL with duplicate slashes removed.

    Example:
    >>> clean_url("https://example.com//api//endpoint")
    'https://example.com/api/endpoint'
    """
    cleaned_url = re.sub(r"(?<!:)/{2,}", "/", url)
    return cleaned_url


def transform_bounds(bounds: BoundingBox, crs_in: int = 25833, crs_out: int = 4386) -> list[float]:
    """
    Transform bounding box coordinates from one CRS to another.

    Args:
    bounds (BoundingBox): A BoundingBox object containing the bounding box coordinates
        in the format [minx, miny, maxx, maxy].
    crs_in (int): The EPSG code of the input CRS. Default is 25833 (UTM zone 33N).
    crs_out (int): The EPSG code of the output CRS. Default is
        4326 (WGS 84).

    Returns:
    list[float]: A list containing the transformed bounding box coordinates
        in the format [minx, miny, maxx, maxy]. The coordinates are transformed from the input CRS to the output CRS.

    Example:
    >>> transform_bounds([500000, 0, 600000, 100000], crs_in=25833, crs_out=4326)
    [9.0, 0.0, 10.0, 1.0]
    """

    bbox = gpd.GeoDataFrame(geometry=[box(*bounds)], crs=crs_in).to_crs(crs_out).total_bounds
    return bbox


def split_bbox(bbox: gpd.GeoDataFrame, n_rows: int, n_cols: int) -> gpd.GeoDataFrame:
    """
    Split a bounding box into a regular grid of sub-boxes.
    This function divides a bounding box into a rectangular grid of smaller
    bounding boxes. Each sub-box is of equal size and covers a portion of the
    original bounding box area.

    Args:
        bbox (gpd.GeoDataFrame): A GeoDataFrame containing a single bounding box
            geometry. The bounding box is defined by the total bounds of the
            GeoDataFrame.
        n_rows (int): Number of rows in the grid. Must be a positive integer.
        n_cols (int): Number of columns in the grid. Must be a positive integer.

    Returns:
        gpd.GeoDataFrame: A GeoDataFrame containing the grid of sub-boxes as
            polygon geometries. Each row represents one sub-box and includes:
            - geometry: A box polygon for the sub-box
            - id: A unique integer identifier (1-indexed) for each sub-box

    Example:
        >>> bbox = gpd.GeoDataFrame(
        ...     geometry=[box(0, 0, 10, 10)],
        ...     crs="EPSG:4326"
        ... )
        >>> subgrid = split_bbox(bbox, n_rows=2, n_cols=2)
        >>> len(subgrid)
        4
    """

    minx, miny, maxx, maxy = bbox.total_bounds
    width = (maxx - minx) / n_cols
    height = (maxy - miny) / n_rows
    sub_boxes = []
    for i in range(n_cols):
        for j in range(n_rows):
            sub_minx = minx + i * width
            sub_miny = miny + j * height
            sub_maxx = sub_minx + width
            sub_maxy = sub_miny + height
            sub_boxes.append(box(sub_minx, sub_miny, sub_maxx, sub_maxy))
    subgrid = gpd.GeoDataFrame(geometry=sub_boxes, crs=bbox.crs)
    subgrid["id"] = range(1, len(subgrid) + 1)
    return subgrid


def case_insensitive_rename(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Rename columns in a DataFrame using a case-insensitive mapping.

    Args:
        df (pd.DataFrame): The DataFrame whose columns are to be renamed.
        mapping (dict): A dictionary where keys are the original column names (case-insensitive)
            and values are the new column names.

    Returns:
    pd.DataFrame: A new DataFrame with columns renamed according to the mapping.

    """
    # Create a mapping from lowercase column names to actual column names
    col_map = {col.lower(): col for col in df.columns}

    # Create the actual rename mapping
    rename_map = {}
    for old_name, new_name in mapping.items():
        old_lower = old_name.lower()
        if old_lower in col_map:
            rename_map[col_map[old_lower]] = new_name

    return df.rename(columns=rename_map)


def extract_nested_key_values(obj: Any, key: str) -> list[Any]:
    """
    Extract all non-null values for a key from nested dict/list/tuple structures.

    Args:
        obj (Any): Nested object to traverse.
        key (str): Dictionary key to extract.

    Returns:
        list[Any]: All matching non-null values found during traversal.
    """
    values: list[Any] = []

    if obj is None:
        return values

    if isinstance(obj, dict):
        if key in obj and obj[key] is not None:
            values.append(obj[key])

        for value in obj.values():
            if isinstance(value, (dict, list, tuple)):
                values.extend(extract_nested_key_values(value, key))

        return values

    if isinstance(obj, (list, tuple)):
        for item in obj:
            values.extend(extract_nested_key_values(item, key))

    return values


# ---------------------------------------------------------------------------
# Safe access helpers for API responses
# ---------------------------------------------------------------------------


def safe_extract_features(response: Any) -> list[dict]:
    """Safely extract the ``features`` list from a GeoJSON FeatureCollection.

    Args:
        response: The raw API response dict (or any object).

    Returns:
        The list of feature dicts, or ``[]`` if the key is missing or the
        response is not a dict.
    """
    if not isinstance(response, dict):
        logger.warning(f"Expected dict response, got {type(response).__name__}. Returning empty features list.")
        return []

    features = response.get("features")
    if features is None:
        logger.warning("Response missing 'features' key. Returning empty list.")
        return []

    if not isinstance(features, list):
        logger.warning(f"Expected 'features' to be a list, got {type(features).__name__}. Returning empty list.")
        return []

    return features


def safe_extract_properties(feature: Any) -> dict | None:
    """Safely extract ``properties`` from a single GeoJSON feature.

    Args:
        feature: A feature dict.

    Returns:
        The properties dict, or ``None`` if missing/malformed.
    """
    if not isinstance(feature, dict):
        return None
    props = feature.get("properties")
    if props is None or not isinstance(props, dict):
        return None
    return props


def safe_extract_feature_list(response: Any, key: str = "properties") -> list[dict]:
    """Extract a list of property dicts from a FeatureCollection, skipping malformed features.

    Combines :func:`safe_extract_features` and :func:`safe_extract_properties`
    into a single call.  Malformed features are silently skipped (with a debug
    log) so the pipeline keeps running with whatever data is valid.

    Args:
        response: The raw API response dict.
        key: The key to extract from each feature (default ``"properties"``).

    Returns:
        A list of dicts (one per valid feature).
    """
    features = safe_extract_features(response)
    results: list[dict] = []
    for feat in features:
        if not isinstance(feat, dict):
            logger.debug(f"Skipping non-dict feature: {type(feat).__name__}")
            continue
        value = feat.get(key)
        if value is None or not isinstance(value, dict):
            logger.debug(f"Skipping feature missing '{key}' key.")
            continue
        results.append(value)
    return results


def safe_first(collection: Any, default: Any = None) -> Any:
    """Safely get the first element of a sequence.

    Args:
        collection: Any indexable sequence (list, tuple, etc.) or ``None``.
        default: Value to return if the collection is empty or not a sequence.

    Returns:
        The first element, or *default*.
    """
    if collection is None:
        return default
    if not isinstance(collection, (list, tuple)):
        return default
    if len(collection) == 0:
        return default
    return collection[0]


def safe_iloc(df: pd.DataFrame | pd.core.groupby.DataFrameGroupBy, index: int = 0, default: Any = None) -> Any:
    """Safely index into a DataFrame with ``.iloc``.

    Args:
        df: A DataFrame (or Series).
        index: The positional index to retrieve.
        default: Value to return if the DataFrame is empty or index is out of range.

    Returns:
        The row at *index*, or *default*.
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return default
    try:
        return df.iloc[index]
    except (IndexError, KeyError):
        return default
