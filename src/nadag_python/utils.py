import re

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from .data_models import BoundingBox


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
