"""Shared type aliases used across the package.

Kept in a standalone module to avoid circular imports between
``data_models`` and ``utils``.
"""

from typing import Union

import geopandas as gpd
import pandas as pd

BoundingBox = (
    list[int | float] | tuple[int | float, int | float, int | float, int | float]
)  # [x_min, y_min, x_max, y_max]

GeoDataFrameType = Union[gpd.GeoDataFrame, pd.DataFrame]
