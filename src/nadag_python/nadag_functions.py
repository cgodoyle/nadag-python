import asyncio
import time
from typing import Optional

import geopandas as gpd
import pandas as pd
from shapely.geometry import box

from .config import CRS, settings
from .data_models import (
    FIELD,
    BoundingBox,
    MethodDataDataFrame,
    NadagData,
    PaginatedResponse,
    SampleDataFrame,
)
from .http_client import NadagHTTPClient
from .logging import get_module_logger
from .postprocessing import (
    add_empty_soundings,
    create_intervals_from_comments,
    get_samples_dataframe,
    merge_sample_dataframes,
    postprocess_methods_data_and_info,
)
from .utils import case_insensitive_rename, split_bbox, transform_bounds

logger = get_module_logger(__name__)


async def check_api_status() -> bool:
    """
    Check the status of the NADAG API.

    Returns:
        bool: True if the API is reachable and returns a successful status code, False otherwise.
    """
    client = NadagHTTPClient()
    return await client.check_api_status()


async def get_features_in_bbox_single(
    http_client: NadagHTTPClient,
    bbox: BoundingBox,
    collection: str,
    limit: int = 1000,
    pagination_concurrency: Optional[int] = None,
) -> PaginatedResponse:
    """
    Fetch features from a specified collection within a bounding box.

    Args:
        bbox (BoundingBox): A list or tuple of four floats representing the bounding box [minx, miny, maxx, maxy].
        collection (str): The name of the collection to fetch features from.
        limit (int): The maximum number of features to return.
        pagination_concurrency (int, optional): Max concurrent requests for pagination.
                                               Defaults to API_MAX_CONCURRENCY if None.

    Returns:
        PaginatedResponse: A paginated response containing the features within the bounding box.
    """

    bbox = transform_bounds(bbox, crs_in=settings.DEFAULT_CRS, crs_out=settings.API_CRS)
    ss = f"{bbox[0]} {bbox[1]},{bbox[0]} {bbox[3]},{bbox[2]} {bbox[3]},{bbox[2]} {bbox[1]},{bbox[0]} {bbox[1]}"
    params = {
        "filter-lang": "cql2-text",
        "filter": f"S_INTERSECTS(posisjon,POLYGON(({ss})))",
        "crs": CRS.UTM33.url,
        "limit": limit,
    }

    url = http_client.build_collection_url(collection=collection)

    results = [
        item
        async for item in http_client.get_features_paginated(
            url, params=params, page_size=limit, max_concurrency=pagination_concurrency
        )
    ]
    return PaginatedResponse.merge(results)


async def get_features_in_bbox(
    http_client: NadagHTTPClient,
    bbox: BoundingBox,
    collection: str,
    limit: int = 1000,
    max_dist_query: int | float = settings.API_MAX_DIST_QUERY,
    pagination_concurrency: Optional[int] = None,
) -> PaginatedResponse:
    """
    Fetch features from a specified collection within a bounding box, splitting the query
    into smaller sub-boxes if the bounding box is too large.

    Args:
        bbox (BoundingBox): A list or tuple of four floats representing the bounding box [minx, miny, maxx, maxy].
        collection (str): The name of the collection to fetch features from.
        limit (int): The maximum number of features to return per sub-box.
        max_dist_query (int): The maximum distance in meters for each sub-box query.
        pagination_concurrency (int, optional): Max concurrent requests for pagination.
                                               Defaults to API_MAX_CONCURRENCY if None.

    Returns:
        PaginatedResponse: A paginated response containing the features within the bounding box.
    """
    n_cols, n_rows = (
        int(max((bbox[2] - bbox[0]) // max_dist_query, 1)),
        int(max((bbox[3] - bbox[1]) // max_dist_query, 1)),
    )
    if n_cols > 1 or n_rows > 1:
        logger.info(
            f"Max. distance for query is set to {max_dist_query} m. Splitting bounding box into {n_rows} rows and "
            f"{n_cols} columns for collection {collection}"
        )
    sub_boxes = split_bbox(
        gpd.GeoDataFrame(geometry=[box(*bbox)], crs=settings.DEFAULT_CRS),
        n_rows,
        n_cols,
    )
    response_list = await asyncio.gather(
        *[
            get_features_in_bbox_single(
                http_client,
                bbox=list(item.geometry.bounds),  # ty:ignore[unresolved-attribute]
                collection=collection,
                limit=limit,
                pagination_concurrency=pagination_concurrency,
            )
            for item in sub_boxes.itertuples()
        ]
    )

    response_list = [item for item in response_list if isinstance(item, PaginatedResponse) and len(item) > 0]
    return PaginatedResponse.merge(response_list)


async def get_soundings_data_raw(
    http_client: NadagHTTPClient,
    nadag_data: NadagData,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Fetch soundings data from the API for various methods in GBHU.

    Args:
    http_client (NadagHTTPClient): An instance of the NadagHTTPClient to use for fetching data.
    nadag_data (NadagData): The NadagData object containing the investigations data to extract method information from.

    Returns:
        tuple: A tuple containing two DataFrames:
            - soundings_info: Information about the soundings methods.
            - soundings_data: The actual soundings data for each method.
    """
    if nadag_data.investigations is None:
        raise ValueError("No data available. Please fetch first")

    methods_field_gbhu = [method.metode_key for method in FIELD.methods]
    found_methods = []
    urls = []
    gbhu_ids = []

    for method in FIELD.methods:
        mm_key = method.metode_key

        if mm_key not in nadag_data.investigations.columns:
            continue

        valid_rows = nadag_data.investigations.dropna(subset=[mm_key])

        current_urls = valid_rows[mm_key].map(lambda x: x[0][FIELD.href]).to_list()

        if not current_urls:
            continue

        found_methods.append(method.name)
        urls.append(current_urls)
        gbhu_ids.append(valid_rows[FIELD.id_field].to_list())

    if len(found_methods) != len(urls):
        logger.warning(
            f"Found methods: {found_methods}, but URLs for {methods_field_gbhu} are not all present in the data."
        )

    responses = []
    logger.debug(f"{len(urls)} methods found in GBHU.")

    async def _fetch_group(url_group: list[str]) -> list[dict]:
        group_responses = []
        async for feature in http_client.get_features_from_urls_stream(url_group):
            group_responses.append(feature)
        return group_responses

    responses = await asyncio.gather(*[_fetch_group(ug) for ug in urls])

    soundings_info = {mm: pd.DataFrame([xx["properties"] for xx in rr]) for mm, rr in zip(found_methods, responses)}
    # for ii, mm in enumerate(found_methods):
    #     soundings_info[mm][FIELD.model_gbhu_id] = gbhu_ids[ii]

    data_obs_href_list = [
        (sounding[FIELD.get_method_by_type(method).observasjon].tolist())
        for method, sounding in soundings_info.items()
        if FIELD.get_method_by_type(method).observasjon in sounding.columns
    ]

    data_obs_list = await asyncio.gather(*[http_client.get_href_list(href) for href in data_obs_href_list])

    data_obs = {method: data_obs_list[i] for i, method in enumerate(soundings_info.keys())}
    soundings_data = {
        method: pd.concat(
            [pd.DataFrame([xx["properties"] for xx in data_i["features"]]) for data_i in data_obs[method]]
        )
        for method in found_methods
    }

    return soundings_info, soundings_data


def _get_sample_series_from_responses(
    sample_responses: list,
    location_ids: list,
    investigations_ids: list,
) -> pd.DataFrame:
    """
    Process sample responses to create a DataFrame of sample series.

    Args:
        sample_responses (list): A list of sample responses from the API.
        location_ids (list): A list of location IDs corresponding to the sample responses.
        investigations_ids (list): A list of investigation IDs corresponding to the sample responses.

    Returns:
        pd.DataFrame: A DataFrame containing sample series with their properties, location IDs, and investigation IDs.
    """
    samples_dataframe_list = []
    for sr, loc_id, inv_id in zip(sample_responses, location_ids, investigations_ids):
        for feature_dict in sr:
            feature_list = feature_dict["features"]
            sample_df = pd.DataFrame([item["properties"] for item in feature_list])
            sample_df[SampleDataFrame.location_id.name] = loc_id
            sample_df[SampleDataFrame.gbhu_id.name] = inv_id
            samples_dataframe_list.append(sample_df)
    samples_dataframe = pd.concat(samples_dataframe_list, ignore_index=True)
    samples_dataframe[FIELD.sample.href] = samples_dataframe[FIELD.sample.data_href]
    return samples_dataframe


async def _get_sample_data_from_series(
    http_client: NadagHTTPClient,
    sample_series: pd.DataFrame,
) -> pd.DataFrame:
    """
    Fetch sample data from the API based on the sample series DataFrame.

    Args:
        http_client (NadagHTTPClient): An instance of the NadagHTTPClient to use for fetching data.
        sample_series (pd.DataFrame): A DataFrame containing sample series with their properties.
    Returns:
        pd.DataFrame: A DataFrame containing the sample data fetched from the API.
    """
    href_list = sample_series[FIELD.sample.href].to_list()
    sample_data_responses = await http_client.get_href_list(href_list)

    sample_data = http_client.process_api_responses(sample_data_responses)

    sample_data_df = pd.concat([pd.DataFrame(xx) for xx in sample_data])

    return sample_data_df


async def get_test_series(
    http_client: NadagHTTPClient,
    nadag_data: NadagData,
) -> pd.DataFrame:
    """
    Fetch test from the api by joining the data in (Prøveserie, ) Prøveseriedel and GeotekniskPrøveseriedelData endpoints.
    Each row is a GeotekniskPrøveseriedelData point with the info of its Prøveseriedel (so a lot of repetition and Nones)

    Args:
        http_client (NadagHTTPClient): An instance of the NadagHTTPClient to use for fetching data.
        nadag_data (NadagData): The NadagData object containing the investigations data to extract sample series information from.

    Returns:
        pd.DataFrame: A DataFrame containing the processed sample series data.
    """
    if FIELD.sample.metode_key not in nadag_data.investigations.columns:
        logger.info("No samples found in GBHU. Returning empty DataFrame.")
        return pd.DataFrame()

    filtered_gbhu = nadag_data.investigations.copy().dropna(subset=[FIELD.sample.metode_key]).reset_index(drop=True)

    filtered_gbhu[FIELD.sample.href] = (
        filtered_gbhu[FIELD.sample.metode_key].apply(lambda x: x[0].get(FIELD.sample.href)).to_list()
    )

    responses = await http_client.get_href_list(filtered_gbhu.href.to_list())

    sample_series = [[feat["properties"] for feat in response.get("features", [])] for response in responses]

    sample_series_locations = filtered_gbhu[SampleDataFrame.location_id.value].to_list()
    sample_series_investigations = filtered_gbhu[FIELD.id_field].to_list()

    if len(sample_series) != len(sample_series_investigations):
        logger.warning(
            f"Number of sample series responses ({len(sample_series)}) does not match number of GBHU features with sample series ({len(sample_series_investigations)})."
        )

    logger.debug(f"Fetched sample series for {len(sample_series)} locations.")

    sample_hrefs = [[sample[FIELD.sample.serie_href] for sample in sample_serie] for sample_serie in sample_series]

    sample_responses = await asyncio.gather(*[http_client.get_href_list(urls) for urls in sample_hrefs])

    samples_series_df = _get_sample_series_from_responses(
        sample_responses, sample_series_locations, sample_series_investigations
    )
    samples_data_df = await _get_sample_data_from_series(http_client, samples_series_df)

    merged_samples_df = merge_sample_dataframes(samples_series_df, samples_data_df)

    return merged_samples_df


async def get_method_and_sample_nadag_data(http_client: NadagHTTPClient, temp_data: NadagData) -> NadagData:
    """
    Fetch soundings and test series data from the API based on the investigations and locations data in temp_data.

    Args:
        http_client (NadagHTTPClient): An instance of the NadagHTTPClient to use for fetching data.
        temp_data (NadagData): A NadagData object containing the investigations and locations data to extract method and sample information from.

    Returns:
        NadagData: A new NadagData object with the fetched soundings and test series data added.

    """
    logger.info("Fetching soundings & test data...")
    try:
        (soundings_info_pre, soundings_data_pre), test_series = await asyncio.gather(
            get_soundings_data_raw(http_client, temp_data),
            get_test_series(http_client, temp_data),
        )
    except RuntimeError as e:
        logger.error(f"Failed to fetch complete data: {e}")
        raise RuntimeError(
            f"Could not fetch complete sounding/sample data. "
            f"Try reducing the bounding box area or increasing API_RETRY_ATTEMPTS. "
            f"Original error: {e}"
        ) from e

    soundings_info, soundings_data = postprocess_methods_data_and_info(soundings_info_pre, soundings_data_pre)

    soundings_info = add_empty_soundings(temp_data.investigations, soundings_info)

    if test_series.empty:
        logger.warning("No test series data found.")
        temp_data = NadagData(
            bounds=tuple(temp_data.bounds),
            locations=temp_data.locations,
            investigations=temp_data.investigations,
            methods_info=soundings_info,
            methods_data=soundings_data,
        )

        return temp_data

    temp_data = NadagData(
        bounds=tuple(temp_data.bounds),
        locations=temp_data.locations,
        investigations=temp_data.investigations,
        methods_info=soundings_info,
        methods_data=soundings_data,
        test_series_data=test_series,
    )

    logger.info("Creating samples dataframe...")
    samples = get_samples_dataframe(temp_data)

    return NadagData(
        bounds=tuple(temp_data.bounds),
        locations=temp_data.locations,
        investigations=temp_data.investigations,
        methods_info=soundings_info,
        methods_data=soundings_data,
        test_series_data=test_series,
        test_series_aggregated=samples,
    )


async def fetch_from_bounds(
    bounds: BoundingBox,
    max_distance_query: int | float = settings.API_MAX_DIST_QUERY,
    pagination_concurrency: Optional[int] = None,
) -> NadagData:
    """
    Fetch features from the API within the specified bounds.

    Args:
        bounds (BoundingBox): A list or tuple of four floats representing the bounding box [minx, miny, maxx, maxy].
        max_distance_query (int | float): The maximum distance for the query.
                                              Defaults to the value in settings.API_MAX_DIST_QUERY.
        pagination_concurrency (int, optional): Max concurrent requests for pagination.
                                               Defaults to API_MAX_CONCURRENCY if None.

    Returns:
        NadagData: A new NadagData object with the fetched data.
    """

    logger.info(f"Fetching features in bounds: {bounds}")
    logger.debug(settings.model_dump())

    async with NadagHTTPClient() as http_client:
        t0 = time.monotonic()
        gbhu_response, gbh_response = await asyncio.gather(
            get_features_in_bbox(
                http_client,
                bounds,
                FIELD.geotekniskborehullunders,
                max_dist_query=max_distance_query,
                pagination_concurrency=pagination_concurrency,
            ),
            get_features_in_bbox(
                http_client,
                bounds,
                FIELD.geotekniskborehull,
                max_dist_query=max_distance_query,
                pagination_concurrency=pagination_concurrency,
            ),
        )
        logger.info(f"Bbox feature fetch took {time.monotonic() - t0:.1f}s")

        if len(gbhu_response) == 0:
            logger.warning(f"No features found in {FIELD.geotekniskborehullunders} collection for the given bounds.")
            return NadagData(bounds=tuple(bounds))

        investigations = gbhu_response.to_gdf()
        logger.info(f"Fetched {len(investigations)} features in {FIELD.geotekniskborehullunders} collection.")

        locations = gbh_response.to_gdf()
        logger.info(f"Fetched {len(locations)} features in {FIELD.geotekniskborehull} collection.")

        # Create intermediate object for soundings fetch
        temp_data = NadagData(
            bounds=tuple(bounds),
            locations=locations,
            investigations=investigations,
        )

        t1 = time.monotonic()
        temp_data = await get_method_and_sample_nadag_data(http_client, temp_data)
        logger.info(f"Method & sample fetch took {time.monotonic() - t1:.1f}s")
        logger.info(f"Total fetch_from_bounds took {time.monotonic() - t0:.1f}s")
        return temp_data


async def fetch_from_location_ids(location_ids: list[str]) -> NadagData:
    """
    Fetch data from the NADAG API for a given list of location IDs.

    Args:
        location_ids (list[str]): A list of location IDs to fetch data for.
    Returns:
        NadagData: A NadagData object containing the fetched data for the given location ID.
    """

    async with NadagHTTPClient() as nadag_client:
        href_list = [
            nadag_client.build_collection_url(collection="geotekniskborehull") + f"/{lid}" for lid in location_ids
        ]
        resp = await nadag_client.get_href_list(href_list)

        locations = pd.concat(
            [
                PaginatedResponse(
                    type="FeatureCollection",
                    features=[rr],
                    numberReturned=len(rr) if isinstance(rr, dict) else 1,
                    numberMatched=len(rr) if isinstance(rr, dict) else 1,
                    timeStamp=None,
                ).to_gdf()
                for rr in resp
            ]
        )
        locations = locations.set_crs(settings.API_CRS, allow_override=True).to_crs(settings.DEFAULT_CRS)
        href_list = [
            nadag_client.build_collection_url(collection="geotekniskborehullunders", query_params={"underspkt_fk": lid})
            for lid in location_ids
        ]
        resp = await nadag_client.get_href_list(href_list)
        investigations = pd.concat([PaginatedResponse(**rr).to_gdf() for rr in resp])
        investigations = investigations.set_crs(settings.API_CRS, allow_override=True).to_crs(settings.DEFAULT_CRS)

        temp_data = NadagData(bounds=locations.total_bounds, locations=locations, investigations=investigations)

        temp_data = await get_method_and_sample_nadag_data(nadag_client, temp_data)

        return temp_data


def get_sounding_by_id(
    method_id: str,
    method_type: str,
) -> pd.DataFrame:
    """
    Fetch soundings data for a specific method ID and method type.

    Args:
        method_id (str): The ID of the method to fetch soundings data for. Example of method_id in nadag api: kombinasjonSondering=6cb9e8d8-d009-440c-bfa7-70f867750ea5
        method_type (str): The type of the method (e.g., "tot", "cpt", etc.).

    Returns:
        pd.DataFrame: A DataFrame containing the soundings data for the specified method ID and type.

    Example:
    >>> get_sounding_by_id("6cb9e8d8-d009-440c-bfa7-70f867750ea5", "tot")
    >>> get_sounding_by_id("some-other-id", "cpt")
    """
    collection = FIELD.get_method_by_type(method_type).data_collection
    nadag_method_name = FIELD.get_method_by_type(method_type).api_name

    client = NadagHTTPClient()
    url = client.build_collection_url(collection=collection, query_params={nadag_method_name: method_id})

    response = asyncio.run(client._get_async(href=url))

    if response is None:
        return pd.DataFrame()

    df = pd.DataFrame(gpd.GeoDataFrame.from_features(response["features"]).drop(columns="geometry"))
    df["method_type"] = method_type
    method_data = df.reset_index(drop=True)
    method_data = case_insensitive_rename(method_data, MethodDataDataFrame.column_mapper())
    if method_type == "tot":
        method_data[["hammering", "increased_rotation_rate", "flushing"]] = create_intervals_from_comments(method_data)
    else:
        method_data[["hammering", "increased_rotation_rate", "flushing"]] = False
    method_data = method_data[[col for col in method_data.columns if col in MethodDataDataFrame.fields()]]
    return method_data


def get_sounding_urls(
    method_type: str,
    method_id: Optional[str] = None,
    location_id: Optional[str] = None,
    gbhu_id: Optional[str] = None,
    investigation_area_id: Optional[str] = None,
) -> dict:
    """
    Get the urls for the different tables in the NADAG API for a given item.
    A sounding can be either be a borehole or a sample. At least the method_type must be provided, and one of
    the method_id, location_id, gbhu_id or investigation_area_id must be provided.

    Args:
        method_type (str): The type of sounding method, e.g. "tot", "rp", "cpt", "sa".
        method_id (str, optional): The ID of the method item. Required if method_type is provided.
        location_id (str, optional): The ID of the location item. Required if method_type is provided.
        gbhu_id (str, optional): The ID of the geotekniskborehullunders item. Required if method_type is provided.
        investigation_area_id (str, optional): The ID of the investigation area item. Required if method_type is provided.

    Returns:
        dict: A dictionary containing the urls for the different tables in the NADAG
            API for the given item.

    """

    method_parser = FIELD.api_url_mapper
    method_nadag = method_parser.get(method_type)
    out = {
        "geotekniskborehullunders": f"{settings.API_BASE_URL}/geotekniskborehullunders/items/{gbhu_id}"
        if gbhu_id is not None
        else "Not available",
        "method": f"{settings.API_BASE_URL}/{method_nadag}/items/{method_id}"
        if method_id is not None
        else "Not available",
        "location": f"{settings.API_BASE_URL}/geotekniskborehull/items/{location_id}"
        if location_id is not None
        else "Not available",
        "documents": f"{settings.API_BASE_URL}/geotekniskdokument/items?tilhorergu_fk={investigation_area_id}"
        if investigation_area_id is not None
        else "Not available",
        "infopage": f"{settings.API_FAKTAARK_URL}?id={location_id}" if location_id is not None else "Not available",
    }
    return out


def get_sounding_urls_from_series(method_item: pd.Series) -> dict:
    """
    Get the urls for the different tables in the NADAG API for a given method item.

    Args:
        method_item (pd.Series): A pandas Series containing the method item data. Must contain the following fields:
            - method_type: The type of sounding method, e.g. "tot", "rp", "cpt", "sa".
            - method_id: The ID of the method item.
            - location_id: The ID of the location item.
            - gbhu_id: The ID of the geotekniskborehullunders item.
            - investigation_area_id: The ID of the investigation area item.

    Returns:
        dict: A dictionary containing the urls for the different tables in the NADAG
            API for the given method item.

    """
    query_dict = method_item[["method_type", "method_id", "gbhu_id", "location_id", "investigation_area_id"]].to_dict()
    return get_sounding_urls(**query_dict)
