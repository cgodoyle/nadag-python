import asyncio
import logging
from typing import Any, AsyncGenerator, Optional

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .config import settings
from .data_models import (
    FIELD,
    PaginatedResponse,
)
from .logging import get_module_logger
from .utils import clean_url

TIMEOUT = httpx.Timeout(
    connect=10.0,
    read=settings.API_TIMEOUT,
    write=10.0,
    pool=10.0,
)

logger = get_module_logger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)


def api_retry(
    max_attempts=settings.API_RETRY_ATTEMPTS,
    wait_min=settings.API_RETRY_MIN_WAIT,
    wait_max=settings.API_RETRY_MAX_WAIT,
):
    """Simple retry decorator for API calls"""
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )


class NadagHTTPClient:
    """
    A client for making asynchronous HTTP requests to the NADAG API.

    """

    def __init__(
        self,
        base_url: str = settings.API_BASE_URL,
        max_concurrency: int = settings.API_MAX_CONCURRENCY,
    ):

        self.base_url = clean_url(base_url)
        self.semaphore = asyncio.Semaphore(max_concurrency)

    @property
    def query_url(self):
        return self.base_url + "/{collection}/items"

    @api_retry()
    async def check_api_status(self) -> bool:
        """
        Check the status of the NADAG API.

        Returns:
            bool: True if the API is reachable and responsive, False otherwise.
        """
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.get(self.base_url)
            response.raise_for_status()
            return True

    def build_collection_url(self, collection: str, query_params: Optional[dict[str, Any]] = None) -> str:
        """
        Build a URL for querying a collection with optional query parameters.

        Args:
            collection (str): The collection name
            query_params (dict, optional): Query parameters as key-value pairs

        Returns:
            str: The complete URL with query parameters

        Example:
            client.build_collection_url(
                "geotekniskborehullunders",
                {"underspkt_fk": "6d887f7b-5f3c-450c-8e4b-c038b912c170"}
                )
        """
        base = self.query_url.format(collection=collection)

        if query_params:
            # Usar httpx.URL para construir URLs de forma segura
            url = httpx.URL(base, params=query_params)
            return str(url)

        return base

    @api_retry()
    async def get_feature(self, url: str) -> dict:
        """
        Fetch a single feature by its URL.
        Args:
            url (str): The URL of the feature to fetch.
        Returns:
            dict: The JSON response containing the feature data.
        """
        url = clean_url(url)
        async with self.semaphore:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()

    async def get_features_from_urls(self, urls: list[str]) -> list[dict]:
        """
        Fetch features from a list of URLs asynchronously.
        Args:
            urls (list[str]): A list of URLs to fetch features from.
        Returns:
            list[dict]: A list of JSON responses containing the feature data.

        """
        tasks = [self.get_feature(url) for url in urls]
        return await asyncio.gather(*tasks)

    async def get_features_from_urls_stream(self, urls: list[str]):
        """
        Yield features from a list of URLs asynchronously as they complete.
        Args:
            urls (list[str]): A list of URLs to fetch features from.
        Yields:
            dict: The JSON response containing the feature data.
        """
        async with self.semaphore:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:

                async def fetch(url):
                    response = await client.get(clean_url(url))
                    response.raise_for_status()
                    return response.json()

                tasks = [fetch(url) for url in urls]
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    yield result

    @api_retry()
    async def get_features_paginated(
        self,
        url: str,
        params: Optional[dict] = None,
        page_size: int = 100,
    ) -> AsyncGenerator[PaginatedResponse, None]:
        """
        Fetch features from a collection in a paginated manner.
        Args:
            url (str): The URL of the collection to fetch features from.
            params (Optional[dict]): Query parameters for the request.
            page_size (int): The number of items to fetch per page.
        Yields:
            PaginatedResponse: A paginated response containing the features.

        """
        logger.debug(f"API endpoint: {url}")
        params = params or {}
        params["limit"] = page_size
        next_url = clean_url(url)
        first_page = True

        async with self.semaphore:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                while next_url:
                    current_params = params if first_page else None
                    first_page = False
                    response = await client.get(next_url, params=current_params)
                    response.raise_for_status()
                    data = response.json()
                    yield PaginatedResponse(**data)

                    next_link = next(
                        (link["href"] for link in data.get("links", []) if link.get("rel") == "next"),
                        None,
                    )
                    if next_link:
                        next_url = next_link
                        params = {}  # limpiar params después de la primera iteración
                    else:
                        next_url = None

    @api_retry()
    async def _get_async(self, href: str, params: dict | None = None) -> dict | None:
        """
        Optimized version of get_async that automatically handles pagination.
        It's compatible with the original function signature to facilitate substitution.

        Args:
            href (str): URL for the request
            params (dict): Query parameters for the request

        Returns:
            dict: Combined data from all pages
        """
        if href is None:
            return None

        all_features = []
        next_url = href
        first_page = True

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            while next_url:
                # Use parameters only in the first request
                current_params = params if first_page else None
                first_page = False

                try:
                    response = await client.get(next_url, params=current_params)
                    response.raise_for_status()  # Check for HTTP errors
                    data = response.json()

                    # Extract features from the current page
                    if "features" in data and data["features"]:
                        all_features.extend(data["features"])

                    # Look for the "next" link for the next page
                    next_url = None
                    if "links" in data:
                        for link in data["links"]:
                            if link.get("rel") == "next":
                                next_url = link.get("href")
                                break

                except httpx.HTTPError as e:
                    logger.error(f"HTTP error fetching {next_url}: {e}")
                    raise

                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    raise

        # Create a combined response with all features
        if all_features:
            # Maintain the structure of the original response
            result = data.copy()
            result["features"] = all_features
            result["numberMatched"] = len(all_features)
            result["numberReturned"] = len(all_features)
            return result
        else:
            return data

    async def get_href_list(self, href_list: list[str]) -> list[dict]:
        """
        Fetch a list of URLs asynchronously.
        Args:
            href_list (list[str]): A list of URLs to fetch.
        Returns:
            list[dict]: A list of JSON responses containing the feature data.
        """
        return await asyncio.gather(*[self._get_async(href) for href in href_list])

    @staticmethod
    def process_api_responses(response_list: list[dict]) -> list[list[dict]]:
        """
        Process a list of API responses to extract feature properties.
        Args:
            response_list (list[dict]): A list of API responses.
        Returns:
            list[list[dict]]: A list of lists containing feature properties.
        """
        properties = []
        for feature_dict in response_list:
            feature_list = feature_dict["features"]
            feature_properties = []
            for item in feature_list:
                item_properties = item["properties"]

                item_properties[FIELD.feature_id] = item["id"]  # need to check if this works outside processing samples
                # i needed bc it does not come with a identifikasjon.lokalId in the properties, but it is needed for processing samples

                feature_properties.append(item_properties)

            properties.append(feature_properties)
        return properties
