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
    connect=settings.API_TIMEOUT,
    read=settings.API_TIMEOUT,
    pool=60.0,
    write=10.0,
)

DEFAULT_CHUNK_SIZE = settings.API_MAX_CONCURRENCY

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
        retry=retry_if_exception_type(
            (
                ConnectionError,
                TimeoutError,
                httpx.ConnectTimeout,
                httpx.ReadTimeout,
                httpx.WriteTimeout,
                httpx.PoolTimeout,
                httpx.ConnectError,
                httpx.RemoteProtocolError,
            )
        ),
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

    async def get_features_from_urls(self, urls: list[str], chunk_size: int = DEFAULT_CHUNK_SIZE) -> list[dict]:
        """
        Fetch features from a list of URLs asynchronously in chunks to avoid server saturation.
        Raises an error if any URL fails after retries.

        Args:
            urls (list[str]): A list of URLs to fetch features from.
            chunk_size (int): Number of concurrent requests per chunk.
        Returns:
            list[dict]: A list of JSON responses containing the feature data.
        Raises:
            RuntimeError: If any URLs fail after all retry attempts.
        """
        all_results = []
        failed_urls = []

        for i in range(0, len(urls), chunk_size):
            chunk = urls[i : i + chunk_size]
            logger.debug(f"Fetching chunk {i // chunk_size + 1}/{-(-len(urls) // chunk_size)} ({len(chunk)} URLs)")

            results = await asyncio.gather(
                *[self.get_feature(url) for url in chunk],
                return_exceptions=True,
            )

            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_urls.append((chunk[j], result))
                else:
                    all_results.append(result)

        if failed_urls:
            logger.warning(
                f"{len(failed_urls)}/{len(urls)} URLs failed after retries. "
                f"First failure: {failed_urls[0][0]} -> {failed_urls[0][1]}"
            )
            raise RuntimeError(
                f"{len(failed_urls)}/{len(urls)} URL requests failed after retries. "
                f"Failed URLs: {[url for url, _ in failed_urls[:5]]}{'...' if len(failed_urls) > 5 else ''}"
            )

        return all_results

    async def get_features_from_urls_stream(self, urls: list[str]):
        """
        Yield features from a list of URLs asynchronously as they complete.
        Args:
            urls (list[str]): A list of URLs to fetch features from.
        Yields:
            dict: The JSON response containing the feature data.
        """

        @api_retry()
        async def fetch(client: httpx.AsyncClient, url: str):
            async with self.semaphore:
                response = await client.get(clean_url(url))
                response.raise_for_status()
                return response.json()

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            tasks = [fetch(client, url) for url in urls]
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    yield result
                except Exception as e:
                    logger.error(f"Failed to fetch URL after retries: {e}")
                    continue

    async def get_features_paginated(
        self,
        url: str,
        params: Optional[dict] = None,
        page_size: int = 100,
    ) -> AsyncGenerator[PaginatedResponse, None]:
        """
        Fetch features from a collection in a paginated manner.
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

                    data = await self._fetch_page(client, next_url, current_params)
                    yield PaginatedResponse(**data)

                    next_link = next(
                        (link["href"] for link in data.get("links", []) if link.get("rel") == "next"),
                        None,
                    )
                    next_url = next_link if next_link else None

    @api_retry()
    async def _fetch_page(self, client: httpx.AsyncClient, url: str, params: Optional[dict] = None) -> dict:
        """Fetch a single page with retry support."""
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @api_retry()
    async def _get_async(self, href: str, params: dict | None = None) -> dict | None:
        """
        Fetch a single URL and handle pagination if necessary.
        This method is used internally by get_href_list to fetch each URL in the list, and it will handle pagination
        if the response includes a "next" link.

        Args:
            href (str): The URL to fetch.
            params (dict | None): Optional query parameters for the initial request.

        Returns:
            dict | None: The JSON response containing the feature data, or None if the URL is invalid.

        """
        if href is None:
            return None

        all_features = []
        next_url = href
        first_page = True

        async with self.semaphore:
            async with httpx.AsyncClient(timeout=TIMEOUT) as client:
                while next_url:
                    current_params = params if first_page else None
                    first_page = False

                    try:
                        response = await client.get(next_url, params=current_params)
                        response.raise_for_status()
                        data = response.json()

                        if "features" in data and data["features"]:
                            all_features.extend(data["features"])

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

        if all_features:
            result = data.copy()
            result["features"] = all_features
            result["numberMatched"] = len(all_features)
            result["numberReturned"] = len(all_features)
            return result
        else:
            return data

    async def get_href_list(self, href_list: list[str], chunk_size: int = DEFAULT_CHUNK_SIZE) -> list[dict]:
        """
        Fetch a list of URLs asynchronously in chunks.
        Raises an error if any URL fails after retries.

        Args:
            href_list (list[str]): A list of URLs to fetch.
            chunk_size (int): Number of concurrent requests per chunk.
        Returns:
            list[dict]: A list of JSON responses containing the feature data.
        Raises:
            RuntimeError: If any URLs fail after all retry attempts.
        """
        all_results = []
        failed_urls = []

        for i in range(0, len(href_list), chunk_size):
            chunk = href_list[i : i + chunk_size]
            logger.debug(
                f"Fetching href chunk {i // chunk_size + 1}/{-(-len(href_list) // chunk_size)} ({len(chunk)} URLs)"
            )

            results = await asyncio.gather(
                *[self._get_async(href) for href in chunk],
                return_exceptions=True,
            )

            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    failed_urls.append((chunk[j], result))
                else:
                    all_results.append(result)

        if failed_urls:
            raise RuntimeError(
                f"{len(failed_urls)}/{len(href_list)} href requests failed after retries. "
                f"Failed URLs: {[url for url, _ in failed_urls[:5]]}{'...' if len(failed_urls) > 5 else ''}"
            )

        return all_results

    @staticmethod
    def process_api_responses(response_list: list[dict]) -> list[list[dict]]:
        """
        Process a list of API responses to extract feature properties.
        """
        properties = []
        for feature_dict in response_list:
            if feature_dict is None or "features" not in feature_dict:
                logger.warning("Skipping invalid or empty API response.")
                continue
            feature_list = feature_dict["features"]
            feature_properties = []
            for item in feature_list:
                item_properties = item["properties"]
                item_properties[FIELD.feature_id] = item["id"]
                feature_properties.append(item_properties)

            properties.append(feature_properties)
        return properties
