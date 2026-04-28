import asyncio
import logging
from collections.abc import AsyncGenerator
from typing import Any
from urllib.parse import parse_qs, urlparse

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
from .utils import clean_url, safe_extract_features, safe_extract_properties, safe_first

TIMEOUT = httpx.Timeout(
    connect=settings.API_TIMEOUT,
    read=settings.API_TIMEOUT,
    pool=settings.API_POOL_TIMEOUT,
    write=settings.API_WRITE_TIMEOUT,
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
    """A client for making asynchronous HTTP requests to the NADAG API.

    Uses a shared ``httpx.AsyncClient`` for connection pooling (TCP + TLS reuse).
    Must be used as an async context manager::

        async with NadagHTTPClient() as client:
            data = await client.get_feature(url)
    """

    def __init__(
        self,
        base_url: str = settings.API_BASE_URL,
        max_concurrency: int = settings.API_MAX_CONCURRENCY,
    ):
        self.base_url = clean_url(base_url)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._client: httpx.AsyncClient | None = None
        self._owns_client = False

    async def __aenter__(self) -> "NadagHTTPClient":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=TIMEOUT)
            self._owns_client = True
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
            self._owns_client = False

    def _get_client(self) -> httpx.AsyncClient:
        """Return the shared client, creating one lazily if not in context manager."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=TIMEOUT)
            self._owns_client = True
        return self._client

    @property
    def query_url(self):
        return self.base_url + "/{collection}/items"

    @api_retry()
    async def check_api_status(self) -> bool:
        """Check the status of the NADAG API.

        Returns:
            bool: True if the API is reachable and responsive, False otherwise.
        """
        client = self._get_client()
        try:
            response = await client.get(self.base_url)
            return response.is_success

        except httpx.RequestError as e:
            logger.warning(f"API status check failed: {e}")
            return False

    def build_collection_url(self, collection: str, query_params: dict[str, Any] | None = None) -> str:
        """Build a URL for querying a collection with optional query parameters.

        Args:
            collection: The collection name
            query_params: Query parameters as key-value pairs

        Returns:
            The complete URL with query parameters

        Example:
            client.build_collection_url(
                "geotekniskborehullunders",
                {"underspkt_fk": "6d887f7b-5f3c-450c-8e4b-c038b912c170"}
                )
        """
        base = self.query_url.format(collection=collection)

        if query_params:
            url = httpx.URL(base, params=query_params)
            return str(url)

        return base

    @api_retry()
    async def get_feature(self, url: str) -> dict:
        """Fetch a single feature by its URL.

        Args:
            url: The URL of the feature to fetch.

        Returns:
            The JSON response containing the feature data.
        """
        url = clean_url(url)
        client = self._get_client()
        async with self.semaphore:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()

    async def get_features_from_urls(self, urls: list[str], chunk_size: int | None = None) -> list[dict]:
        """Fetch features from a list of URLs asynchronously.

        Raises an error if any URL fails after retries.

        Args:
            urls: A list of URLs to fetch features from.
            chunk_size: Deprecated and ignored. Concurrency is controlled by the client semaphore.

        Returns:
            A list of JSON responses containing the feature data.

        Raises:
            RuntimeError: If any URLs fail after all retry attempts.
        """
        all_results = []
        failed_urls = []

        results = await asyncio.gather(
            *[self.get_feature(url) for url in urls],
            return_exceptions=True,
        )

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_urls.append((urls[i], result))
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
        """Yield features from a list of URLs asynchronously as they complete.

        Args:
            urls: A list of URLs to fetch features from.

        Yields:
            The JSON response containing the feature data.
        """
        client = self._get_client()

        @api_retry()
        async def fetch(url: str):
            async with self.semaphore:
                response = await client.get(clean_url(url))
                response.raise_for_status()
                return response.json()

        tasks = [fetch(url) for url in urls]
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
        params: dict | None = None,
        page_size: int = settings.API_PAGE_SIZE,
        max_concurrency: int | None = None,
    ) -> AsyncGenerator[PaginatedResponse, None]:
        """Fetch features from a collection in a paginated manner.

        Uses concurrent offset-based pagination when available for better performance.

        Args:
            url: The API endpoint URL
            params: Query parameters
            page_size: Number of items per page
            max_concurrency: Max concurrent requests for offset-based pagination.
                           Defaults to API_MAX_CONCURRENCY if None.
        """
        logger.debug(f"API endpoint: {url}")
        params = params or {}
        params["limit"] = page_size
        url = clean_url(url)

        if max_concurrency is None:
            max_concurrency = settings.API_MAX_CONCURRENCY

        client = self._get_client()

        # Fetch first page
        async with self.semaphore:
            first_data = await self._fetch_page(client, url, params)
        yield PaginatedResponse(**first_data)

        # Check if there are more pages
        next_link = next(
            (
                link.get("href")
                for link in first_data.get("links", [])
                if link.get("rel") == "next" and link.get("href")
            ),
            None,
        )

        if not next_link:
            return

        # Try to detect offset-based pagination
        parsed_next_q = parse_qs(urlparse(next_link).query)
        offset_key = next((k for k in ("offset", "startindex", "startIndex") if k in parsed_next_q), None)
        number_matched = first_data.get("numberMatched")

        if offset_key and isinstance(number_matched, int):
            # Use concurrent offset-based pagination
            start_offset = int(safe_first(parsed_next_q.get(offset_key, []), 0))
            offsets = range(start_offset, number_matched, page_size)

            logger.debug(f"Using concurrent pagination: {len(list(offsets))} pages, concurrency={max_concurrency}")

            sem = asyncio.Semaphore(max_concurrency)

            async def fetch_page_offset(offset: int):
                async with sem:
                    page_params = {**params, offset_key: offset}
                    return await self._fetch_page(client, url, page_params)

            tasks = [asyncio.create_task(fetch_page_offset(off)) for off in offsets]
            for task in asyncio.as_completed(tasks):
                data = await task
                yield PaginatedResponse(**data)
        else:
            # Fallback to sequential next-link pagination
            logger.debug("Using sequential pagination (no offset parameter detected)")
            current_url = next_link
            while current_url:
                async with self.semaphore:
                    data = await self._fetch_page(client, current_url)
                yield PaginatedResponse(**data)
                current_url = next(
                    (
                        link.get("href")
                        for link in data.get("links", [])
                        if link.get("rel") == "next" and link.get("href")
                    ),
                    None,
                )

    @api_retry()
    async def _fetch_page(self, client: httpx.AsyncClient, url: str, params: dict | None = None) -> dict:
        """Fetch a single page with retry support."""
        response = await client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    @api_retry()
    async def _get_async(self, href: str, params: dict | None = None) -> dict | None:
        """Fetch a single URL and handle pagination if necessary.

        This method is used internally by get_href_list to fetch each URL in the list,
        and it will handle pagination if the response includes a "next" link.

        Args:
            href: The URL to fetch.
            params: Optional query parameters for the initial request.

        Returns:
            The JSON response containing the feature data, or None if the URL is invalid.
        """
        if href is None:
            return None

        all_features = []
        next_url = href
        first_page = True
        client = self._get_client()

        async with self.semaphore:
            while next_url:
                current_params = params if first_page else None
                first_page = False

                try:
                    response = await client.get(next_url, params=current_params)
                    response.raise_for_status()
                    data = response.json()

                    if data.get("features"):
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

    async def get_href_list(self, href_list: list[str], chunk_size: int | None = None) -> list[dict]:
        """Fetch a list of URLs asynchronously.

        Raises an error if any URL fails after retries.

        Args:
            href_list: A list of URLs to fetch.
            chunk_size: Deprecated and ignored. Concurrency is controlled by the client semaphore.

        Returns:
            A list of JSON responses containing the feature data.

        Raises:
            RuntimeError: If any URLs fail after all retry attempts.
        """
        all_results = []
        failed_urls = []

        results = await asyncio.gather(
            *[self._get_async(href) for href in href_list],
            return_exceptions=True,
        )

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                failed_urls.append((href_list[i], result))
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
        """Process a list of API responses to extract feature properties."""
        properties = []
        for feature_dict in response_list:
            if feature_dict is None:
                logger.warning("Skipping None API response.")
                continue
            feature_list = safe_extract_features(feature_dict)
            if not feature_list:
                logger.warning("Skipping API response with no valid features.")
                continue
            feature_properties = []
            for item in feature_list:
                item_properties = safe_extract_properties(item)
                if item_properties is None:
                    logger.debug("Skipping feature with missing/malformed properties.")
                    continue
                item_properties[FIELD.feature_id] = item.get("id")
                feature_properties.append(item_properties)

            properties.append(feature_properties)
        return properties
