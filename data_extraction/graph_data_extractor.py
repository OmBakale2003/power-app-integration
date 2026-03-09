import json
import logging
import time
from pathlib import Path
import requests
from auth import get_graph_token

logger = logging.getLogger(__name__)

DELTA_LINK_FILE = Path(".delta_links.json")
RETRYABLE_CODES = {500, 502, 503, 504}
MAX_RETRIES = 3


class GraphDataExtractor:
    def __init__(self):
        self.base_url = "https://graph.microsoft.com/v1.0/"
        self._delta_links: dict[str, str] = self._load_delta_links()

    # Delta link persistence

    def _load_delta_links(self) -> dict[str, str]:
        if DELTA_LINK_FILE.exists():
            try:
                with open(DELTA_LINK_FILE, "r") as f:
                    links = json.load(f)
                logger.info(f"Loaded {len(links)} delta link(s) from disk")
                return links
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Could not load delta links: {e} — starting fresh")
        return {}

    def _persist_delta_links(self):
        try:
            with open(DELTA_LINK_FILE, "w") as f:
                json.dump(self._delta_links, f, indent=2)
            logger.debug("Delta links persisted to disk")
        except IOError as e:
            logger.error(f"Failed to persist delta links: {e}")

    def save_delta_link(self, endpoint_key: str, delta_link: str):
        """
        Called by pipeline AFTER successful load.
        Only persists delta link once data is safely in DB.
        """
        self._delta_links[endpoint_key] = delta_link
        self._persist_delta_links()
        logger.info(f"[{endpoint_key}] Delta link saved — next run will be incremental")

    def clear_delta_link(self, endpoint_key: str):
        """Force full sync on next run for this endpoint."""
        if endpoint_key in self._delta_links:
            del self._delta_links[endpoint_key]
            self._persist_delta_links()
            logger.info(
                f"[{endpoint_key}] Delta link cleared — next run will be full sync"
            )

    # URL builder

    def _build_url(self, append_url: str) -> str:
        if append_url.startswith("https://"):
            return append_url
        return self.base_url + append_url.lstrip("/")

    # Core extraction

    def extract(
        self,
        append_url: str,
        endpoint_key: str,
        page_limit: int | None = None,
    ) -> tuple[list[dict], str | None]:
        """
        Extracts all pages from a Graph API endpoint.
        Uses stored delta link for incremental sync if available.

        Args:
            append_url:   Graph API path e.g. "users/delta"
            endpoint_key: Short stable key for delta link storage e.g. "users"
            page_limit:   Max pages to fetch (None = all pages)

        Returns:
            tuple of (records, new_delta_link)
            new_delta_link is None if page_limit was hit before end of dataset.
            Caller must call save_delta_link() after successful load.
        """
        if endpoint_key in self._delta_links:
            url = self._delta_links[endpoint_key]
            logger.info(f"[{endpoint_key}] Incremental sync using stored delta link")
        else:
            url = self._build_url(append_url)
            logger.info(f"[{endpoint_key}] Full sync — no delta link found")

        all_results: list[dict] = []
        total_records = 0
        page_count = 0
        new_delta_link: str | None = None

        while url:
            if page_limit is not None and page_count >= page_limit:
                logger.info(
                    f"[{endpoint_key}] Page limit ({page_limit}) reached — stopping"
                )
                break

            logger.info(
                f"[{endpoint_key}] Fetching page {page_count + 1}"
                + (f"/{page_limit}" if page_limit else "")
            )

            token = get_graph_token()
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

            data = self._fetch_with_retry(url, headers, endpoint_key)

            batch: list[dict] = data.get("value", [])
            total_records += len(batch)
            page_count += 1

            logger.info(
                f"[{endpoint_key}] Page {page_count}: "
                f"{len(batch)} records (total: {total_records})"
            )

            all_results.extend(batch)

            # Pagination — move to next page
            url = data.get("@odata.nextLink")
            if url:
                logger.debug(f"[{endpoint_key}] Next page found")

            # Capture delta link — DO NOT save here, returned to caller
            if "@odata.deltaLink" in data:
                new_delta_link = data["@odata.deltaLink"]
                logger.info(
                    f"[{endpoint_key}] Delta link received — "
                    f"will be saved after successful load"
                )

        logger.info(
            f"[{endpoint_key}] Extraction complete — "
            f"{total_records} records across {page_count} page(s)"
        )

        return all_results, new_delta_link

    # HTTP with retry + backoff

    def _fetch_with_retry(self, url: str, headers: dict, endpoint_key: str) -> dict:
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                resp = requests.get(url, headers=headers, timeout=30)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    logger.warning(
                        f"[{endpoint_key}] Rate limited (429) — "
                        f"retrying after {retry_after}s "
                        f"(attempt {attempt}/{MAX_RETRIES})"
                    )
                    time.sleep(retry_after)
                    continue

                if resp.status_code in RETRYABLE_CODES:
                    wait = 2**attempt  # 2s, 4s, 8s
                    logger.warning(
                        f"[{endpoint_key}] HTTP {resp.status_code} — "
                        f"retrying after {wait}s "
                        f"(attempt {attempt}/{MAX_RETRIES})"
                    )
                    time.sleep(wait)
                    continue

                if resp.status_code == 410:
                    logger.warning(
                        f"[{endpoint_key}] Delta token expired (410) — "
                        f"clearing stored link, falling back to full sync"
                    )
                    self.clear_delta_link(endpoint_key)
                    raise DeltaTokenExpiredError(endpoint_key)

                resp.raise_for_status()
                return resp.json()

            except requests.exceptions.Timeout:
                logger.error(
                    f"[{endpoint_key}] Request timed out "
                    f"(attempt {attempt}/{MAX_RETRIES})"
                )
                if attempt >= MAX_RETRIES:
                    raise
                time.sleep(2**attempt)

            except DeltaTokenExpiredError:
                raise

            except requests.exceptions.RequestException as e:
                logger.error(f"[{endpoint_key}] Request failed: {e}")
                raise

        raise RuntimeError(
            f"[{endpoint_key}] Max retries ({MAX_RETRIES}) exceeded for {url}"
        )


class DeltaTokenExpiredError(Exception):
    """Raised when Graph API returns 410 — delta token is stale."""

    def __init__(self, endpoint_key: str):
        self.endpoint_key = endpoint_key
        super().__init__(f"Delta token expired for endpoint: {endpoint_key}")
