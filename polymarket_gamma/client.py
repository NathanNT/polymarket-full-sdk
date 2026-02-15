from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests
from dotenv import load_dotenv
from os import getenv


class GammaAPIError(Exception):
    """Raised when the Gamma API returns an error response."""

    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        self.status_code = status_code
        self.payload = payload
        super().__init__(message)


class GammaClient:
    """Simple client for the Polymarket Gamma REST API."""

    DEFAULT_BASE_URL = "https://gamma-api.polymarket.com"
    DEFAULT_TIMEOUT = 20.0

    def __init__(
        self,
        base_url: Optional[str] = None,
        timeout: Optional[float] = None,
        api_key: Optional[str] = None,
        dotenv_path: Optional[str] = None,
    ) -> None:
        root_env = Path(__file__).resolve().parents[1] / ".env"
        load_dotenv(dotenv_path=dotenv_path or root_env, override=False)

        self.base_url = (base_url or getenv("POLYMARKET_GAMMA_BASE_URL") or self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = float(timeout or getenv("POLYMARKET_GAMMA_TIMEOUT") or self.DEFAULT_TIMEOUT)
        self.api_key = api_key or getenv("POLYMARKET_GAMMA_API_KEY")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None, expect_json: bool = True) -> Any:
        url = f"{self.base_url}{path}"
        try:
            response = self.session.get(url, params=self._clean_params(params), timeout=self.timeout)
        except requests.RequestException as exc:
            raise GammaAPIError(f"Network error while calling {url}: {exc}") from exc

        if response.status_code >= 400:
            payload: Any
            try:
                payload = response.json()
            except ValueError:
                payload = response.text
            raise GammaAPIError(
                f"Gamma API error {response.status_code} on {path}",
                status_code=response.status_code,
                payload=payload,
            )

        if not expect_json:
            return response.text

        try:
            return response.json()
        except ValueError as exc:
            raise GammaAPIError(f"Expected JSON response for {path}, received non-JSON payload.") from exc

    @staticmethod
    def _clean_params(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not params:
            return {}
        return {k: v for k, v in params.items() if v is not None}

    def paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all records for paginated endpoints using limit/offset."""
        if page_size <= 0:
            raise ValueError("page_size must be > 0")

        merged = dict(params or {})
        merged["limit"] = page_size
        merged.setdefault("offset", 0)

        all_items: List[Dict[str, Any]] = []
        pages = 0

        while True:
            batch = self._request(path, params=merged, expect_json=True)
            if not isinstance(batch, list):
                raise GammaAPIError(f"Paginated endpoint {path} did not return a list.", payload=batch)
            all_items.extend(batch)

            pages += 1
            if len(batch) < page_size:
                break
            if max_pages is not None and pages >= max_pages:
                break

            merged["offset"] = int(merged.get("offset", 0)) + page_size

        return all_items

    def iter_paginate(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        page_size: int = 100,
    ) -> Iterable[List[Dict[str, Any]]]:
        """Yield paginated results page-by-page."""
        merged = dict(params or {})
        merged["limit"] = page_size
        merged.setdefault("offset", 0)

        while True:
            batch = self._request(path, params=merged, expect_json=True)
            if not isinstance(batch, list):
                raise GammaAPIError(f"Paginated endpoint {path} did not return a list.", payload=batch)
            yield batch
            if len(batch) < page_size:
                break
            merged["offset"] = int(merged.get("offset", 0)) + page_size

    # Health
    def get_status(self) -> str:
        return self._request("/status", expect_json=False)

    # Teams
    def list_teams(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/teams", params=params, expect_json=True)

    # Tags
    def list_tags(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/tags", params=params, expect_json=True)

    def get_tag(self, tag_id: int, include_template: Optional[bool] = None) -> Dict[str, Any]:
        return self._request(f"/tags/{tag_id}", params={"include_template": include_template}, expect_json=True)

    def get_tag_by_slug(self, slug: str, include_template: Optional[bool] = None) -> Dict[str, Any]:
        return self._request(f"/tags/slug/{slug}", params={"include_template": include_template}, expect_json=True)

    def get_related_tags_by_id(
        self,
        tag_id: int,
        omit_empty: Optional[bool] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._request(
            f"/tags/{tag_id}/related-tags",
            params={"omit_empty": omit_empty, "status": status},
            expect_json=True,
        )

    def get_related_tags_by_slug(
        self,
        slug: str,
        omit_empty: Optional[bool] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._request(
            f"/tags/slug/{slug}/related-tags",
            params={"omit_empty": omit_empty, "status": status},
            expect_json=True,
        )

    def get_tags_related_to_tag_by_id(
        self,
        tag_id: int,
        omit_empty: Optional[bool] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._request(
            f"/tags/{tag_id}/related-tags/tags",
            params={"omit_empty": omit_empty, "status": status},
            expect_json=True,
        )

    def get_tags_related_to_tag_by_slug(
        self,
        slug: str,
        omit_empty: Optional[bool] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return self._request(
            f"/tags/slug/{slug}/related-tags/tags",
            params={"omit_empty": omit_empty, "status": status},
            expect_json=True,
        )

    # Events
    def list_events(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/events", params=params, expect_json=True)

    def list_all_events(self, page_size: int = 100, **params: Any) -> List[Dict[str, Any]]:
        return self.paginate("/events", params=params, page_size=page_size)

    def get_event(self, event_id: int, include_chat: Optional[bool] = None, include_template: Optional[bool] = None) -> Dict[str, Any]:
        return self._request(
            f"/events/{event_id}",
            params={"include_chat": include_chat, "include_template": include_template},
            expect_json=True,
        )

    def get_event_by_slug(
        self, slug: str, include_chat: Optional[bool] = None, include_template: Optional[bool] = None
    ) -> Dict[str, Any]:
        return self._request(
            f"/events/slug/{slug}",
            params={"include_chat": include_chat, "include_template": include_template},
            expect_json=True,
        )

    def get_event_tags(self, event_id: int) -> List[Dict[str, Any]]:
        return self._request(f"/events/{event_id}/tags", expect_json=True)

    # Markets
    def list_markets(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/markets", params=params, expect_json=True)

    def list_all_markets(self, page_size: int = 100, **params: Any) -> List[Dict[str, Any]]:
        return self.paginate("/markets", params=params, page_size=page_size)

    def get_market(self, market_id: int, include_tag: Optional[bool] = None) -> Dict[str, Any]:
        return self._request(f"/markets/{market_id}", params={"include_tag": include_tag}, expect_json=True)

    def get_market_by_slug(self, slug: str, include_tag: Optional[bool] = None) -> Dict[str, Any]:
        return self._request(f"/markets/slug/{slug}", params={"include_tag": include_tag}, expect_json=True)

    def get_market_tags(self, market_id: int) -> List[Dict[str, Any]]:
        return self._request(f"/markets/{market_id}/tags", expect_json=True)

    # Series
    def list_series(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/series", params=params, expect_json=True)

    def list_all_series(self, page_size: int = 100, **params: Any) -> List[Dict[str, Any]]:
        return self.paginate("/series", params=params, page_size=page_size)

    def get_series(self, series_id: int, include_chat: Optional[bool] = None) -> Dict[str, Any]:
        return self._request(f"/series/{series_id}", params={"include_chat": include_chat}, expect_json=True)

    # Profiles
    def get_public_profile(self, address: str) -> Dict[str, Any]:
        return self._request("/public-profile", params={"address": address}, expect_json=True)

    # Search
    def public_search(self, q: str, **params: Any) -> Dict[str, Any]:
        merged = dict(params)
        merged["q"] = q
        return self._request("/public-search", params=merged, expect_json=True)
