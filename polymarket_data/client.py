from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zipfile import ZipFile

import requests
from dotenv import load_dotenv
from os import getenv


class DataAPIError(Exception):
    """Raised when the Data API returns an error response."""

    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        self.status_code = status_code
        self.payload = payload
        super().__init__(message)


class DataClient:
    """Simple client for the Polymarket Data API."""

    DEFAULT_BASE_URL = "https://data-api.polymarket.com"
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

        self.base_url = (base_url or getenv("POLYMARKET_DATA_BASE_URL") or self.DEFAULT_BASE_URL).rstrip("/")
        self.timeout = float(timeout or getenv("POLYMARKET_DATA_TIMEOUT") or self.DEFAULT_TIMEOUT)
        self.api_key = api_key or getenv("POLYMARKET_DATA_API_KEY")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def _request(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        expect_json: bool = True,
        accept: Optional[str] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        headers: Dict[str, str] = {}
        if accept:
            headers["Accept"] = accept

        try:
            response = self.session.get(
                url,
                params=self._clean_params(params),
                timeout=self.timeout,
                headers=headers or None,
            )
        except requests.RequestException as exc:
            raise DataAPIError(f"Network error while calling {url}: {exc}") from exc

        if response.status_code >= 400:
            payload: Any
            try:
                payload = response.json()
            except ValueError:
                payload = response.text
            raise DataAPIError(
                f"Data API error {response.status_code} on {path}",
                status_code=response.status_code,
                payload=payload,
            )

        if not expect_json:
            return response.content

        try:
            return response.json()
        except ValueError as exc:
            raise DataAPIError(f"Expected JSON response for {path}, received non-JSON payload.") from exc

    @staticmethod
    def _clean_params(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not params:
            return {}

        clean: Dict[str, Any] = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, (list, tuple, set)):
                clean[key] = ",".join(str(v) for v in value)
            else:
                clean[key] = value
        return clean

    # Status
    def get_health(self) -> Dict[str, Any]:
        return self._request("/", expect_json=True)

    # Misc
    def download_accounting_snapshot(self, user: str) -> bytes:
        return self._request(
            "/v1/accounting/snapshot",
            params={"user": user},
            expect_json=False,
            accept="application/zip",
        )

    def parse_accounting_snapshot(self, zip_bytes: bytes) -> Dict[str, List[Dict[str, str]]]:
        data: Dict[str, List[Dict[str, str]]] = {}
        with ZipFile(io.BytesIO(zip_bytes)) as zf:
            for name in ("positions.csv", "equity.csv"):
                if name in zf.namelist():
                    with zf.open(name) as f:
                        rows = list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))
                    data[name] = rows
        return data

    def get_traded(self, user: str) -> Dict[str, Any]:
        return self._request("/traded", params={"user": user}, expect_json=True)

    def get_open_interest(self, market: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        return self._request("/oi", params={"market": market}, expect_json=True)

    def get_live_volume(self, event_id: int) -> List[Dict[str, Any]]:
        return self._request("/live-volume", params={"id": event_id}, expect_json=True)

    # Core
    def get_positions(self, user: str, **params: Any) -> List[Dict[str, Any]]:
        merged = dict(params)
        merged["user"] = user
        return self._request("/positions", params=merged, expect_json=True)

    def get_trades(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/trades", params=params, expect_json=True)

    def get_activity(self, user: str, **params: Any) -> List[Dict[str, Any]]:
        merged = dict(params)
        merged["user"] = user
        return self._request("/activity", params=merged, expect_json=True)

    def get_holders(self, market: Iterable[str], **params: Any) -> List[Dict[str, Any]]:
        merged = dict(params)
        merged["market"] = market
        return self._request("/holders", params=merged, expect_json=True)

    def get_value(self, user: str, market: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        return self._request("/value", params={"user": user, "market": market}, expect_json=True)

    def get_closed_positions(self, user: str, **params: Any) -> List[Dict[str, Any]]:
        merged = dict(params)
        merged["user"] = user
        return self._request("/closed-positions", params=merged, expect_json=True)

    def get_leaderboard(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/v1/leaderboard", params=params, expect_json=True)

    # Builders
    def get_builders_leaderboard(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/v1/builders/leaderboard", params=params, expect_json=True)

    def get_builders_volume(self, **params: Any) -> List[Dict[str, Any]]:
        return self._request("/v1/builders/volume", params=params, expect_json=True)
