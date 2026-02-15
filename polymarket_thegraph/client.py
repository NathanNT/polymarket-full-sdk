from __future__ import annotations

from os import getenv
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv


class TheGraphError(Exception):
    """Raised when The Graph API returns an error response."""

    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        self.status_code = status_code
        self.payload = payload
        super().__init__(message)


class TheGraphClient:
    """Simple GraphQL client for Polymarket subgraph on The Graph."""

    DEFAULT_BASE_URL = "https://gateway.thegraph.com/api"
    DEFAULT_SUBGRAPH_ID = "81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC"
    DEFAULT_TIMEOUT = 20.0

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        subgraph_id: Optional[str] = None,
        timeout: Optional[float] = None,
        dotenv_path: Optional[str] = None,
    ) -> None:
        root_env = Path(__file__).resolve().parents[1] / ".env"
        load_dotenv(dotenv_path=dotenv_path or root_env, override=False)

        self.api_key = api_key or getenv("POLYMARKET_THEGRAPH_API_KEY") or getenv("THEGRAPH_API_KEY")
        self.base_url = (base_url or getenv("POLYMARKET_THEGRAPH_BASE_URL") or self.DEFAULT_BASE_URL).rstrip("/")
        self.subgraph_id = subgraph_id or getenv("POLYMARKET_THEGRAPH_SUBGRAPH_ID") or self.DEFAULT_SUBGRAPH_ID
        self.timeout = float(timeout or getenv("POLYMARKET_THEGRAPH_TIMEOUT") or self.DEFAULT_TIMEOUT)

        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    @property
    def query_url(self) -> str:
        return f"{self.base_url}/subgraphs/id/{self.subgraph_id}"

    def query(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        operation_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"query": query, "variables": variables or {}}
        if operation_name:
            payload["operationName"] = operation_name

        try:
            response = self.session.post(self.query_url, json=payload, timeout=self.timeout)
        except requests.RequestException as exc:
            raise TheGraphError(f"Network error while calling {self.query_url}: {exc}") from exc

        if response.status_code >= 400:
            try:
                error_payload: Any = response.json()
            except ValueError:
                error_payload = response.text
            raise TheGraphError(
                f"The Graph API error {response.status_code}",
                status_code=response.status_code,
                payload=error_payload,
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise TheGraphError("The Graph returned non-JSON payload.") from exc

        if isinstance(data, dict) and data.get("errors"):
            raise TheGraphError("GraphQL error returned by The Graph.", payload=data["errors"])

        return data

    def query_polymarket_overview(self, first: int = 5) -> Dict[str, Any]:
        q = """
        query PolymarketOverview($first: Int!) {
          globals(first: $first) {
            id
            numConditions
            numOpenConditions
            numClosedConditions
          }
          accounts(first: $first) {
            id
            creationTimestamp
            lastSeenTimestamp
            collateralVolume
          }
        }
        """
        return self.query(q, variables={"first": first}, operation_name="PolymarketOverview")
