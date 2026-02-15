from __future__ import annotations

from os import getenv
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv


class GoldskyError(Exception):
    """Raised when Goldsky returns an error response."""

    def __init__(self, message: str, status_code: Optional[int] = None, payload: Any = None):
        self.status_code = status_code
        self.payload = payload
        super().__init__(message)


class GoldskyClient:
    """GraphQL client for a Goldsky subgraph deployment."""

    DEFAULT_BASE_URL = "https://api.goldsky.com"
    DEFAULT_TIMEOUT = 20.0
    DEFAULT_PROJECT_ID = "project_cl6mb8i9h0003e201j6li0diw"
    DEFAULT_SUBGRAPH_NAME = "orderbook-subgraph"
    DEFAULT_VERSION = "0.0.1"

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        project_id: Optional[str] = None,
        subgraph_name: Optional[str] = None,
        version: Optional[str] = None,
        timeout: Optional[float] = None,
        dotenv_path: Optional[str] = None,
    ) -> None:
        root_env = Path(__file__).resolve().parents[1] / ".env"
        load_dotenv(dotenv_path=dotenv_path or root_env, override=False)

        self.api_key = api_key or getenv("POLYMARKET_GOLDSKY_API_KEY")
        self.base_url = (base_url or getenv("POLYMARKET_GOLDSKY_BASE_URL") or self.DEFAULT_BASE_URL).rstrip("/")
        self.project_id = project_id or getenv("POLYMARKET_GOLDSKY_PROJECT_ID") or self.DEFAULT_PROJECT_ID
        self.subgraph_name = subgraph_name or getenv("POLYMARKET_GOLDSKY_SUBGRAPH_NAME") or self.DEFAULT_SUBGRAPH_NAME
        self.version = version or getenv("POLYMARKET_GOLDSKY_VERSION") or self.DEFAULT_VERSION
        self.endpoint_url = getenv("POLYMARKET_GOLDSKY_ENDPOINT_URL")
        self.timeout = float(timeout or getenv("POLYMARKET_GOLDSKY_TIMEOUT") or self.DEFAULT_TIMEOUT)

        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
        if self.api_key:
            self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    @property
    def query_url(self) -> str:
        if self.endpoint_url:
            return self.endpoint_url.strip().rstrip("/")
        # Public endpoint for shared datasets/subgraphs.
        return (
            f"{self.base_url}/api/public/{self.project_id}/subgraphs/"
            f"{self.subgraph_name}/{self.version}/gn"
        )

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
            raise GoldskyError(f"Network error while calling {self.query_url}: {exc}") from exc

        if response.status_code >= 400:
            try:
                error_payload: Any = response.json()
            except ValueError:
                error_payload = response.text
            raise GoldskyError(
                f"Goldsky API error {response.status_code}",
                status_code=response.status_code,
                payload=error_payload,
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise GoldskyError("Goldsky returned non-JSON payload.") from exc

        if isinstance(data, dict) and data.get("errors"):
            raise GoldskyError("GraphQL error returned by Goldsky.", payload=data["errors"])

        return data

    def fetch_order_filled_events(
        self,
        token_ids: List[str],
        first: int = 1000,
        max_pages: int = 200,
    ) -> List[Dict[str, Any]]:
        if first <= 0:
            raise ValueError("first must be > 0")
        if max_pages <= 0:
            raise ValueError("max_pages must be > 0")

        q = """
        query Fills($ids: [String!], $first: Int!, $skip: Int!) {
          orderFilledEvents(
            first: $first
            skip: $skip
            orderBy: timestamp
            orderDirection: desc
            where: { or: [{ makerAssetId_in: $ids }, { takerAssetId_in: $ids }] }
          ) {
            id
            timestamp
            makerAssetId
            takerAssetId
            makerAmountFilled
            takerAmountFilled
            fee
            maker { id }
            taker { id }
          }
        }
        """

        rows: List[Dict[str, Any]] = []
        skip = 0

        for _ in range(max_pages):
            payload = self.query(
                query=q,
                variables={"ids": token_ids, "first": first, "skip": skip},
                operation_name="Fills",
            )
            batch = payload.get("data", {}).get("orderFilledEvents", [])
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < first:
                break
            skip += first

        return rows
