from __future__ import annotations

from os import getenv
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds, TradeParams
except Exception as exc:  # pragma: no cover
    ClobClient = None  # type: ignore[assignment]
    ApiCreds = None  # type: ignore[assignment]
    TradeParams = None  # type: ignore[assignment]
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


class ClobAPIError(Exception):
    """Raised when CLOB client setup or request fails."""


class ClobClientWrapper:
    """Simple wrapper around py-clob-client for Polymarket CLOB trades."""

    DEFAULT_HOST = "https://clob.polymarket.com"
    DEFAULT_CHAIN_ID = 137

    def __init__(
        self,
        host: Optional[str] = None,
        chain_id: Optional[int] = None,
        private_key: Optional[str] = None,
        funder: Optional[str] = None,
        signature_type: Optional[int] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        derive_api_creds: Optional[bool] = None,
        dotenv_path: Optional[str] = None,
    ) -> None:
        if ClobClient is None:
            raise ClobAPIError(f"py-clob-client is required: {_IMPORT_ERROR}")

        root_env = Path(__file__).resolve().parents[1] / ".env"
        load_dotenv(dotenv_path=dotenv_path or root_env, override=False)

        self.host = (host or getenv("POLYMARKET_CLOB_HOST") or self.DEFAULT_HOST).rstrip("/")
        self.chain_id = int(chain_id or getenv("POLYMARKET_CLOB_CHAIN_ID") or self.DEFAULT_CHAIN_ID)
        self.private_key = private_key or getenv("POLYMARKET_CLOB_PRIVATE_KEY")
        self.funder = funder or getenv("POLYMARKET_CLOB_FUNDER")
        self.signature_type = int(signature_type or getenv("POLYMARKET_CLOB_SIGNATURE_TYPE") or 0)

        self.api_key = api_key or getenv("POLYMARKET_CLOB_API_KEY")
        self.api_secret = api_secret or getenv("POLYMARKET_CLOB_API_SECRET")
        self.api_passphrase = api_passphrase or getenv("POLYMARKET_CLOB_API_PASSPHRASE")

        if derive_api_creds is None:
            derive_api_creds = (getenv("POLYMARKET_CLOB_DERIVE_API_CREDS") or "false").strip().lower() in {
                "1",
                "true",
                "yes",
                "on",
            }
        self.derive_api_creds = bool(derive_api_creds)

        self.client = self._build_client()

    def _build_client(self) -> Any:
        # Option A: direct API creds
        if self.api_key and self.api_secret and self.api_passphrase:
            creds = ApiCreds(
                api_key=self.api_key,
                api_secret=self.api_secret,
                api_passphrase=self.api_passphrase,
            )
            return ClobClient(
                host=self.host,
                chain_id=self.chain_id,
                key=self.private_key,
                creds=creds,
                signature_type=self.signature_type,
                funder=self.funder,
            )

        # Option B: derive API creds from private key
        if self.derive_api_creds:
            if not self.private_key:
                raise ClobAPIError("POLYMARKET_CLOB_PRIVATE_KEY is required when derive_api_creds=true")
            c = ClobClient(
                host=self.host,
                chain_id=self.chain_id,
                key=self.private_key,
                signature_type=self.signature_type,
                funder=self.funder,
            )
            creds = c.create_or_derive_api_creds()
            c.set_api_creds(creds)
            return c

        raise ClobAPIError(
            "Missing CLOB credentials. Provide API creds (POLYMARKET_CLOB_API_KEY/SECRET/PASSPHRASE) "
            "or enable POLYMARKET_CLOB_DERIVE_API_CREDS=true with POLYMARKET_CLOB_PRIVATE_KEY."
        )

    @staticmethod
    def _trade_params(**kwargs: Any) -> Any:
        data: Dict[str, Any] = {}
        for field in ("id", "maker_address", "market", "asset_id", "before", "after"):
            v = kwargs.get(field)
            if v is not None:
                data[field] = v
        return TradeParams(**data)

    def get_trades(
        self,
        market: Optional[str] = None,
        maker_address: Optional[str] = None,
        asset_id: Optional[str] = None,
        before: Optional[int] = None,
        after: Optional[int] = None,
        trade_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        params = self._trade_params(
            id=trade_id,
            maker_address=maker_address,
            market=market,
            asset_id=asset_id,
            before=before,
            after=after,
        )
        return self.client.get_trades(params=params)

    def get_market_trades(
        self,
        market: str,
        before: Optional[int] = None,
        after: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return self.get_trades(market=market, before=before, after=after)
