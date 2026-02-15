from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from dotenv import load_dotenv

from polymarket_clob import ClobClientWrapper
from polymarket_gamma import GammaClient


def _extract_slug(market_url: str) -> Tuple[str, str]:
    parsed = urlparse(market_url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2 or parts[0] not in {"event", "market"}:
        raise ValueError("URL invalide. Attendu: /event/{slug} ou /market/{slug}")
    return parts[0], parts[1]


def _parse_token_ids(raw: object) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if x is not None]
    s = str(raw).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if x is not None]
    except Exception:
        pass
    return [x.strip().strip('"').strip("'") for x in s.strip("[]").split(",") if x.strip()]


def _to_decimal(value: object) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)


def _to_ts(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        v = int(value)
        # ms -> s fallback
        if v > 10_000_000_000:
            return v // 1000
        return v
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return _to_ts(int(s))
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return int(dt.astimezone(timezone.utc).timestamp())
    except Exception:
        return None


def _market_targets(gamma: GammaClient, url: str, wanted_condition: Optional[str]) -> List[Dict[str, Any]]:
    kind, slug = _extract_slug(url)
    if kind == "market":
        markets = [gamma.get_market_by_slug(slug)]
    else:
        event = gamma.get_event_by_slug(slug)
        markets = event.get("markets") or []

    out: List[Dict[str, Any]] = []
    for m in markets:
        cid = str(m.get("conditionId") or "")
        token_ids = _parse_token_ids(m.get("clobTokenIds"))
        if not cid or not token_ids:
            continue
        if wanted_condition and cid.lower() != wanted_condition.lower():
            continue
        out.append(
            {
                "condition_id": cid,
                "label": str(m.get("question") or m.get("slug") or cid),
                "ui_volume": _to_decimal(m.get("volumeNum") if m.get("volumeNum") is not None else m.get("volume")),
            }
        )
    return out


def _trade_side(row: Dict[str, Any]) -> str:
    side = str(row.get("side") or row.get("taker_side") or row.get("type") or "").upper()
    if side in {"BUY", "SELL"}:
        return side
    return "UNKNOWN"


def _trade_size(row: Dict[str, Any]) -> Decimal:
    for key in ("size", "asset_amount", "filled_size", "amount"):
        if key in row:
            return _to_decimal(row.get(key))
    return Decimal(0)


def _trade_price(row: Dict[str, Any]) -> Decimal:
    for key in ("price", "executed_price", "avg_price"):
        if key in row:
            return _to_decimal(row.get(key))
    return Decimal(0)


def _trade_notional(row: Dict[str, Any]) -> Decimal:
    # Prefer explicit USD/USDC amount if present.
    for key in ("amount", "usdc_amount", "notional", "amount_usd", "amountUsd"):
        if key in row:
            v = _to_decimal(row.get(key))
            if v > 0:
                return v
    size = _trade_size(row)
    price = _trade_price(row)
    return size * price


def _trade_actor(row: Dict[str, Any]) -> Optional[str]:
    for key in ("maker_address", "taker_address", "trader", "owner", "user", "proxyWallet"):
        v = row.get(key)
        if v:
            return str(v).lower()
    return None


def _kpis_from_trades(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    trades = 0
    buy = 0
    sell = 0
    volume = Decimal(0)
    wallets = set()
    timestamps: List[int] = []

    for r in rows:
        trades += 1
        side = _trade_side(r)
        if side == "BUY":
            buy += 1
        elif side == "SELL":
            sell += 1
        volume += _trade_notional(r)
        actor = _trade_actor(r)
        if actor:
            wallets.add(actor)
        ts = _to_ts(r.get("timestamp") or r.get("match_time") or r.get("created_at"))
        if ts is not None:
            timestamps.append(ts)

    return {
        "trades": trades,
        "buy": buy,
        "sell": sell,
        "unique_wallets": len(wallets),
        "volume": volume,
        "min_ts": min(timestamps) if timestamps else None,
        "max_ts": max(timestamps) if timestamps else None,
    }


def _fmt_ts(ts: Optional[int]) -> str:
    if ts is None:
        return "n/a"
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def main() -> None:
    load_dotenv()
    url = os.getenv("POLYMARKET_TEST_MARKET_URL")
    wanted = os.getenv("POLYMARKET_COMPARE_MARKET_CONDITION_ID")
    force_all = (os.getenv("POLYMARKET_COMPARE_ALL_MARKETS") or "").strip().lower() in {"1", "true", "yes", "on"}
    if force_all:
        wanted = None

    before = os.getenv("POLYMARKET_CLOB_BEFORE")
    after = os.getenv("POLYMARKET_CLOB_AFTER")
    maker_address = os.getenv("POLYMARKET_CLOB_MAKER_ADDRESS")
    before_ts = int(before) if before and before.strip() else None
    after_ts = int(after) if after and after.strip() else None
    print_sample = (os.getenv("POLYMARKET_CLOB_PRINT_SAMPLE") or "false").strip().lower() in {"1", "true", "yes", "on"}

    if not url:
        raise ValueError("POLYMARKET_TEST_MARKET_URL manquant")

    gamma = GammaClient()
    clob = ClobClientWrapper()

    print("=== Test auth CLOB ===")
    now = int(time.time())
    test_before = before_ts if before_ts is not None else now
    test_after = after_ts if after_ts is not None else max(0, now - 60)
    try:
        _ = clob.get_trades(before=test_before, after=test_after)
        print("Auth CLOB: OK")
    except Exception as exc:
        raise RuntimeError(f"Auth CLOB echouee: {exc}") from exc

    # Diagnostic: /data/trades is user-centric for the authenticated account.
    try:
        own_rows = clob.get_trades()
        print(f"Trades comptes auth (all): {len(own_rows)}")
        if len(own_rows) == 0:
            print("Note: ce endpoint semble lister les trades du compte authentifie, pas le flux global du marche.")
    except Exception:
        pass

    markets = _market_targets(gamma, url, wanted)
    if not markets:
        raise ValueError("Aucun marche exploitable trouve")

    print(f"\nURL: {url}")
    print(f"Marches testes: {len(markets)}")
    if after_ts is not None or before_ts is not None:
        print(f"Filtre temps: after={after_ts} before={before_ts}")

    total_trades = 0
    total_volume = Decimal(0)
    total_ui = Decimal(0)

    for i, m in enumerate(markets, start=1):
        cid = str(m["condition_id"])
        label = str(m["label"])
        ui_volume = Decimal(m["ui_volume"])

        trades = clob.get_trades(
            market=cid,
            maker_address=maker_address,
            before=before_ts,
            after=after_ts,
        )
        k = _kpis_from_trades(trades)

        total_trades += int(k["trades"])
        total_volume += Decimal(k["volume"])
        total_ui += ui_volume

        delta = Decimal(k["volume"]) - ui_volume
        pct = (delta / ui_volume * 100) if ui_volume > 0 else Decimal(0)

        print(f"\n=== Marche {i}/{len(markets)} ===")
        print(f"conditionId: {cid}")
        print(f"label: {label}")
        print(f"Trades CLOB: {k['trades']}")
        print(f"BUY/SELL: {k['buy']}/{k['sell']}")
        print(f"Wallets uniques: {k['unique_wallets']}")
        print(f"Volume CLOB: {Decimal(k['volume']):,.6f}")
        print(f"Volume UI Gamma: {ui_volume:,.6f}")
        print(f"Delta: {delta:,.6f} ({pct:+.2f}%)")
        print(f"Premier trade: {_fmt_ts(k['min_ts'])}")
        print(f"Dernier trade: {_fmt_ts(k['max_ts'])}")
        if print_sample and trades:
            print("Sample row:")
            print(trades[0])

    g_delta = total_volume - total_ui
    g_pct = (g_delta / total_ui * 100) if total_ui > 0 else Decimal(0)

    print("\n=== Global ===")
    print(f"Trades CLOB: {total_trades}")
    print(f"Volume CLOB: {total_volume:,.6f}")
    print(f"Volume UI Gamma: {total_ui:,.6f}")
    print(f"Delta: {g_delta:,.6f} ({g_pct:+.2f}%)")


if __name__ == "__main__":
    main()
