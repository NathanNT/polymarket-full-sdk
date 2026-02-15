from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv

from polymarket_gamma import GammaClient
from polymarket_goldsky import GoldskyClient, GoldskyError


TOKEN_DECIMALS = 6


def _extract_slug(market_url: str) -> tuple[str, str]:
    parsed = urlparse(market_url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        raise ValueError("URL de marche invalide. Format attendu: /event/{slug} ou /market/{slug}")
    if parts[0] not in {"event", "market"}:
        raise ValueError("URL invalide: le chemin doit commencer par /event/ ou /market/")
    return parts[0], parts[1]


def _parse_clob_token_ids(raw: object) -> List[str]:
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


def _to_float_opt(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _market_targets_from_url(gamma: GammaClient, market_url: str) -> List[Dict[str, object]]:
    kind, slug = _extract_slug(market_url)
    markets: List[Dict[str, object]]
    if kind == "market":
        markets = [gamma.get_market_by_slug(slug)]
    else:
        event = gamma.get_event_by_slug(slug)
        markets = event.get("markets") or []

    targets: List[Dict[str, object]] = []
    for m in markets:
        condition_id = m.get("conditionId")
        label = m.get("question") or m.get("slug") or condition_id
        token_ids = _parse_clob_token_ids(m.get("clobTokenIds"))
        if not condition_id or not token_ids:
            continue
        targets.append(
            {
                "condition_id": str(condition_id),
                "label": str(label),
                "token_ids": token_ids,
                "ui_volume": _to_float_opt(m.get("volumeNum")) or _to_float_opt(m.get("volume")),
            }
        )

    if not targets:
        raise ValueError("Aucun marche exploitable trouve (conditionId/clobTokenIds manquants).")
    return targets


def _to_decimal(value: object, scale: int = 0) -> Decimal:
    if value is None:
        return Decimal(0)
    try:
        v = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal(0)
    if scale:
        v = v / (Decimal(10) ** scale)
    return v


def _to_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _actor_id(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dict):
        actor = value.get("id")
        return str(actor) if actor else None
    return str(value)


def _kpis_from_fills(rows: List[Dict], token_ids: List[str]) -> Dict[str, object]:
    token_set = set(token_ids)

    trade_count = 0
    buy_count = 0
    sell_count = 0
    total_tokens = Decimal(0)
    total_usdc = Decimal(0)
    prices: List[Decimal] = []
    wallets = set()
    timestamps: List[int] = []

    for r in rows:
        maker_asset = str(r.get("makerAssetId") or "")
        taker_asset = str(r.get("takerAssetId") or "")
        maker_amt = _to_decimal(r.get("makerAmountFilled"), TOKEN_DECIMALS)
        taker_amt = _to_decimal(r.get("takerAmountFilled"), TOKEN_DECIMALS)

        if maker_asset in token_set and taker_asset not in token_set:
            token_amt = maker_amt
            usdc_amt = taker_amt
            side = "SELL"
        elif taker_asset in token_set and maker_asset not in token_set:
            token_amt = taker_amt
            usdc_amt = maker_amt
            side = "BUY"
        else:
            continue

        trade_count += 1
        if side == "BUY":
            buy_count += 1
        else:
            sell_count += 1

        total_tokens += token_amt
        total_usdc += usdc_amt
        if token_amt > 0:
            prices.append(usdc_amt / token_amt)

        maker = _actor_id(r.get("maker"))
        taker = _actor_id(r.get("taker"))
        if maker:
            wallets.add(maker)
        if taker:
            wallets.add(taker)

        ts = r.get("timestamp")
        if ts is not None:
            try:
                timestamps.append(int(ts))
            except ValueError:
                pass

    vwap = (total_usdc / total_tokens) if total_tokens > 0 else Decimal(0)
    return {
        "num_trades": trade_count,
        "buy_trades": buy_count,
        "sell_trades": sell_count,
        "unique_traders": len(wallets),
        "volume_tokens": total_tokens,
        "volume_usdc": total_usdc,
        "vwap": vwap,
        "min_price": min(prices) if prices else Decimal(0),
        "max_price": max(prices) if prices else Decimal(0),
        "min_ts": min(timestamps) if timestamps else None,
        "max_ts": max(timestamps) if timestamps else None,
    }


def _merge_kpis(all_k: List[Dict[str, object]]) -> Dict[str, object]:
    if not all_k:
        return {
            "num_trades": 0,
            "buy_trades": 0,
            "sell_trades": 0,
            "unique_traders": 0,
            "volume_tokens": Decimal(0),
            "volume_usdc": Decimal(0),
            "vwap": Decimal(0),
            "min_price": Decimal(0),
            "max_price": Decimal(0),
            "min_ts": None,
            "max_ts": None,
        }

    total_tokens = sum((Decimal(k["volume_tokens"]) for k in all_k), Decimal(0))
    total_usdc = sum((Decimal(k["volume_usdc"]) for k in all_k), Decimal(0))
    prices_min = [Decimal(k["min_price"]) for k in all_k if Decimal(k["min_price"]) > 0]
    prices_max = [Decimal(k["max_price"]) for k in all_k if Decimal(k["max_price"]) > 0]
    min_ts = [k["min_ts"] for k in all_k if k["min_ts"] is not None]
    max_ts = [k["max_ts"] for k in all_k if k["max_ts"] is not None]

    return {
        "num_trades": sum(int(k["num_trades"]) for k in all_k),
        "buy_trades": sum(int(k["buy_trades"]) for k in all_k),
        "sell_trades": sum(int(k["sell_trades"]) for k in all_k),
        "unique_traders": None,
        "volume_tokens": total_tokens,
        "volume_usdc": total_usdc,
        "vwap": (total_usdc / total_tokens) if total_tokens > 0 else Decimal(0),
        "min_price": min(prices_min) if prices_min else Decimal(0),
        "max_price": max(prices_max) if prices_max else Decimal(0),
        "min_ts": min(min_ts) if min_ts else None,
        "max_ts": max(max_ts) if max_ts else None,
    }


def _print_kpis(k: Dict[str, object]) -> None:
    print("=== KPI ===")
    print(f"Nombre de trades: {k['num_trades']}")
    print(f"Trades BUY: {k['buy_trades']}")
    print(f"Trades SELL: {k['sell_trades']}")
    if k.get("unique_traders") is not None:
        print(f"Traders uniques: {k['unique_traders']}")
    print(f"Volume tokens (size): {Decimal(k['volume_tokens']):,.4f}")
    print(f"Volume notionnel (USDC): {Decimal(k['volume_usdc']):,.4f}")
    print(f"Prix moyen pondere (VWAP): {Decimal(k['vwap']):.6f}")
    print(f"Prix min/max: {Decimal(k['min_price']):.6f} / {Decimal(k['max_price']):.6f}")
    if k["min_ts"] is not None:
        print(f"Premier trade (UTC): {_to_utc(int(k['min_ts']))}")
    if k["max_ts"] is not None:
        print(f"Dernier trade (UTC): {_to_utc(int(k['max_ts']))}")


def main() -> None:
    load_dotenv()
    market_url = os.getenv("POLYMARKET_TEST_MARKET_URL")
    page_size = int(os.getenv("POLYMARKET_GOLDSKY_PAGE_SIZE", "1000"))
    max_pages = int(os.getenv("POLYMARKET_GOLDSKY_MAX_PAGES", "200"))

    if not market_url:
        raise ValueError("POLYMARKET_TEST_MARKET_URL manquant dans .env")

    gamma = GammaClient()
    goldsky = GoldskyClient()

    targets = _market_targets_from_url(gamma, market_url)
    print(f"URL test: {market_url}")
    print(f"Nombre de marches detectes: {len(targets)}")
    print(f"Goldsky endpoint: {goldsky.query_url}")

    per_market_kpis: List[Dict[str, object]] = []

    for idx, t in enumerate(targets, start=1):
        cid = str(t["condition_id"])
        label = str(t["label"])
        token_ids = list(t["token_ids"])

        print(f"\n=== Marche {idx}/{len(targets)} ===")
        print(f"conditionId: {cid}")
        print(f"label: {label}")
        print(f"tokenIds: {len(token_ids)}")

        try:
            rows = goldsky.fetch_order_filled_events(
                token_ids=token_ids,
                first=page_size,
                max_pages=max_pages,
            )
        except GoldskyError as exc:
            print(f"Marche ignore: erreur Goldsky ({exc.payload})")
            continue

        k = _kpis_from_fills(rows, token_ids)
        _print_kpis(k)
        ui_vol = t.get("ui_volume")
        if ui_vol is not None:
            diff = float(Decimal(k["volume_usdc"]) - Decimal(str(ui_vol)))
            pct = (diff / ui_vol * 100) if ui_vol else 0.0
            print(f"UI volume (Gamma): {ui_vol:,.4f} | Ecart: {diff:,.4f} ({pct:+.2f}%)")
        per_market_kpis.append(k)

    print("\n=== Global (tous marches) ===")
    global_k = _merge_kpis(per_market_kpis)
    _print_kpis(global_k)
    ui_total = sum((t.get("ui_volume") or 0.0) for t in targets)
    if ui_total:
        gdiff = float(Decimal(global_k["volume_usdc"]) - Decimal(str(ui_total)))
        gpct = gdiff / ui_total * 100
        print(f"UI volume total (Gamma): {ui_total:,.4f} | Ecart: {gdiff:,.4f} ({gpct:+.2f}%)")


if __name__ == "__main__":
    main()
