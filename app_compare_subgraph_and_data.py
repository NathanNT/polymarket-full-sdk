from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv

from polymarket_gamma import GammaClient
from polymarket_data import DataClient
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


# -----------------------------------------------------------------------------
# KPIs from Goldsky (orderFilledEvents)
# -----------------------------------------------------------------------------
def _kpis_from_goldsky_fills(rows: List[Dict], token_ids: List[str]) -> Dict[str, object]:
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


# -----------------------------------------------------------------------------
# KPIs from Data API (/trades)
# -----------------------------------------------------------------------------
def _fetch_all_data_trades(
    data_client: DataClient,
    market: str,
    limit: int = 1000,
    max_offset: int = 10000,
) -> List[Dict[str, Any]]:
    """Fetch all trades for a market using offset-based pagination.

    Data API:
    - limit: doc says max 10000, but server caps at 1000
    - offset: max 10000
    - takerOnly: default true (set to false to get all trades)

    With limit=1000 and max_offset=10000, we can fetch up to 11000 trades.
    """
    all_trades: List[Dict[str, Any]] = []
    offset = 0

    while offset <= max_offset:
        params = {
            "market": market,
            "limit": limit,
            "offset": offset,
            "takerOnly": "false",
        }
        batch = data_client.get_trades(**params)
        if not batch:
            break
        all_trades.extend(batch)
        if len(batch) < limit:
            # Got less than requested = no more data
            break
        offset += limit

    return all_trades


def _kpis_from_data_trades(trades: List[Dict[str, Any]]) -> Dict[str, object]:
    """Compute KPIs from Data API /trades response."""
    trade_count = len(trades)
    buy_count = 0
    sell_count = 0
    total_tokens = Decimal(0)
    total_usdc = Decimal(0)
    prices: List[Decimal] = []
    wallets = set()
    timestamps: List[int] = []

    for t in trades:
        side = str(t.get("side") or "").upper()
        if side == "BUY":
            buy_count += 1
        elif side == "SELL":
            sell_count += 1

        size = _to_decimal(t.get("size"))
        price = _to_decimal(t.get("price"))
        usdc = size * price

        total_tokens += size
        total_usdc += usdc
        if price > 0:
            prices.append(price)

        maker = t.get("maker")
        taker = t.get("taker")
        if maker:
            wallets.add(str(maker))
        if taker:
            wallets.add(str(taker))

        ts_str = t.get("timestamp") or t.get("createdAt")
        if ts_str:
            try:
                dt = datetime.fromisoformat(str(ts_str).replace("Z", "+00:00"))
                timestamps.append(int(dt.timestamp()))
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


# -----------------------------------------------------------------------------
# Display helpers
# -----------------------------------------------------------------------------
def _print_kpis(k: Dict[str, object], title: str = "KPI") -> None:
    print(f"=== {title} ===")
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


def _print_comparison(goldsky_kpi: Dict[str, object], data_kpi: Dict[str, object]) -> None:
    print("\n=== COMPARAISON GOLDSKY vs DATA API ===")
    print(f"{'Metrique':<30} {'Goldsky':>20} {'Data API':>20} {'Ecart':>15}")
    print("-" * 85)

    def fmt_num(v: object) -> str:
        if v is None:
            return "N/A"
        if isinstance(v, Decimal):
            return f"{v:,.4f}"
        return str(v)

    def fmt_pct(g: object, d: object) -> str:
        if g is None or d is None:
            return "N/A"
        try:
            gv = float(Decimal(str(g)))
            dv = float(Decimal(str(d)))
            if gv == 0:
                return "N/A" if dv == 0 else "+inf"
            pct = (dv - gv) / gv * 100
            return f"{pct:+.2f}%"
        except Exception:
            return "N/A"

    metrics = [
        ("Nombre de trades", "num_trades"),
        ("Trades BUY", "buy_trades"),
        ("Trades SELL", "sell_trades"),
        ("Traders uniques", "unique_traders"),
        ("Volume tokens", "volume_tokens"),
        ("Volume USDC", "volume_usdc"),
        ("VWAP", "vwap"),
        ("Prix min", "min_price"),
        ("Prix max", "max_price"),
    ]

    for label, key in metrics:
        gv = goldsky_kpi.get(key)
        dv = data_kpi.get(key)
        print(f"{label:<30} {fmt_num(gv):>20} {fmt_num(dv):>20} {fmt_pct(gv, dv):>15}")

    # Timestamps
    g_min = goldsky_kpi.get("min_ts")
    d_min = data_kpi.get("min_ts")
    g_max = goldsky_kpi.get("max_ts")
    d_max = data_kpi.get("max_ts")

    print(f"{'Premier trade':<30} {_to_utc(int(g_min)) if g_min else 'N/A':>20} {_to_utc(int(d_min)) if d_min else 'N/A':>20}")
    print(f"{'Dernier trade':<30} {_to_utc(int(g_max)) if g_max else 'N/A':>20} {_to_utc(int(d_max)) if d_max else 'N/A':>20}")


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

    # For unique traders, we can't simply sum (duplicates across markets)
    # We'll mark it as None for merged results
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


def main() -> None:
    load_dotenv()
    market_url = os.getenv("POLYMARKET_TEST_MARKET_URL")
    page_size = int(os.getenv("POLYMARKET_GOLDSKY_PAGE_SIZE", "1000"))
    max_pages = int(os.getenv("POLYMARKET_GOLDSKY_MAX_PAGES", "200"))

    if not market_url:
        raise ValueError("POLYMARKET_TEST_MARKET_URL manquant dans .env")

    gamma = GammaClient()
    goldsky = GoldskyClient()
    data_client = DataClient()

    targets = _market_targets_from_url(gamma, market_url)
    print(f"URL test: {market_url}")
    print(f"Nombre de marches detectes: {len(targets)}")
    print(f"Goldsky endpoint: {goldsky.query_url}")
    print(f"Data API endpoint: {data_client.base_url}")

    goldsky_kpis_list: List[Dict[str, object]] = []
    data_kpis_list: List[Dict[str, object]] = []

    for idx, t in enumerate(targets, start=1):
        cid = str(t["condition_id"])
        label = str(t["label"])
        token_ids = list(t["token_ids"])

        print(f"\n{'='*80}")
        print(f"=== Marche {idx}/{len(targets)} ===")
        print(f"conditionId: {cid}")
        print(f"label: {label}")
        print(f"tokenIds: {len(token_ids)}")

        # --- Goldsky ---
        print(f"\n--- Goldsky (subgraph) ---")
        try:
            goldsky_rows = goldsky.fetch_order_filled_events(
                token_ids=token_ids,
                first=page_size,
                max_pages=max_pages,
            )
            print(f"Rows recuperees: {len(goldsky_rows)}")
            goldsky_kpi = _kpis_from_goldsky_fills(goldsky_rows, token_ids)
            _print_kpis(goldsky_kpi, "KPI Goldsky")
            goldsky_kpis_list.append(goldsky_kpi)
        except GoldskyError as exc:
            print(f"Erreur Goldsky: {exc.payload}")
            goldsky_kpi = None

        # --- Data API ---
        print(f"\n--- Data API (/trades) ---")
        try:
            data_trades = _fetch_all_data_trades(
                data_client,
                market=cid,
                limit=1000,  # Server caps at 1000 despite doc saying 10000
                max_offset=10000,  # Max allowed by API
            )
            print(f"Trades recuperes: {len(data_trades)}")
            data_kpi = _kpis_from_data_trades(data_trades)
            _print_kpis(data_kpi, "KPI Data API")
            data_kpis_list.append(data_kpi)
        except Exception as exc:
            print(f"Erreur Data API: {exc}")
            data_kpi = None

        # --- Comparison ---
        if goldsky_kpi and data_kpi:
            _print_comparison(goldsky_kpi, data_kpi)

        # UI volume comparison
        ui_vol = t.get("ui_volume")
        if ui_vol is not None:
            print(f"\nUI volume (Gamma): {ui_vol:,.4f}")
            if goldsky_kpi:
                diff_g = float(Decimal(goldsky_kpi["volume_usdc"]) - Decimal(str(ui_vol)))
                pct_g = (diff_g / ui_vol * 100) if ui_vol else 0.0
                print(f"  Ecart Goldsky: {diff_g:,.4f} ({pct_g:+.2f}%)")
            if data_kpi:
                diff_d = float(Decimal(data_kpi["volume_usdc"]) - Decimal(str(ui_vol)))
                pct_d = (diff_d / ui_vol * 100) if ui_vol else 0.0
                print(f"  Ecart Data API: {diff_d:,.4f} ({pct_d:+.2f}%)")

    # Global summary
    print(f"\n{'='*80}")
    print("=== RESUME GLOBAL (tous marches) ===")

    if goldsky_kpis_list:
        global_goldsky = _merge_kpis(goldsky_kpis_list)
        _print_kpis(global_goldsky, "KPI Global Goldsky")

    if data_kpis_list:
        global_data = _merge_kpis(data_kpis_list)
        print()
        _print_kpis(global_data, "KPI Global Data API")

    if goldsky_kpis_list and data_kpis_list:
        _print_comparison(global_goldsky, global_data)

    ui_total = sum((t.get("ui_volume") or 0.0) for t in targets)
    if ui_total:
        print(f"\nUI volume total (Gamma): {ui_total:,.4f}")
        if goldsky_kpis_list:
            gdiff = float(Decimal(global_goldsky["volume_usdc"]) - Decimal(str(ui_total)))
            gpct = gdiff / ui_total * 100
            print(f"  Ecart Goldsky: {gdiff:,.4f} ({gpct:+.2f}%)")
        if data_kpis_list:
            ddiff = float(Decimal(global_data["volume_usdc"]) - Decimal(str(ui_total)))
            dpct = ddiff / ui_total * 100
            print(f"  Ecart Data API: {ddiff:,.4f} ({dpct:+.2f}%)")


if __name__ == "__main__":
    main()
