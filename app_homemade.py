from __future__ import annotations

import json
import os
import time
from decimal import Decimal
from datetime import datetime, timezone
from typing import Dict, List
from urllib.parse import urlparse

from dotenv import dotenv_values

from polymarket_gamma import GammaClient
from polymarket_homemade import OnchainFillIndexer


DECIMALS = Decimal(10) ** 6


def _extract_slug(url: str) -> tuple[str, str]:
    parts = [p for p in urlparse(url).path.split("/") if p]
    if len(parts) < 2 or parts[0] not in {"event", "market"}:
        raise ValueError("URL invalide. Attendu: /event/{slug} ou /market/{slug}")
    return parts[0], parts[1]


def _parse_token_ids(raw: object) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    s = str(raw).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed]
    except Exception:
        pass
    return [x.strip().strip('"').strip("'") for x in s.strip("[]").split(",") if x.strip()]


def _market_targets(gamma: GammaClient, url: str) -> List[Dict[str, object]]:
    kind, slug = _extract_slug(url)
    if kind == "market":
        markets = [gamma.get_market_by_slug(slug)]
    else:
        event = gamma.get_event_by_slug(slug)
        markets = event.get("markets") or []
    targets = []
    for m in markets:
        condition_id = m.get("conditionId")
        token_ids = _parse_token_ids(m.get("clobTokenIds"))
        if not condition_id or not token_ids:
            continue
        targets.append(
            {
                "label": m.get("question") or m.get("slug") or condition_id,
                "condition_id": str(condition_id),
                "token_ids": token_ids,
                "ui_volume": float(m.get("volumeNum") or 0.0),
                "created_at": m.get("createdAt") or m.get("creationDate") or m.get("startDate"),
            }
        )
    if not targets:
        raise ValueError("Aucun marche exploitable (conditionId/tokenIds).")
    return targets


def _iso_to_ts(value: object) -> int | None:
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return int(dt.astimezone(timezone.utc).timestamp())
    except Exception:
        return None


def _kpi_from_rows(rows: List[Dict], token_ids: List[str]) -> Dict[str, object]:
    token_set = set(token_ids)
    n = 0
    buy = 0
    sell = 0
    volume_usdc = Decimal(0)
    volume_tokens = Decimal(0)
    wallets = set()
    for r in rows:
        maker_asset = str(r["maker_asset_id"])
        taker_asset = str(r["taker_asset_id"])
        maker_amt = Decimal(str(r["maker_amount_filled"])) / DECIMALS
        taker_amt = Decimal(str(r["taker_amount_filled"])) / DECIMALS

        if maker_asset in token_set and taker_asset not in token_set:
            # seller sends outcome token, receives USDC
            n += 1
            sell += 1
            volume_tokens += maker_amt
            volume_usdc += taker_amt
        elif taker_asset in token_set and maker_asset not in token_set:
            # buyer receives outcome token, pays USDC
            n += 1
            buy += 1
            volume_tokens += taker_amt
            volume_usdc += maker_amt
        else:
            continue
        wallets.add(str(r["maker"]))
        wallets.add(str(r["taker"]))

    vwap = (volume_usdc / volume_tokens) if volume_tokens > 0 else Decimal(0)
    return {
        "num_trades": n,
        "buy": buy,
        "sell": sell,
        "unique_wallets": len(wallets),
        "volume_usdc": volume_usdc,
        "volume_tokens": volume_tokens,
        "vwap": vwap,
    }


def main() -> None:
    env = dotenv_values(".env")
    rpc_url = os.getenv("POLYMARKET_POLYGON_RPC_URL") or env.get("POLYMARKET_POLYGON_RPC_URL")
    url = os.getenv("POLYMARKET_TEST_MARKET_URL") or env.get("POLYMARKET_TEST_MARKET_URL")
    db_path = os.getenv("POLYMARKET_HOMEMADE_DB_PATH") or env.get("POLYMARKET_HOMEMADE_DB_PATH") or "polymarket_homemade.db"
    chunk_size = int(os.getenv("POLYMARKET_HOMEMADE_CHUNK_SIZE") or env.get("POLYMARKET_HOMEMADE_CHUNK_SIZE") or 10)
    min_chunk_size = int(os.getenv("POLYMARKET_HOMEMADE_MIN_CHUNK_SIZE") or env.get("POLYMARKET_HOMEMADE_MIN_CHUNK_SIZE") or 1)
    from_block_env = os.getenv("POLYMARKET_HOMEMADE_FROM_BLOCK") or env.get("POLYMARKET_HOMEMADE_FROM_BLOCK")
    to_block_env = os.getenv("POLYMARKET_HOMEMADE_TO_BLOCK") or env.get("POLYMARKET_HOMEMADE_TO_BLOCK")
    max_span_env = os.getenv("POLYMARKET_HOMEMADE_MAX_BLOCK_SPAN") or env.get("POLYMARKET_HOMEMADE_MAX_BLOCK_SPAN")
    debug = (
        (os.getenv("POLYMARKET_HOMEMADE_DEBUG") or env.get("POLYMARKET_HOMEMADE_DEBUG") or "true")
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )
    debug_every = int(os.getenv("POLYMARKET_HOMEMADE_DEBUG_EVERY") or env.get("POLYMARKET_HOMEMADE_DEBUG_EVERY") or 20)
    auto_from_market_start = (
        (os.getenv("POLYMARKET_HOMEMADE_AUTO_FROM_MARKET_START") or env.get("POLYMARKET_HOMEMADE_AUTO_FROM_MARKET_START") or "true")
        .strip()
        .lower()
        in {"1", "true", "yes", "on"}
    )
    wanted_condition = os.getenv("POLYMARKET_COMPARE_MARKET_CONDITION_ID") or env.get("POLYMARKET_COMPARE_MARKET_CONDITION_ID")

    if not rpc_url:
        raise ValueError("POLYMARKET_POLYGON_RPC_URL manquant dans .env")
    if not url:
        raise ValueError("POLYMARKET_TEST_MARKET_URL manquant dans .env")

    gamma = GammaClient()
    targets = _market_targets(gamma, url)
    if wanted_condition:
        targets = [t for t in targets if str(t["condition_id"]).lower() == wanted_condition.lower()]
        if not targets:
            raise ValueError("POLYMARKET_COMPARE_MARKET_CONDITION_ID non trouve dans les marches de l'URL.")

    indexer = OnchainFillIndexer(rpc_url=rpc_url, db_path=str(db_path))
    try:
        latest = int(indexer.w3.eth.block_number)
        last = indexer.get_last_scanned_block()
        if from_block_env:
            from_block = int(from_block_env)
        elif last is not None:
            from_block = last + 1
        elif auto_from_market_start:
            ts_candidates = [_iso_to_ts(t.get("created_at")) for t in targets]
            ts_candidates = [x for x in ts_candidates if x is not None]
            if ts_candidates:
                min_ts = min(ts_candidates) - 24 * 3600  # 1 day safety margin
                from_block = int(indexer.get_block_by_timestamp(min_ts))
            else:
                from_block = max(0, latest - 50_000)
        else:
            from_block = max(0, latest - 50_000)
        to_block = int(to_block_env) if to_block_env else latest
        if max_span_env:
            max_span = int(max_span_env)
            if max_span > 0:
                to_block = min(to_block, from_block + max_span - 1)
        if from_block <= to_block:
            scan_started = time.monotonic()

            def on_progress(p: Dict[str, int]) -> None:
                if not debug:
                    return
                idx = p["chunk_index"]
                if idx % max(1, debug_every) != 0 and p["end_block"] != to_block:
                    return
                done = p["end_block"] - from_block + 1
                total = to_block - from_block + 1
                pct = (done / total * 100) if total > 0 else 100.0
                elapsed = max(1, int(time.monotonic() - scan_started))
                rate = done / elapsed
                remaining_blocks = max(0, total - done)
                eta = int(remaining_blocks / rate) if rate > 0 else -1
                print(
                    f"[scan] chunk={idx} blocks={p['start_block']}..{p['end_block']} "
                    f"progress={pct:.2f}% logs={p['scanned_logs']} fills={p['decoded_fills']} "
                    f"elapsed={elapsed}s eta={eta if eta >= 0 else '?'}s"
                )

            sync = indexer.scan(
                from_block=from_block,
                to_block=to_block,
                chunk_size=chunk_size,
                min_chunk_size=min_chunk_size,
                progress_callback=on_progress,
            )
            print(f"Scan complete: {sync}")
            if to_block < latest and not to_block_env:
                print(f"Resume hint: next from block = {to_block + 1}")
        else:
            print("Scan skip: database already up to date for selected range.")

        print(f"\nURL: {url}")
        total = Decimal(0)
        for i, t in enumerate(targets, start=1):
            rows = indexer.get_fills_for_token_ids(t["token_ids"])
            kpi = _kpi_from_rows([dict(r) for r in rows], token_ids=t["token_ids"])  # type: ignore[arg-type]
            total += kpi["volume_usdc"]  # type: ignore[operator]
            ui = Decimal(str(t["ui_volume"]))
            diff = (kpi["volume_usdc"] - ui) if ui else Decimal(0)  # type: ignore[operator]
            pct = (diff / ui * 100) if ui else Decimal(0)
            print(f"\n=== Marche {i}/{len(targets)} ===")
            print(f"label: {t['label']}")
            print(f"conditionId: {t['condition_id']}")
            print(f"fills(trades): {kpi['num_trades']}")
            print(f"buy/sell: {kpi['buy']}/{kpi['sell']}")
            print(f"volume_usdc(onchain): {kpi['volume_usdc']:,.6f}")
            print(f"volume_ui(gamma): {ui:,.6f}")
            print(f"ecart: {diff:,.6f} ({pct:+.2f}%)")
        print(f"\nTotal onchain volume_usdc: {total:,.6f}")
    finally:
        indexer.close()


if __name__ == "__main__":
    main()
