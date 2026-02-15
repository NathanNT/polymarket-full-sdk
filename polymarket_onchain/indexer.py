from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware


ORDER_FILLED_TOPIC = "0x" + Web3.keccak(
    text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)"
).hex()

# Main CLOB Exchange and NegRisk Exchange (Polygon)
DEFAULT_EXCHANGE_ADDRESSES = [
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
]


@dataclass
class FillRow:
    chain_id: int
    exchange: str
    block_number: int
    tx_hash: str
    log_index: int
    timestamp: int
    order_hash: str
    maker: str
    taker: str
    maker_asset_id: str
    taker_asset_id: str
    maker_amount_filled: int
    taker_amount_filled: int
    fee: int


class OnchainFillIndexer:
    def __init__(
        self,
        rpc_url: str,
        db_path: str = "polymarket_onchain.db",
        chain_id: int = 137,
        exchange_addresses: Optional[Iterable[str]] = None,
    ) -> None:
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        # Polygon requires POA extraData middleware for block decoding
        self.w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        if not self.w3.is_connected():
            raise RuntimeError("Unable to connect to Polygon RPC")
        self.chain_id = chain_id
        self.exchange_addresses = [
            Web3.to_checksum_address(a) for a in (exchange_addresses or DEFAULT_EXCHANGE_ADDRESSES)
        ]
        self.db_path = str(Path(db_path))
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def close(self) -> None:
        self.conn.close()

    def _init_db(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
                chain_id INTEGER NOT NULL,
                exchange TEXT NOT NULL,
                block_number INTEGER NOT NULL,
                tx_hash TEXT NOT NULL,
                log_index INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                order_hash TEXT NOT NULL,
                maker TEXT NOT NULL,
                taker TEXT NOT NULL,
                maker_asset_id TEXT NOT NULL,
                taker_asset_id TEXT NOT NULL,
                maker_amount_filled TEXT NOT NULL,
                taker_amount_filled TEXT NOT NULL,
                fee TEXT NOT NULL,
                PRIMARY KEY (chain_id, tx_hash, log_index)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_state (
                chain_id INTEGER PRIMARY KEY,
                last_scanned_block INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_block ON fills(block_number)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_maker_asset ON fills(maker_asset_id)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_fills_taker_asset ON fills(taker_asset_id)"
        )
        self.conn.commit()

    def get_last_scanned_block(self) -> Optional[int]:
        row = self.conn.execute(
            "SELECT last_scanned_block FROM sync_state WHERE chain_id = ?",
            (self.chain_id,),
        ).fetchone()
        return int(row["last_scanned_block"]) if row else None

    def get_block_by_timestamp(self, target_ts: int) -> int:
        """
        Return the first block whose timestamp is >= target_ts (binary search).
        """
        latest = int(self.w3.eth.block_number)
        latest_ts = int(self.w3.eth.get_block(latest)["timestamp"])
        if target_ts >= latest_ts:
            return latest

        lo = 0
        hi = latest
        while lo < hi:
            mid = (lo + hi) // 2
            mid_ts = int(self.w3.eth.get_block(mid)["timestamp"])
            if mid_ts < target_ts:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def set_last_scanned_block(self, block: int) -> None:
        self.conn.execute(
            """
            INSERT INTO sync_state (chain_id, last_scanned_block)
            VALUES (?, ?)
            ON CONFLICT(chain_id) DO UPDATE SET last_scanned_block=excluded.last_scanned_block
            """,
            (self.chain_id, int(block)),
        )
        self.conn.commit()

    @staticmethod
    def _to_int(hex_or_int: object) -> int:
        if isinstance(hex_or_int, int):
            return hex_or_int
        if isinstance(hex_or_int, str) and hex_or_int.startswith("0x"):
            return int(hex_or_int, 16)
        return int(hex_or_int)  # type: ignore[arg-type]

    @staticmethod
    def _as_hex(value: object) -> str:
        if isinstance(value, str):
            return value if value.startswith("0x") else "0x" + value
        return Web3.to_hex(value)  # type: ignore[arg-type]

    @classmethod
    def _hex_to_address(cls, topic_hex: object) -> str:
        hx = cls._as_hex(topic_hex)
        raw = hx[2:] if hx.startswith("0x") else hx
        return "0x" + raw[-40:].lower()

    @classmethod
    def _split_data_words(cls, data_hex: object) -> List[str]:
        hx = cls._as_hex(data_hex)
        data = hx[2:] if hx.startswith("0x") else hx
        if not data:
            return []
        return ["0x" + data[i : i + 64] for i in range(0, len(data), 64)]

    def _decode_log(self, log: Dict, block_timestamp: int) -> Optional[FillRow]:
        topics = log.get("topics", [])
        data_words = self._split_data_words(log.get("data", "0x"))
        tx_hash = log["transactionHash"].hex().lower()
        log_index = self._to_int(log["logIndex"])
        block_number = self._to_int(log["blockNumber"])
        exchange = log["address"].lower()

        # Expected layout for Polymarket: 3 indexed + 5 words in data
        if len(topics) >= 4 and len(data_words) >= 5:
            order_hash = self._as_hex(topics[1]).lower()
            maker = self._hex_to_address(topics[2])
            taker = self._hex_to_address(topics[3])
            maker_asset_id = str(int(data_words[0], 16))
            taker_asset_id = str(int(data_words[1], 16))
            maker_amount_filled = int(data_words[2], 16)
            taker_amount_filled = int(data_words[3], 16)
            fee = int(data_words[4], 16)
        # Fallback layout: everything in data
        elif len(data_words) >= 8:
            order_hash = data_words[0].lower()
            maker = self._hex_to_address(data_words[1])
            taker = self._hex_to_address(data_words[2])
            maker_asset_id = str(int(data_words[3], 16))
            taker_asset_id = str(int(data_words[4], 16))
            maker_amount_filled = int(data_words[5], 16)
            taker_amount_filled = int(data_words[6], 16)
            fee = int(data_words[7], 16)
        else:
            return None

        return FillRow(
            chain_id=self.chain_id,
            exchange=exchange,
            block_number=block_number,
            tx_hash=tx_hash,
            log_index=log_index,
            timestamp=int(block_timestamp),
            order_hash=order_hash,
            maker=maker,
            taker=taker,
            maker_asset_id=maker_asset_id,
            taker_asset_id=taker_asset_id,
            maker_amount_filled=maker_amount_filled,
            taker_amount_filled=taker_amount_filled,
            fee=fee,
        )

    def _insert_fills(self, fills: List[FillRow]) -> None:
        if not fills:
            return
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO fills (
                chain_id, exchange, block_number, tx_hash, log_index, timestamp,
                order_hash, maker, taker, maker_asset_id, taker_asset_id,
                maker_amount_filled, taker_amount_filled, fee
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    f.chain_id,
                    f.exchange,
                    f.block_number,
                    f.tx_hash,
                    f.log_index,
                    f.timestamp,
                    f.order_hash,
                    f.maker,
                    f.taker,
                    f.maker_asset_id,
                    f.taker_asset_id,
                    str(f.maker_amount_filled),
                    str(f.taker_amount_filled),
                    str(f.fee),
                )
                for f in fills
            ],
        )
        self.conn.commit()

    def scan(
        self,
        from_block: int,
        to_block: Optional[int] = None,
        chunk_size: int = 10,
        min_chunk_size: int = 1,
        progress_callback: Optional[Callable[[Dict[str, int]], None]] = None,
    ) -> Dict[str, int]:
        if to_block is None:
            to_block = int(self.w3.eth.block_number)
        if from_block > to_block:
            raise ValueError("from_block must be <= to_block")
        if chunk_size <= 0 or min_chunk_size <= 0:
            raise ValueError("chunk_size and min_chunk_size must be > 0")
        if min_chunk_size > chunk_size:
            min_chunk_size = chunk_size

        scanned_logs = 0
        decoded_fills = 0
        started = time.monotonic()
        chunk_index = 0

        start = from_block
        while start <= to_block:
            current_chunk = chunk_size
            logs = None
            end = min(start + current_chunk - 1, to_block)

            while logs is None:
                end = min(start + current_chunk - 1, to_block)
                try:
                    logs = self.w3.eth.get_logs(
                        {
                            "fromBlock": start,
                            "toBlock": end,
                            "address": self.exchange_addresses,
                            "topics": [ORDER_FILLED_TOPIC],
                        }
                    )
                except Exception as exc:  # provider-specific HTTP or RPC errors
                    if current_chunk <= min_chunk_size:
                        raise RuntimeError(
                            f"eth_getLogs failed even at min chunk {min_chunk_size} "
                            f"for range [{start}, {end}] - provider error: {exc}"
                        ) from exc
                    current_chunk = max(min_chunk_size, current_chunk // 2)

            scanned_logs += len(logs)
            chunk_index += 1

            block_ts_cache: Dict[int, int] = {}
            fills: List[FillRow] = []
            for log in logs:
                bn = self._to_int(log["blockNumber"])
                if bn not in block_ts_cache:
                    block_ts_cache[bn] = int(self.w3.eth.get_block(bn)["timestamp"])
                decoded = self._decode_log(log, block_ts_cache[bn])
                if decoded:
                    fills.append(decoded)
            decoded_fills += len(fills)
            self._insert_fills(fills)
            self.set_last_scanned_block(end)
            if progress_callback:
                progress_callback(
                    {
                        "chunk_index": chunk_index,
                        "from_block": from_block,
                        "to_block": to_block,
                        "start_block": start,
                        "end_block": end,
                        "scanned_logs": scanned_logs,
                        "decoded_fills": decoded_fills,
                        "elapsed_sec": int(time.monotonic() - started),
                    }
                )
            start = end + 1

        return {
            "from_block": from_block,
            "to_block": to_block,
            "scanned_logs": scanned_logs,
            "decoded_fills": decoded_fills,
        }

    def get_fills_for_token_ids(
        self,
        token_ids: Iterable[str],
        from_ts: Optional[int] = None,
        to_ts: Optional[int] = None,
    ) -> List[sqlite3.Row]:
        ids = [str(x) for x in token_ids]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        where = f"(maker_asset_id IN ({placeholders}) OR taker_asset_id IN ({placeholders}))"
        params: List[object] = ids + ids
        if from_ts is not None:
            where += " AND timestamp >= ?"
            params.append(int(from_ts))
        if to_ts is not None:
            where += " AND timestamp <= ?"
            params.append(int(to_ts))
        sql = f"""
            SELECT *
            FROM fills
            WHERE {where}
            ORDER BY block_number DESC, log_index DESC
        """
        return list(self.conn.execute(sql, params).fetchall())
