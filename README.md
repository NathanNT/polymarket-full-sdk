# Polymarketer

A Python SDK for interacting with [Polymarket](https://polymarket.com) - fetch market data, analyze trades, and interact with the orderbook and on-chain smart contracts.

## Features

- **Multiple data sources**: Gamma API, Data API, The Graph, Goldsky, CLOB, and on-chain indexing
- **Trading analytics**: Compute KPIs like volume, VWAP, trade counts, unique traders
- **On-chain indexing**: Index `OrderFilled` events directly from Polygon RPC
- **Cross-source comparison**: Compare data from different sources to validate accuracy

## Installation

```bash
pip install -r requirements.txt
```

### Dependencies

- `requests` - HTTP client for REST APIs
- `python-dotenv` - Environment variable management
- `web3` - Polygon RPC interaction
- `py-clob-client` - Polymarket CLOB trading library

## Configuration

Copy `.env.example` to `.env` and configure your API keys:

```bash
cp .env.example .env
```

Key configuration variables:

| Variable | Description |
|----------|-------------|
| `POLYMARKET_TEST_MARKET_URL` | Target market/event URL for analysis |
| `POLYMARKET_THEGRAPH_API_KEY` | The Graph API key (required for `app.py`, `app_thegraph.py`) |
| `POLYMARKET_GOLDSKY_API_KEY` | Goldsky API key (optional) |
| `POLYMARKET_POLYGON_RPC_URL` | Polygon RPC endpoint (required for `app_onchain.py`) |
| `POLYMARKET_CLOB_PRIVATE_KEY` | Private key for CLOB trading |

## Modules

### polymarket_gamma

REST API client for Polymarket Gamma API - market metadata, events, tags, and search.

```python
from polymarket_gamma import GammaClient

gamma = GammaClient()
event = gamma.get_event_by_slug("fed-decision-in-march")
markets = gamma.list_markets(limit=10)
```

### polymarket_data

REST API client for Polymarket Data API - user positions, leaderboards, trades.

```python
from polymarket_data import DataClient

data = DataClient()
trades = data.get_trades(market="0x...", limit=1000, offset=0)
positions = data.get_positions(user="0x...")
leaderboard = data.get_leaderboard()
```

### polymarket_thegraph

GraphQL client for The Graph - query orderbook fill events.

```python
from polymarket_thegraph import TheGraphClient

graph = TheGraphClient()
result = graph.query(
    query="{ orderFilledEvents(first: 100) { id timestamp } }",
)
```

### polymarket_goldsky

GraphQL client for Goldsky subgraph - alternative orderbook indexing.

```python
from polymarket_goldsky import GoldskyClient

goldsky = GoldskyClient()
fills = goldsky.fetch_order_filled_events(
    token_ids=["123456...", "789012..."],
    first=1000,
    max_pages=10,
)
```

### polymarket_clob

CLOB (Central Limit Order Book) client for L2 trading.

```python
from polymarket_clob import ClobClientWrapper

clob = ClobClientWrapper()
# Requires API credentials or private key with derive_api_creds=True
```

### polymarket_onchain

On-chain indexer that fetches `OrderFilled` events directly from Polygon RPC and stores them in SQLite.

```python
from polymarket_onchain import OnchainFillIndexer

indexer = OnchainFillIndexer()
indexer.sync()
fills = indexer.get_fills(condition_id="0x...")
```

## Applications

### app.py - The Graph Analytics

Analyze trades from The Graph subgraph and compute KPIs per market.

```bash
python app.py
```

### app_goldsky.py - Goldsky Analytics

Same analytics using Goldsky subgraph as data source.

```bash
python app_goldsky.py
```

### app_thegraph.py - The Graph Details

Extended The Graph analytics with additional GraphQL details.

```bash
python app_thegraph.py
```

### app_clob.py - CLOB Trade Analysis

Fetch and analyze trades from the CLOB L2 orderbook.

```bash
python app_clob.py
```

### app_onchain.py - On-Chain Indexing

Index `OrderFilled` events directly from Polygon RPC into SQLite.

```bash
python app_onchain.py
```

### app_compare_subgraph_and_data.py - Source Comparison

Compare trades and KPIs between Goldsky subgraph and Data API.

```bash
python app_compare_subgraph_and_data.py
```

**Output example:**
```
=== COMPARAISON GOLDSKY vs DATA API ===
Metrique                       Goldsky             Data API           Ecart
---------------------------------------------------------------------------
Nombre de trades                  3853                 3850          -0.08%
Trades BUY                        3061                 3058          -0.10%
Trades SELL                        792                  792          +0.00%
Traders uniques                    217                  215          -0.92%
Volume tokens              186,175.7091         186,100.2341          -0.04%
Volume USDC                 78,395.5778          78,380.1234          -0.02%
VWAP                             0.4211               0.4210          -0.02%
```

## KPIs Computed

| Metric | Description |
|--------|-------------|
| `num_trades` | Total number of trades |
| `buy_trades` | Number of BUY trades |
| `sell_trades` | Number of SELL trades |
| `unique_traders` | Distinct wallet addresses |
| `volume_tokens` | Total token volume (size) |
| `volume_usdc` | Notional volume in USDC |
| `vwap` | Volume-weighted average price |
| `min_price` / `max_price` | Price range |
| `min_ts` / `max_ts` | First/last trade timestamps |

## Project Structure

```
polymarketer/
├── app.py                           # The Graph analytics
├── app_clob.py                      # CLOB trade analysis
├── app_goldsky.py                   # Goldsky analytics
├── app_onchain.py                  # On-chain indexing
├── app_thegraph.py                  # The Graph details
├── app_compare_subgraph_and_data.py # Source comparison
├── requirements.txt                 # Dependencies
├── .env.example                     # Configuration template
│
├── polymarket_gamma/                # Gamma API client
│   ├── __init__.py
│   └── client.py
│
├── polymarket_data/                 # Data API client
│   ├── __init__.py
│   └── client.py
│
├── polymarket_thegraph/             # The Graph client
│   ├── __init__.py
│   └── client.py
│
├── polymarket_goldsky/              # Goldsky client
│   ├── __init__.py
│   └── client.py
│
├── polymarket_clob/                 # CLOB client
│   ├── __init__.py
│   └── client.py
│
└── polymarket_onchain/              # On-chain indexer (Polygon RPC)
    ├── __init__.py
    └── indexer.py
```

## License

MIT
