# BC.Game Up/Down History

Scrapes live **Up/Down** round history from [bc.game/trading/up-down](https://bc.game/trading/up-down) (powered by DeTrade), stores it in a **local SQLite** database, and serves an analytics dashboard with charts.

> Personal research / analysis tool. Respect BC.Game / DeTrade terms of service and local laws. Crypto trading involves risk.

## What it captures

Each settled round (when available from the live feed):

- Round id, symbol, period label (`btc_5`, …)
- Result (`UP` / `DOWN` from `winSide`)
- Start / end price and timestamps
- Pool amounts when present
- Raw JSON payload for debugging

Data source:

1. Temporary guest session via `api.detrade.com`
2. WebSocket `wss://websocket.detrade.com/ws` contest ticker (`previousRoundResult` + live game)
3. Optional kline seed from REST for price context

## Setup

```powershell
cd D:\F\work\bc-up-down-history
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Scrape into SQLite

Listen for ~90 seconds and upsert rounds into `data/updown_history.db`:

```powershell
python run.py scrape --symbol "BTC/USD" --duration 90
```

Optional period label (defaults to first available, usually `btc_5`):

```powershell
python run.py scrape --symbol "BTC/USD" --label btc_5 --duration 180
```

## Dashboard + API

```powershell
python run.py serve
```

Open http://127.0.0.1:8787

- **Start scrape** from the UI, or use the CLI above
- Charts: cumulative UP/DOWN, mix doughnut, price moves, recent table

### Useful endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | DB path + round count |
| GET | `/api/periods?symbol=BTC/USD` | Available Up/Down periods |
| GET | `/api/rounds?limit=200` | Stored history |
| GET | `/api/analytics?symbol=BTC/USD` | Summary + chart series |
| POST | `/api/scrape` | `{ "symbol", "label?", "duration_sec" }` |
| GET | `/api/scrape/status` | Live scrape progress |

## Notes

- History on the site is a **rolling live buffer** (`previousRoundResult`). Keep the scraper running to accumulate more rounds over time.
- Guest auth is enough for public contest ticker / history chips; it does **not** pull your private bet history unless you later add a logged-in token.
- SQLite file: `data/updown_history.db`
