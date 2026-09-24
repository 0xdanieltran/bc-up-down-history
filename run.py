"""CLI entrypoints for scraping and serving BC Up/Down history."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.db import Database
from app.scraper import scrape_live_history_sync


def cmd_scrape(args: argparse.Namespace) -> None:
    db = Database(ROOT / "data" / "updown_history.db")

    def on_event(evt: dict) -> None:
        print(json.dumps(evt), flush=True)

    result = scrape_live_history_sync(
        db,
        symbol=args.symbol,
        label=args.label,
        duration_sec=args.duration,
        on_event=on_event,
    )
    print(json.dumps({"ok": True, **result}, indent=2))


def cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        app_dir=str(ROOT),
    )


def cmd_maintain(args: argparse.Namespace) -> None:
    db = Database(ROOT / "data" / "updown_history.db")
    pruned_ticks = db.prune_ticks(keep_last=args.keep_ticks)
    pruned_rounds = db.prune_rounds(keep_last=args.keep_rounds)
    info = db.optimize()
    print(
        json.dumps(
            {
                "ok": True,
                "pruned_ticks": pruned_ticks,
                "pruned_rounds": pruned_rounds,
                "stats": db.stats(),
                "optimize": info,
            },
            indent=2,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="BC.Game Up/Down history scraper + analytics")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_scrape = sub.add_parser("scrape", help="Scrape live Up/Down round history into SQLite")
    p_scrape.add_argument("--symbol", default="BTC/USD")
    p_scrape.add_argument("--label", default=None, help="Period label, e.g. btc_5")
    p_scrape.add_argument("--duration", type=int, default=0, help="Seconds to listen (0 = continuous until Ctrl+C)")
    p_scrape.set_defaults(func=cmd_scrape)

    p_serve = sub.add_parser("serve", help="Run analytics dashboard + API")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8787)
    p_serve.add_argument("--reload", action="store_true")
    p_serve.set_defaults(func=cmd_serve)

    p_maint = sub.add_parser("maintain", help="Prune old ticks/rounds and optimize SQLite")
    p_maint.add_argument("--keep-ticks", type=int, default=5000)
    p_maint.add_argument("--keep-rounds", type=int, default=50000)
    p_maint.set_defaults(func=cmd_maintain)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
